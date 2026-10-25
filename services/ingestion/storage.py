"""Raw data and schema-version storage. MinIO can implement the same protocol."""

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Protocol

from .sources import Record


def _run_name(run_id: str) -> str:
    """Run ids (e.g. Airflow's) contain characters that are unsafe in paths and keys."""
    return hashlib.sha256(run_id.encode()).hexdigest()[:24]


class RawStore(Protocol):
    def next_batch_number(self, dataset_id: str, day: str) -> int: ...

    def save_raw(self, dataset_id: str, batch_id: str, records: list[Record]) -> str:
        """Persist records and return their URI."""

    def load_raw(self, dataset_id: str, batch_id: str) -> list[Record]:
        """Read back the records saved for a batch."""

    def schema_version(self, dataset_id: str, fingerprint: str) -> tuple[int, int | None]:
        """Register fingerprint; return (version, previous_version_if_schema_changed)."""

    def load_run(self, dataset_id: str, run_id: str) -> list[str] | None:
        """Events (as JSON) committed for this run, or None if the run was never committed."""

    def save_run(self, dataset_id: str, run_id: str, events: list[str]) -> None:
        """Commit the events of a run. Written before publishing, so a retry can republish them."""


class LocalRawStore:
    def __init__(self, root: Path):
        self._root = root

    def next_batch_number(self, dataset_id: str, day: str) -> int:
        dataset_dir = self._root / dataset_id
        if not dataset_dir.exists():
            return 1
        return len(list(dataset_dir.glob(f"{day}-*.jsonl"))) + 1

    def save_raw(self, dataset_id: str, batch_id: str, records: list[Record]) -> str:
        path = self._root / dataset_id / f"{batch_id}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("x", encoding="utf-8") as f:  # never overwrite a batch another run just wrote
            for record in records:
                f.write(json.dumps(record, default=str) + "\n")
        return path.resolve().as_uri()

    def load_raw(self, dataset_id: str, batch_id: str) -> list[Record]:
        path = self._root / dataset_id / f"{batch_id}.jsonl"
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]

    def schema_version(self, dataset_id: str, fingerprint: str) -> tuple[int, int | None]:
        path = self._root / dataset_id / "schema.json"
        known: list[str] = json.loads(path.read_text())["fingerprints"] if path.exists() else []
        if fingerprint in known:
            return known.index(fingerprint) + 1, None
        previous = len(known) or None
        known.append(fingerprint)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"fingerprints": known}))
        return len(known), previous

    def _run_path(self, dataset_id: str, run_id: str) -> Path:
        return self._root / dataset_id / "runs" / f"{_run_name(run_id)}.json"

    def load_run(self, dataset_id: str, run_id: str) -> list[str] | None:
        path = self._run_path(dataset_id, run_id)
        return json.loads(path.read_text()) if path.exists() else None

    def save_run(self, dataset_id: str, run_id: str, events: list[str]) -> None:
        path = self._run_path(dataset_id, run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(events))
        tmp.replace(path)  # atomic: a run is either fully committed or absent


class S3RawStore:
    """RawStore on any S3-compatible API (MinIO locally). Same layout as LocalRawStore."""

    def __init__(self, client: Any, bucket: str):
        self._client = client
        self._bucket = bucket

    @classmethod
    def from_endpoint(cls, endpoint_url: str, bucket: str, access_key: str, secret_key: str) -> "S3RawStore":
        import boto3

        client = boto3.client(
            "s3", endpoint_url=endpoint_url, aws_access_key_id=access_key, aws_secret_access_key=secret_key
        )
        try:
            client.head_bucket(Bucket=bucket)
        except client.exceptions.ClientError:
            client.create_bucket(Bucket=bucket)
        return cls(client, bucket)

    def next_batch_number(self, dataset_id: str, day: str) -> int:
        paginator = self._client.get_paginator("list_objects_v2")
        count = 0
        for page in paginator.paginate(Bucket=self._bucket, Prefix=f"{dataset_id}/{day}-"):
            count += sum(1 for obj in page.get("Contents", []) if obj["Key"].endswith(".jsonl"))
        return count + 1

    def save_raw(self, dataset_id: str, batch_id: str, records: list[Record]) -> str:
        key = f"{dataset_id}/{batch_id}.jsonl"
        body = "".join(json.dumps(record, default=str) + "\n" for record in records)
        self._client.put_object(Bucket=self._bucket, Key=key, Body=body.encode("utf-8"))
        return f"s3://{self._bucket}/{key}"

    def load_raw(self, dataset_id: str, batch_id: str) -> list[Record]:
        body = self._client.get_object(Bucket=self._bucket, Key=f"{dataset_id}/{batch_id}.jsonl")["Body"].read()
        return [json.loads(line) for line in body.decode("utf-8").splitlines() if line]

    def schema_version(self, dataset_id: str, fingerprint: str) -> tuple[int, int | None]:
        key = f"{dataset_id}/schema.json"
        try:
            known: list[str] = json.loads(self._client.get_object(Bucket=self._bucket, Key=key)["Body"].read())[
                "fingerprints"
            ]
        except Exception as exc:
            if getattr(exc, "response", {}).get("Error", {}).get("Code") != "NoSuchKey":
                raise
            known = []
        if fingerprint in known:
            return known.index(fingerprint) + 1, None
        previous = len(known) or None
        known.append(fingerprint)
        self._client.put_object(Bucket=self._bucket, Key=key, Body=json.dumps({"fingerprints": known}).encode())
        return len(known), previous

    def load_run(self, dataset_id: str, run_id: str) -> list[str] | None:
        try:
            body = self._client.get_object(Bucket=self._bucket, Key=self._run_key(dataset_id, run_id))["Body"]
        except Exception as exc:
            if getattr(exc, "response", {}).get("Error", {}).get("Code") != "NoSuchKey":
                raise
            return None
        return json.loads(body.read())

    def save_run(self, dataset_id: str, run_id: str, events: list[str]) -> None:
        self._client.put_object(
            Bucket=self._bucket, Key=self._run_key(dataset_id, run_id), Body=json.dumps(events).encode()
        )

    @staticmethod
    def _run_key(dataset_id: str, run_id: str) -> str:
        return f"{dataset_id}/runs/{_run_name(run_id)}.json"


def store_from_env() -> RawStore:
    """S3-compatible store if S3_ENDPOINT_URL is set, else the local RAW_DATA_DIR."""
    endpoint = os.environ.get("S3_ENDPOINT_URL")
    if endpoint:
        return S3RawStore.from_endpoint(
            endpoint,
            os.environ.get("S3_BUCKET", "raw-data"),
            os.environ.get("S3_ACCESS_KEY", "minioadmin"),
            os.environ.get("S3_SECRET_KEY", "minioadmin"),
        )
    return LocalRawStore(Path(os.environ.get("RAW_DATA_DIR", "/data/raw")))
