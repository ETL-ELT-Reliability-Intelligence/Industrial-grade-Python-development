"""Raw data and schema-version storage. MinIO can implement the same protocol."""

import json
from pathlib import Path
from typing import Any, Protocol

from .sources import Record


class RawStore(Protocol):
    def next_batch_number(self, dataset_id: str, day: str) -> int: ...

    def save_raw(self, dataset_id: str, batch_id: str, records: list[Record]) -> str:
        """Persist records and return their URI."""

    def schema_version(self, dataset_id: str, fingerprint: str) -> tuple[int, int | None]:
        """Register fingerprint; return (version, previous_version_if_schema_changed)."""


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
        with path.open("w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record, default=str) + "\n")
        return path.resolve().as_uri()

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
