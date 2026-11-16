"""Raw data and schema-version storage. MinIO can implement the same protocol."""

import hashlib
import json
import os
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Protocol

from .sources import Record


def _atomic_create(path: Path, body: bytes) -> bool:
    """Publish a complete file exactly once; readers never see partial writes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".pending-")
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
            return True
        except FileExistsError:
            return False
    finally:
        os.unlink(temporary)


@contextmanager
def _schema_lock(path: Path):
    """Serialize local schema updates across processes; OS releases crash locks."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as stream:
        stream.write(b"0")
        stream.flush()
        deadline = time.monotonic() + 30
        while True:
            try:
                stream.seek(0)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("schema lock timeout")
                time.sleep(0.01)
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream, fcntl.LOCK_UN)


def _conflict(error):
    return getattr(error, "response", {}).get("Error", {}).get("Code") in {
        "PreconditionFailed", "ConditionalRequestConflict", "412", "409",
    }


def _run_name(run_id: str) -> str:
    """Run ids (e.g. Airflow's) contain characters that are unsafe in paths and keys."""
    return hashlib.sha256(run_id.encode()).hexdigest()[:24]


class RawStore(Protocol):
    """Raw batch storage with schema history and committed run event records."""

    def next_batch_number(self, dataset_id: str, day: str) -> int:
        """Suggest the next batch number for a day without reserving it."""
        ...

    def save_raw(self, dataset_id: str, batch_id: str, records: list[Record]) -> str:
        """Persist records and return their URI."""

    def load_raw(self, dataset_id: str, batch_id: str) -> list[Record]:
        """Read back the records saved for a batch."""

    def schema_version(self, dataset_id: str, fingerprint: str) -> tuple[int, int | None]:
        """Register fingerprint; return (version, previous_version_if_schema_changed)."""

    def load_run(self, dataset_id: str, run_id: str) -> list[str] | None:
        """Events (as JSON) committed for this run, or None if the run was never committed."""

    def save_run(self, dataset_id: str, run_id: str, events: list[str]) -> None:
        """Create an immutable run manifest; a losing writer must load the winner."""


class LocalRawStore:
    """Store JSONL batches and metadata on disk without replacing existing raw files."""

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
        body = "".join(json.dumps(record, default=str) + "\n" for record in records).encode()
        if not _atomic_create(path, body):
            raise FileExistsError(path)
        return path.resolve().as_uri()

    def load_raw(self, dataset_id: str, batch_id: str) -> list[Record]:
        path = self._root / dataset_id / f"{batch_id}.jsonl"
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]

    def schema_version(self, dataset_id: str, fingerprint: str) -> tuple[int, int | None]:
        with _schema_lock(self._root / dataset_id / ".schema.lock"):
            return self._schema_version(dataset_id, fingerprint)

    def _schema_version(self, dataset_id, fingerprint):
        path = self._root / dataset_id / "schema.json"
        known: list[str] = json.loads(path.read_text())["fingerprints"] if path.exists() else []
        if fingerprint in known:
            return known.index(fingerprint) + 1, None
        previous = len(known) or None
        known.append(fingerprint)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(dir=path.parent)
        try:
            with os.fdopen(fd, "w") as stream:
                json.dump({"fingerprints": known}, stream)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        return len(known), previous

    def _run_path(self, dataset_id: str, run_id: str) -> Path:
        return self._root / dataset_id / "runs" / f"{_run_name(run_id)}.json"

    def load_run(self, dataset_id: str, run_id: str) -> list[str] | None:
        path = self._run_path(dataset_id, run_id)
        return json.loads(path.read_text()) if path.exists() else None

    def save_run(self, dataset_id: str, run_id: str, events: list[str]) -> None:
        """Atomically commit the first complete manifest, preserving the winner."""
        path = self._run_path(dataset_id, run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        _atomic_create(path, json.dumps(events).encode())


class S3RawStore:
    """Store raw batches and metadata through an S3-compatible object API.

    Requires conditional PutObject support. Raw files and run manifests are
    immutable; schema history uses optimistic concurrency with ETags.
    """

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
        except client.exceptions.ClientError as error:
            if error.response["Error"]["Code"] not in {"404", "NoSuchBucket"}:
                raise
            try:
                client.create_bucket(Bucket=bucket)
            except client.exceptions.ClientError as error:
                if error.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
                    raise
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
        try:
            self._client.put_object(Bucket=self._bucket, Key=key, Body=body.encode("utf-8"), IfNoneMatch="*")
        except Exception as error:
            if _conflict(error):
                raise FileExistsError(key) from error
            raise
        return f"s3://{self._bucket}/{key}"

    def load_raw(self, dataset_id: str, batch_id: str) -> list[Record]:
        body = self._client.get_object(Bucket=self._bucket, Key=f"{dataset_id}/{batch_id}.jsonl")["Body"].read()
        return [json.loads(line) for line in body.decode("utf-8").splitlines() if line]

    def schema_version(self, dataset_id: str, fingerprint: str) -> tuple[int, int | None]:
        for _ in range(128):
            try:
                return self._schema_version(dataset_id, fingerprint)
            except Exception as error:
                if not _conflict(error):
                    raise
        raise TimeoutError("schema update contention exceeded retry budget")

    def _schema_version(self, dataset_id, fingerprint):
        key = f"{dataset_id}/schema.json"
        try:
            stored = self._client.get_object(Bucket=self._bucket, Key=key)
            known = json.loads(stored["Body"].read())["fingerprints"]
            condition = {"IfMatch": stored["ETag"]}
        except Exception as exc:
            if getattr(exc, "response", {}).get("Error", {}).get("Code") != "NoSuchKey":
                raise
            known = []
            condition = {"IfNoneMatch": "*"}
        if fingerprint in known:
            return known.index(fingerprint) + 1, None
        previous = len(known) or None
        known.append(fingerprint)
        self._client.put_object(Bucket=self._bucket, Key=key, Body=json.dumps({"fingerprints": known}).encode(), **condition)
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
        for _ in range(128):
            try:
                self._client.put_object(
                    Bucket=self._bucket, Key=self._run_key(dataset_id, run_id),
                    Body=json.dumps(events).encode(), IfNoneMatch="*",
                )
                return
            except Exception as error:
                if not _conflict(error):
                    raise
                if self.load_run(dataset_id, run_id) is not None:
                    return
        raise TimeoutError("run commit contention exceeded retry budget")

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
