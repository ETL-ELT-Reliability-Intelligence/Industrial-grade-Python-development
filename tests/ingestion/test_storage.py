"""Both RawStore implementations must satisfy the same behaviour."""

import io

import pytest

from services.ingestion.storage import LocalRawStore, S3RawStore


class NoSuchKey(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class FakeS3Client:
    """Minimal in-memory stand-in for the boto3 calls S3RawStore uses."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def put_object(self, Bucket, Key, Body):
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise NoSuchKey()
        return {"Body": io.BytesIO(self.objects[Key])}

    def get_paginator(self, _name):
        client = self

        class Paginator:
            def paginate(self, Bucket, Prefix):
                yield {"Contents": [{"Key": k} for k in client.objects if k.startswith(Prefix)]}

        return Paginator()


@pytest.fixture(params=["local", "s3"])
def store(request, tmp_path):
    if request.param == "local":
        return LocalRawStore(tmp_path)
    return S3RawStore(FakeS3Client(), "raw-data")


def test_batch_numbers_count_saved_batches_per_day(store):
    assert store.next_batch_number("orders", "2026-09-26") == 1
    store.save_raw("orders", "2026-09-26-001", [{"id": 1}])
    store.save_raw("orders", "2026-09-25-001", [{"id": 1}])
    assert store.next_batch_number("orders", "2026-09-26") == 2


def test_schema_version_tracks_changes(store):
    assert store.schema_version("orders", "a") == (1, None)
    assert store.schema_version("orders", "a") == (1, None)
    assert store.schema_version("orders", "b") == (2, 1)
    assert store.schema_version("orders", "a") == (1, None)


def test_s3_save_raw_returns_s3_uri_and_jsonl_body():
    client = FakeS3Client()
    uri = S3RawStore(client, "raw-data").save_raw("orders", "2026-09-26-001", [{"id": 1}, {"id": 2}])

    assert uri == "s3://raw-data/orders/2026-09-26-001.jsonl"
    assert client.objects["orders/2026-09-26-001.jsonl"] == b'{"id": 1}\n{"id": 2}\n'


def test_s3_unexpected_error_is_not_swallowed():
    class Broken(FakeS3Client):
        def get_object(self, Bucket, Key):
            raise RuntimeError("connection refused")

    with pytest.raises(RuntimeError):
        S3RawStore(Broken(), "raw-data").schema_version("orders", "a")


def test_run_commit_roundtrip(store):
    assert store.load_run("orders", "scheduled__2026-09-26T10:00:00+00:00") is None
    store.save_run("orders", "scheduled__2026-09-26T10:00:00+00:00", ['{"a": 1}'])
    assert store.load_run("orders", "scheduled__2026-09-26T10:00:00+00:00") == ['{"a": 1}']
    assert store.load_run("orders", "other") is None


def test_load_raw_returns_what_was_saved(store):
    records = [{"id": 1, "name": "a", "missing": None}, {"id": 2, "name": "b", "missing": None}]
    store.save_raw("orders", "2026-09-26-001", records)
    assert store.load_raw("orders", "2026-09-26-001") == records
