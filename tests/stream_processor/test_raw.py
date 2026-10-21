import io
import json
import sys
import types

import pytest

from services.stream_processor.raw import RawDataError, read_raw


def write_jsonl(path, records):
    path.write_text("\n".join(json.dumps(record) for record in records), encoding="utf-8")
    return path.as_uri()


def test_reads_records_from_a_file_uri(tmp_path):
    uri = write_jsonl(tmp_path / "заказы.jsonl", [{"id": 1}, {"id": 2}])

    assert read_raw(uri) == [{"id": 1}, {"id": 2}]


def test_blank_lines_are_skipped(tmp_path):
    path = tmp_path / "orders.jsonl"
    path.write_text('{"id": 1}\n\n{"id": 2}\n', encoding="utf-8")

    assert read_raw(path.as_uri()) == [{"id": 1}, {"id": 2}]


def test_other_schemes_are_rejected():
    with pytest.raises(RawDataError, match="scheme"):
        read_raw("http://example.com/orders/batch.jsonl")


def test_reads_records_from_an_s3_uri(monkeypatch):
    class Client:
        def get_object(self, Bucket, Key):
            assert (Bucket, Key) == ("raw-data", "orders/batch.jsonl")
            return {"Body": io.BytesIO(b'{"id": 1}\n\n{"id": 2}\n')}

    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(client=lambda *args, **kwargs: Client()))

    assert read_raw("s3://raw-data/orders/batch.jsonl") == [{"id": 1}, {"id": 2}]


def test_a_missing_s3_object_is_a_raw_data_error(monkeypatch):
    class Client:
        def get_object(self, Bucket, Key):
            raise KeyError("NoSuchKey")

    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(client=lambda *args, **kwargs: Client()))

    with pytest.raises(RawDataError, match="cannot read"):
        read_raw("s3://raw-data/missing.jsonl")


def test_malformed_json_reports_the_line(tmp_path):
    path = tmp_path / "orders.jsonl"
    path.write_text('{"id": 1}\nnot-json\n', encoding="utf-8")

    with pytest.raises(RawDataError, match="line 2"):
        read_raw(path.as_uri())


def test_non_object_line_is_rejected(tmp_path):
    path = tmp_path / "orders.jsonl"
    path.write_text("[1, 2]\n", encoding="utf-8")

    with pytest.raises(RawDataError, match="JSON object"):
        read_raw(path.as_uri())


def test_missing_file_is_rejected(tmp_path):
    with pytest.raises(RawDataError, match="cannot read"):
        read_raw((tmp_path / "missing.jsonl").as_uri())
