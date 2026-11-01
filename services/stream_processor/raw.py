"""Read raw batch files referenced by dataset.ingested events."""

import json
import hashlib
import os
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import unquote, urlparse
from urllib.request import url2pathname


class RawDataError(ValueError):
    """Raised when raw data cannot be read or parsed."""

    def __init__(self, message, *, sha256=None):
        super().__init__(message)
        self.sha256 = sha256


class RawRecords(list):
    """Parsed records with the checksum of the exact bytes read in the same request."""

    def __init__(self, records, sha256):
        super().__init__(records)
        self.sha256 = sha256


def read_raw(uri: str) -> list[dict[str, Any]]:
    """Read JSONL records from a file:// or s3:// URI; blank lines are skipped."""
    if urlparse(uri).scheme == "s3":
        body = _s3_bytes(uri)
    else:
        path = local_path(uri)
        try:
            body = path.read_bytes()
        except OSError as error:
            raise RawDataError(f"cannot read raw data: {path}") from error
    checksum = hashlib.sha256(body).hexdigest()
    try:
        records = _parse_lines(body.decode("utf-8").splitlines())
    except UnicodeDecodeError as error:
        raise RawDataError("raw data is not UTF-8", sha256=checksum) from error
    except RawDataError as error:
        error.sha256 = checksum
        raise
    return RawRecords(records, checksum)


def _parse_lines(lines: Iterable[str]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as error:
            raise RawDataError(f"invalid JSON on line {number}") from error
        if not isinstance(record, dict):
            raise RawDataError(f"line {number} is not a JSON object")
        records.append(record)
    return records


def _s3_bytes(uri: str) -> bytes:
    """Download an s3://bucket/key object; endpoint and credentials come from the S3_* environment."""
    parsed = urlparse(uri)
    bucket, key = parsed.netloc, parsed.path.lstrip("/")
    try:
        import boto3

        client = boto3.client(
            "s3",
            endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
            aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "minioadmin"),
        )
        stream = client.get_object(Bucket=bucket, Key=key)["Body"]
        try:
            body = stream.read()
        finally:
            stream.close()
    except Exception as error:
        raise RawDataError(f"cannot read raw data: {uri}") from error
    return body


def local_path(uri: str) -> Path:
    """Return the local path of a file:// URI; other schemes are rejected."""
    parsed = urlparse(uri)
    if parsed.scheme != "file":
        raise RawDataError(f"unsupported raw URI scheme: {parsed.scheme or 'none'}")
    path = parsed.path
    if parsed.netloc:
        path = f"//{parsed.netloc}{path}"
    return Path(url2pathname(unquote(path)))
