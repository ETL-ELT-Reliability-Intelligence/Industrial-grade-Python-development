"""Read raw batch files referenced by dataset.ingested events."""

import json
import os
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import unquote, urlparse
from urllib.request import url2pathname


class RawDataError(ValueError):
    """Raised when raw data cannot be read or parsed."""


def read_raw(uri: str) -> list[dict[str, Any]]:
    """Read JSONL records from a file:// or s3:// URI; blank lines are skipped."""
    if urlparse(uri).scheme == "s3":
        return _parse_lines(_s3_lines(uri))
    path = local_path(uri)
    try:
        with path.open(encoding="utf-8") as stream:
            return _parse_lines(stream)
    except OSError as error:
        raise RawDataError(f"cannot read raw data: {path}") from error


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


def _s3_lines(uri: str) -> list[str]:
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
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception as error:
        raise RawDataError(f"cannot read raw data: {uri}") from error
    return body.decode("utf-8").splitlines()


def local_path(uri: str) -> Path:
    """Return the local path of a file:// URI; other schemes are rejected."""
    parsed = urlparse(uri)
    if parsed.scheme != "file":
        raise RawDataError(f"unsupported raw URI scheme: {parsed.scheme or 'none'}")
    path = parsed.path
    if parsed.netloc:
        path = f"//{parsed.netloc}{path}"
    return Path(url2pathname(unquote(path)))
