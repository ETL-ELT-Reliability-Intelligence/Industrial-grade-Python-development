"""Read raw batch files referenced by dataset.ingested events."""

import json
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse
from urllib.request import url2pathname


class RawDataError(ValueError):
    """Raised when raw data cannot be read or parsed."""


def read_raw(uri: str) -> list[dict[str, Any]]:
    """Read JSONL records from a local file URI; blank lines are skipped."""
    path = local_path(uri)
    records: list[dict[str, Any]] = []
    try:
        with path.open(encoding="utf-8") as stream:
            for number, line in enumerate(stream, start=1):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as error:
                    raise RawDataError(f"invalid JSON on line {number}") from error
                if not isinstance(record, dict):
                    raise RawDataError(f"line {number} is not a JSON object")
                records.append(record)
    except OSError as error:
        raise RawDataError(f"cannot read raw data: {path}") from error
    return records


def local_path(uri: str) -> Path:
    """Return the local path of a file:// URI; other schemes are rejected."""
    parsed = urlparse(uri)
    if parsed.scheme != "file":
        raise RawDataError(f"unsupported raw URI scheme: {parsed.scheme or 'none'}")
    path = parsed.path
    if parsed.netloc:
        path = f"//{parsed.netloc}{path}"
    return Path(url2pathname(unquote(path)))
