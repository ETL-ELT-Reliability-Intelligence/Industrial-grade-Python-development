"""Raw data and schema-version storage. MinIO can implement the same protocol."""

import json
from pathlib import Path
from typing import Protocol

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
