"""Data sources. They only supply data and know nothing about reliability."""

import csv
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

Record = dict[str, Any]


@dataclass(frozen=True)
class SourceBatch:
    dataset_id: str
    records: list[Record]


class Source(Protocol):
    dataset_id: str

    def fetch(self) -> SourceBatch: ...


class MockApiSource:
    """Deterministic fake orders API.

    `volume_factor` < 1 simulates the upstream returning partial data while
    still reporting success - the main scenario the platform must detect.
    """

    dataset_id = "orders"
    PAYMENT_METHODS = ("card", "cash", "other")

    def __init__(self, base_rows: int = 1000, volume_factor: float = 1.0, seed: int = 0):
        if base_rows < 0 or not 0 <= volume_factor <= 1:
            raise ValueError("base_rows must be >= 0 and volume_factor within [0, 1]")
        self._rows = int(base_rows * volume_factor)
        self._seed = seed

    def fetch(self) -> SourceBatch:
        rng = random.Random(self._seed)
        records = [
            {
                "order_id": i,
                "customer_id": rng.randint(1, 500),
                "amount": round(rng.uniform(5, 500), 2),
                "payment_method": rng.choices(self.PAYMENT_METHODS, weights=(70, 20, 10))[0],
            }
            for i in range(self._rows)
        ]
        return SourceBatch(self.dataset_id, records)


class CsvSource:
    def __init__(self, dataset_id: str, path: Path):
        self.dataset_id = dataset_id
        self._path = path

    def fetch(self) -> SourceBatch:
        with self._path.open(newline="", encoding="utf-8") as f:
            return SourceBatch(self.dataset_id, list(csv.DictReader(f)))
