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


class OlistSource:
    """One table of the Brazilian E-Commerce (Olist) dataset, one month window per fetch.

    `orders` is cut by purchase month; the other tables keep only rows that belong to the orders of the
    same month, so joins downstream stay consistent. `volume_factor` < 1 drops rows of the batch while
    the source still "succeeds" - the silent partial load the platform must detect.
    Empty CSV cells become None so downstream null-rate checks see real nulls.
    """

    FILES = {
        "orders": "olist_orders_dataset.csv",
        "order_items": "olist_order_items_dataset.csv",
        "order_payments": "olist_order_payments_dataset.csv",
        "customers": "olist_customers_dataset.csv",
    }

    def __init__(self, dataset_id: str, root: Path, window: int = 0, volume_factor: float = 1.0):
        if dataset_id not in self.FILES:
            raise ValueError(f"unknown Olist table {dataset_id!r}; expected one of {sorted(self.FILES)}")
        if window < 0 or not 0 <= volume_factor <= 1:
            raise ValueError("window must be >= 0 and volume_factor within [0, 1]")
        self.dataset_id = dataset_id
        self._root = root
        self._window = window
        self._volume_factor = volume_factor

    def fetch(self) -> SourceBatch:
        orders = self._read("orders")
        months = sorted({row["order_purchase_timestamp"][:7] for row in orders if row["order_purchase_timestamp"]})
        if self._window >= len(months):
            raise ValueError(f"window {self._window} is out of range: dataset has {len(months)} months")
        month_orders = [r for r in orders if (r["order_purchase_timestamp"] or "")[:7] == months[self._window]]

        if self.dataset_id == "orders":
            rows = month_orders
        elif self.dataset_id == "customers":
            customer_ids = {r["customer_id"] for r in month_orders}
            rows = [r for r in self._read("customers") if r["customer_id"] in customer_ids]
        else:
            order_ids = {r["order_id"] for r in month_orders}
            rows = [r for r in self._read(self.dataset_id) if r["order_id"] in order_ids]

        return SourceBatch(self.dataset_id, rows[: int(len(rows) * self._volume_factor)])

    def _read(self, table: str) -> list[Record]:
        with (self._root / self.FILES[table]).open(newline="", encoding="utf-8") as f:
            return [{key: (value or None) for key, value in row.items()} for row in csv.DictReader(f)]
