"""Airflow-independent steps of the Olist pipeline, kept out of the DAG file so they can be unit-tested.

One pipeline run ingests every Olist table for the same month window; each table is its own dataset
(own batch, own `dataset.ingested` event, own checks downstream).
"""

from pathlib import Path
from typing import Any

from services.ingestion.publisher import EventPublisher
from services.ingestion.service import IngestionService
from services.ingestion.sources import OlistSource
from services.ingestion.storage import RawStore

TABLES = ("orders", "order_items", "order_payments", "customers")


def ingest_table(
    table: str,
    *,
    run_id: str,
    window: int,
    volume_factors: dict[str, float],
    olist_dir: Path,
    store: RawStore,
    publisher: EventPublisher,
) -> dict[str, Any]:
    """Ingest one table of the window. `volume_factors` simulates a silent partial load per table.

    Idempotent for a given `run_id`: a task retry republishes the same events instead of a second batch.
    """
    source = OlistSource(table, olist_dir, window=window, volume_factor=volume_factors.get(table, 1.0))
    return IngestionService(store, publisher).ingest(source, run_id=run_id).model_dump(mode="json")
