"""Prepare dashboard evidence by running existing ingestion and quality services.

This explicit local utility never runs during backend startup or API requests.
"""

import argparse
import os
from pathlib import Path
from urllib.parse import urlencode

from contracts.events import DatasetIngestedEvent
from pipelines.airflow.load_postgres import load_dataset_batch
from services.ingestion.publisher import InMemoryPublisher
from services.ingestion.service import IngestionService
from services.ingestion.sources import CsvSource
from services.ingestion.storage import LocalRawStore
from services.stream_processor.handlers import StreamEventHandler
from storage.postgres import (
    PostgresDatasetRepository,
    PostgresMetricRepository,
    PostgresPipelineRunRepository,
    PostgresQualityRepository,
    connect,
)

DATASET_ID = "ingested-app-orders"
SAMPLE_CSV = Path(__file__).parent / "samples" / "orders.csv"


def prepare_sample(dsn: str, raw_root: Path, run_id: str) -> DatasetIngestedEvent:
    """Load sample CSV records and persist quality measured from their raw file.

    The local publisher delivers events synchronously without a Kafka broker.
    A repeated run ID reuses its committed batch. PostgreSQL loading and quality
    writes share a transaction; the source file remains in the raw store.
    """
    store = LocalRawStore(raw_root)
    publisher = InMemoryPublisher()
    with connect(dsn) as connection, connection.transaction():
        # Serialize this utility so concurrent runs cannot allocate the same batch.
        connection.execute("SELECT pg_advisory_xact_lock(81734622)")
        event = IngestionService(store, publisher).ingest(
            CsvSource(DATASET_ID, SAMPLE_CSV), run_id=run_id
        )
        load_dataset_batch(connection, store, event.dataset_id, event.batch_id)
        handler = StreamEventHandler(
            datasets=PostgresDatasetRepository(connection),
            pipelines=PostgresPipelineRunRepository(connection),
            metrics=PostgresMetricRepository(connection),
            quality=PostgresQualityRepository(connection),
        )
        for published in publisher.events:
            handler.handle(published)
    return event


def main() -> None:
    """Prepare a sample in the migrated development database used by the backend."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default="first-load")
    parser.add_argument("--raw-root", type=Path, default=Path("data/app-sample"))
    args = parser.parse_args()
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        parser.error("Set DATABASE_URL to a migrated development database")
    event = prepare_sample(dsn, args.raw_root, args.run_id)
    print(f"Loaded sample records: {event.records}; source: {event.raw_uri}")
    print(
        "Dashboard: /?"
        + urlencode({"dataset": event.dataset_id, "scope": event.batch_id})
    )


if __name__ == "__main__":
    main()
