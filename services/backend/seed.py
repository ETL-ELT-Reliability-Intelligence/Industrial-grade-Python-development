"""Explicit local sample: python -m services.backend.seed (uses DATABASE_URL).

Never runs during startup. Refuses to overwrite either sample dataset.
"""

from datetime import datetime, timedelta, timezone
import os

from contracts.quality import (
    NullRateConstraint, NullRateObservation, QualityContext, RowCountConstraint, RowCountObservation,
)
from contracts.state import Batch
from data_quality.checks.null_rate import check_null_rate
from data_quality.checks.row_count import check_row_count
from data_quality.policies import evaluate_quality
from storage.postgres import PostgresDatasetRepository, PostgresQualityRepository, connect

SAMPLE_DATASET = "sample-app-orders"
EMPTY_DATASET = "sample-app-empty"


def seed(dsn: str) -> None:
    now = datetime.now(timezone.utc)
    with connect(dsn) as connection, connection.transaction():
        # Serialize this explicit utility; do not replace somebody else's saved facts.
        connection.execute("SELECT pg_advisory_xact_lock(81734621)")
        if connection.execute(
            "SELECT 1 FROM datasets WHERE dataset_id IN (%s, %s)", (SAMPLE_DATASET, EMPTY_DATASET),
        ).fetchone():
            raise ValueError("Sample datasets already exist; nothing changed")
        datasets = PostgresDatasetRepository(connection)
        quality = PostgresQualityRepository(connection)
        datasets.ensure_dataset(EMPTY_DATASET)
        for index, records in enumerate((120, 0), start=1):
            datasets.upsert_batch(Batch(
                dataset_id=SAMPLE_DATASET, batch_id=f"sample-batch-{index}", records=records,
                received_at=now - timedelta(minutes=3 - index), schema_version=1,
            ))
        # The window deliberately differs from both batch IDs.
        context = QualityContext(dataset_id=SAMPLE_DATASET, scope_id="sample-window-1")
        results = (
            check_row_count(RowCountObservation(row_count=0),
                            RowCountConstraint(min_count=100), check_id="sample-volume", context=context),
            check_null_rate(NullRateObservation(null_rate=None),
                            NullRateConstraint(max_null_rate=0.05), check_id="sample-nulls",
                            context=QualityContext(dataset_id=SAMPLE_DATASET, scope_id=context.scope_id, column="email")),
        )
        for result in results:
            quality.save_check_result(result)
        quality.save_quality_decision(SAMPLE_DATASET, context.scope_id, evaluate_quality(results), now)
    print(f"Created TEST DATA: {SAMPLE_DATASET}, {EMPTY_DATASET}. No live ingestion integration implied.")


if __name__ == "__main__":
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("Set DATABASE_URL to a local development database first")
    seed(dsn)
