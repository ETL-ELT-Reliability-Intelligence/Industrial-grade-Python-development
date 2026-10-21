"""ingestion -> quality bridge -> PostgreSQL, with real events and a real (migrated, dedicated) test database."""

import os

import pytest

pytest.importorskip("psycopg")
pytest.importorskip("alembic")

from contracts.quality import QualityStatus  # noqa: E402
from services.ingestion.olist_sample import write_sample  # noqa: E402
from services.ingestion.publisher import InMemoryPublisher  # noqa: E402
from services.ingestion.service import IngestionService  # noqa: E402
from services.ingestion.sources import OlistSource  # noqa: E402
from services.ingestion.storage import LocalRawStore  # noqa: E402
from services.stream_processor.handlers import StreamEventHandler  # noqa: E402
from services.stream_processor.policy import QualityThresholds  # noqa: E402
from storage.postgres import (  # noqa: E402
    PostgresDatasetRepository,
    PostgresMetricRepository,
    PostgresPipelineRunRepository,
    PostgresQualityRepository,
    connect,
)
from tests.storage.support import migrate  # noqa: E402

URL = os.environ.get("TEST_DATABASE_URL")
pytestmark = [pytest.mark.integration, pytest.mark.skipif(not URL, reason="TEST_DATABASE_URL not set")]


# Undelivered Olist orders have no delivery date, so that column is legitimately ~40% null.
THRESHOLDS = QualityThresholds(max_null_rate=0.5)


def build_handler(connection):
    return StreamEventHandler(
        PostgresDatasetRepository(connection),
        PostgresPipelineRunRepository(connection),
        PostgresMetricRepository(connection),
        PostgresQualityRepository(connection),
        thresholds=THRESHOLDS,
    )


def decide(handler, quality, event):
    handler.handle(event)
    return quality.get_quality_decision(event.dataset_id, event.batch_id)


@pytest.fixture
def pipeline(tmp_path):
    migrate(URL)
    connection = connect(URL)
    connection.execute("TRUNCATE datasets CASCADE")
    olist = tmp_path / "olist"
    write_sample(olist, months=5, orders_per_month=100)
    store = LocalRawStore(tmp_path / "raw")
    publisher = InMemoryPublisher()
    service = IngestionService(store, publisher)
    processor = build_handler(connection)
    quality = PostgresQualityRepository(connection)

    def ingest(table, window, volume_factor=1.0, run_id=None):
        event = service.ingest(OlistSource(table, olist, window, volume_factor), run_id=run_id or f"{table}-{window}")
        return event, decide(processor, quality, event)

    yield ingest, quality, processor, publisher
    connection.close()


def test_normal_batches_pass_and_a_silently_partial_one_is_blocked(pipeline):
    ingest, quality, *_ = pipeline
    decisions = [ingest("order_payments", window)[1].status for window in range(3)]
    event, decision = ingest("order_payments", 3, volume_factor=0.4)

    assert decisions == [QualityStatus.PASS] * 3
    assert decision.status is QualityStatus.BLOCK
    assert any("row_count" in str(reason) for reason in decision.reasons)
    assert quality.get_quality_decision("order_payments", event.batch_id) == decision


def test_each_table_is_judged_on_its_own_history(pipeline):
    ingest, *_ = pipeline
    for window in range(3):
        for table in ("orders", "order_items", "customers"):
            assert ingest(table, window)[1].status is QualityStatus.PASS
    assert ingest("order_payments", 3, volume_factor=0.4)[1].status is QualityStatus.PASS  # no history yet


def test_replaying_an_event_does_not_change_the_decision(pipeline):
    ingest, quality, processor, _ = pipeline
    for window in range(3):
        ingest("orders", window)
    event, decision = ingest("orders", 3, volume_factor=0.4)
    assert decision.status is QualityStatus.BLOCK

    replayed = decide(processor, quality, event)
    assert replayed.status == decision.status
    assert [(r.code, r.check_id) for r in replayed.reasons] == [(r.code, r.check_id) for r in decision.reasons]
    results = quality.list_check_results("orders", event.batch_id)
    assert len(results) == len({r.check_id for r in results})


def test_a_new_column_fails_the_schema_check(pipeline, tmp_path):
    ingest, quality, *_ = pipeline
    ingest("customers", 0)
    # the same dataset later arrives with an extra column
    store = LocalRawStore(tmp_path / "raw")
    service = IngestionService(store, InMemoryPublisher())

    class WithExtraColumn:
        dataset_id = "customers"

        def fetch(self):
            batch = OlistSource("customers", tmp_path / "olist", window=1).fetch()
            return type(batch)(batch.dataset_id, [{**r, "loyalty_tier": "gold"} for r in batch.records])

    event = service.ingest(WithExtraColumn(), run_id="extra")
    decision = decide(build_handler(connect(URL)), quality, event)

    assert event.schema_version == 2
    assert decision.status is QualityStatus.BLOCK
    assert any("schema" in str(reason) for reason in decision.reasons)
