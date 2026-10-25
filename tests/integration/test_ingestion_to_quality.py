"""ingestion -> raw store -> dataset.ingested -> quality checks, on in-memory repositories (no Kafka, no Postgres)."""

from contracts.events import DatasetIngestedEvent
from contracts.metrics import MetricKind
from contracts.quality import QualityStatus
from services.ingestion.publisher import InMemoryPublisher
from services.ingestion.service import IngestionService
from services.ingestion.sources import MockApiSource
from services.ingestion.storage import LocalRawStore
from tests.stream_processor.test_handlers import NOW, make_handler


def test_ingested_batch_reaches_quality_decision_and_redelivery_changes_nothing(tmp_path):
    published = InMemoryPublisher()
    ingestion = IngestionService(LocalRawStore(tmp_path), published, clock=lambda: NOW)
    handler, datasets, _, metrics, quality, _ = make_handler()

    ingested = ingestion.ingest(MockApiSource(base_rows=20), run_id="r1")
    for event in published.events:
        handler.handle(event)
    decision = quality.get_quality_decision("orders", ingested.batch_id)
    results = list(quality.list_check_results("orders", ingested.batch_id))

    assert isinstance(published.events[0], DatasetIngestedEvent)
    assert datasets.get_batch("orders", ingested.batch_id).records == 20
    assert results and decision is not None and decision.status is not QualityStatus.BLOCK

    # at-least-once: the retried run republishes the same event, the consumer must stay idempotent
    ingestion.ingest(MockApiSource(base_rows=20), run_id="r1")
    handler.handle(published.events[-1])
    assert quality.get_quality_decision("orders", ingested.batch_id) == decision
    assert list(quality.list_check_results("orders", ingested.batch_id)) == results
    assert len(metrics.list_metric_history("orders", MetricKind.ROW_COUNT)) == 1
