"""PostgreSQL integration tests for the stream processor and backend read path."""

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from fastapi.testclient import TestClient

from contracts.events import (
    DataQualityFailedEvent,
    DatasetIngestedEvent,
    PipelineFailedEvent,
    PipelineFinishedEvent,
    PipelineStartedEvent,
)
from contracts.metrics import MetricKind
from contracts.quality import CheckStatus, QualityStatus
from contracts.state import PipelineRunStatus
from services.backend.main import create_app
from services.backend.postgres import open_repository
from services.stream_processor.handlers import StreamEventHandler
from services.stream_processor.publisher import InMemoryPublisher
from storage.postgres import (
    PostgresDatasetRepository,
    PostgresMetricRepository,
    PostgresPipelineRunRepository,
    PostgresQualityRepository,
)
from tests.storage.support import DatabaseTestCase, TEST_DATABASE_URL

NOW = datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc)
MINUTE = timedelta(minutes=1)


def _write_jsonl(directory: Path, name: str, records: list[dict]) -> str:
    path = directory / f"{name}.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")
    return path.as_uri()


class StreamProcessorPostgresTests(DatabaseTestCase):
    """End-to-end persistence and backend visibility against a real PostgreSQL instance."""

    def setUp(self):
        super().setUp()
        self.datasets = PostgresDatasetRepository(self.connection)
        self.pipelines = PostgresPipelineRunRepository(self.connection)
        self.metrics = PostgresMetricRepository(self.connection)
        self.quality = PostgresQualityRepository(self.connection)
        self.publisher = InMemoryPublisher()
        self.handler = StreamEventHandler(
            self.datasets,
            self.pipelines,
            self.metrics,
            self.quality,
            publisher=self.publisher,
            clock=lambda: NOW + 10 * MINUTE,
        )
        self.client = TestClient(
            create_app(repository_factory=lambda: open_repository(TEST_DATABASE_URL))
        )
        self.client.__enter__()
        self.addCleanup(self.client.__exit__, None, None, None)

    def test_ingested_batches_persist_and_are_visible_via_backend_api(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows_normal = [{"order_id": i, "amount": 10.5 * (i + 1)} for i in range(10)]
            for idx in range(1, 4):
                uri = _write_jsonl(root, f"b-{idx}", rows_normal)
                self.handler.handle(DatasetIngestedEvent(
                    dataset_id="orders",
                    batch_id=f"b-{idx}",
                    records=10,
                    received_at=NOW + idx * MINUTE,
                    schema_version=1,
                    raw_uri=uri,
                ))

            # Repeat b-3 to verify idempotency on real PostgreSQL tables
            self.handler.handle(DatasetIngestedEvent(
                dataset_id="orders",
                batch_id="b-3",
                records=10,
                received_at=NOW + 3 * MINUTE,
                schema_version=1,
                raw_uri=_write_jsonl(root, "b-3", rows_normal),
            ))

            # Fourth batch drops volume -> row_count FAIL and policy BLOCK
            uri_drop = _write_jsonl(root, "b-4", rows_normal[:3])
            self.handler.handle(DatasetIngestedEvent(
                dataset_id="orders",
                batch_id="b-4",
                records=3,
                received_at=NOW + 4 * MINUTE,
                schema_version=1,
                raw_uri=uri_drop,
            ))

        batches = self.datasets.list_batches("orders")
        self.assertEqual([b.batch_id for b in batches], ["b-4", "b-3", "b-2", "b-1"])
        self.assertEqual([b.records for b in batches], [3, 10, 10, 10])

        history = self.metrics.list_metric_history("orders", MetricKind.ROW_COUNT)
        self.assertEqual([p.value for p in history], [3, 10, 10, 10])

        results_b4 = {r.check_id: r for r in self.quality.list_check_results("orders", "b-4")}
        self.assertEqual(results_b4["orders.row_count"].status, CheckStatus.FAIL)
        self.assertEqual(
            self.quality.get_quality_decision("orders", "b-4").status,
            QualityStatus.BLOCK,
        )
        self.assertEqual(len(self.publisher.events), 1)
        self.assertIsInstance(self.publisher.events[0], DataQualityFailedEvent)

        # Verify backend endpoints reflect stream-processed records without seed
        api_batches = self.client.get("/api/v1/datasets/orders/batches").json()["items"]
        self.assertEqual([item["batch_id"] for item in api_batches], ["b-4", "b-3", "b-2", "b-1"])
        api_quality = self.client.get("/api/v1/datasets/orders/quality?scope_id=b-4").json()
        self.assertEqual(api_quality["decision"]["status"], "BLOCK")

    def test_pipeline_events_round_trip_in_postgres(self):
        self.handler.handle(
            PipelineStartedEvent(pipeline_id="orders_daily", run_id="run-1", started_at=NOW)
        )
        self.handler.handle(
            PipelineFinishedEvent(
                pipeline_id="orders_daily",
                run_id="run-1",
                started_at=NOW,
                finished_at=NOW + MINUTE,
            )
        )
        self.handler.handle(
            PipelineFailedEvent(
                pipeline_id="orders_daily",
                run_id="run-2",
                started_at=NOW + 2 * MINUTE,
                failed_at=NOW + 3 * MINUTE,
                error="upstream timeout",
            )
        )

        run1 = self.pipelines.get_pipeline_run("orders_daily", "run-1")
        run2 = self.pipelines.get_pipeline_run("orders_daily", "run-2")
        self.assertEqual(run1.status, PipelineRunStatus.SUCCESS)
        self.assertEqual(run2.status, PipelineRunStatus.FAILED)
        self.assertEqual(run2.error, "upstream timeout")
