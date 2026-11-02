"""Durable resume, immutability and current-result projection in PostgreSQL."""

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from contracts.quality import QualityStatus
from services.stream_processor.handlers import StreamEventHandler
from storage.postgres import (
    PostgresDatasetRepository,
    PostgresMetricRepository,
    PostgresPipelineRunRepository,
    PostgresQualityRepository,
)
from tests.storage.support import DatabaseTestCase
from tests.stream_processor.test_evaluation import suite, event_for
from tests.stream_processor.test_handlers import NOW


class EvaluationStorageTests(DatabaseTestCase):
    def handler(self, minimum=1):
        return StreamEventHandler(
            PostgresDatasetRepository(self.connection),
            PostgresPipelineRunRepository(self.connection),
            PostgresMetricRepository(self.connection),
            PostgresQualityRepository(self.connection),
            suites={"orders": suite(minimum)},
            clock=lambda: NOW,
            sleep=lambda _: None,
        )

    def test_new_handler_resumes_without_recalculating_and_history_is_immutable(self):
        quality = PostgresQualityRepository(self.connection)
        with TemporaryDirectory() as directory:
            event = event_for(Path(directory))
            first = self.handler().handle_dataset_ingested(event)
            with patch(
                "services.stream_processor.handlers.read_raw",
                side_effect=AssertionError("unexpected reread"),
            ):
                self.assertEqual(
                    self.handler(100).handle_dataset_ingested(event), first
                )
            second = self.handler(100).handle_dataset_ingested(
                event, request_id="changed-rule"
            )
            self.assertEqual(second.decision.status, QualityStatus.BLOCK)
            self.assertEqual(quality.get_evaluation(first.evaluation_id), first)
            self.assertEqual(len(quality.list_evaluations("orders", "b1")), 2)
            self.handler().handle_dataset_ingested(event)
            self.assertEqual(
                quality.get_quality_decision("orders", "b1").status, QualityStatus.BLOCK
            )
            with self.assertRaisesRegex(ValueError, "immutable"):
                quality.save_evaluation(
                    first.model_copy(update={"decision": second.decision})
                )

    def test_interrupted_check_is_resumed_using_saved_evidence(self):
        from services.stream_processor.evaluation import execute_check

        with TemporaryDirectory() as directory:
            event = event_for(Path(directory))

            def interrupt(check, event):
                if check.check_id == "orders.id":
                    raise KeyboardInterrupt()
                return execute_check(check, event)

            with patch(
                "services.stream_processor.evaluation.execute_check",
                side_effect=interrupt,
            ):
                with self.assertRaises(KeyboardInterrupt):
                    self.handler().handle_dataset_ingested(event)
            with patch(
                "services.stream_processor.handlers.read_raw",
                side_effect=AssertionError("unexpected reread"),
            ):
                resumed = self.handler().handle_dataset_ingested(event)
            self.assertEqual(resumed.decision.status, QualityStatus.PASS)
            self.assertEqual([len(c.attempts) for c in resumed.checks], [1, 2])
