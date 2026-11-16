"""Read both detector results from real migrated PostgreSQL."""

from psycopg.types.json import Jsonb

from services.backend.postgres import PostgresReadRepository
from storage.postgres import PostgresQualityRepository, PostgresAnomalyRepository
from tests.storage.support import DatabaseTestCase
from tests.application.evaluation_support import evaluation_record


class DetectionStorageTests(DatabaseTestCase):
    def test_persisted_results_are_visible_and_missing_predictions_are_not_pass(self):
        evaluation = evaluation_record("detect", completed=True).evaluation
        quality = PostgresQualityRepository(self.connection)
        quality.save_evaluation(evaluation)
        repository = PostgresReadRepository(self.connection)
        record = repository.get_evaluation("orders", "detect")
        assert repository.get_detections(record).rule.status == "SKIPPED"
        PostgresAnomalyRepository(self.connection).save_detection("detect", {
            "status": "PASS", "detector": "rules", "model_version": "v1", "checks": [], "anomalies": [],
        })
        self.connection.execute("INSERT INTO order_volume_predictions VALUES (%s, %s)", (
            "detect", Jsonb({"status": "ERROR", "reason": "model runtime version mismatch", "actual": 0, "model_version": "m1"}),
        ))
        result = repository.get_detections(record)
        assert result.rule.status == "PASS"
        assert result.ml.status == "ERROR" and result.ml.actual == 0
        assert "version mismatch" in result.ml.reason
