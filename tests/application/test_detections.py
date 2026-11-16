"""Detector endpoints retain ownership, missing evidence and numeric zeros."""

from fastapi.testclient import TestClient

from services.backend.main import create_app
from services.backend.models import DetectionEvidence
from tests.application.evaluation_support import evaluation_record
from tests.application.support import MemoryRepository


def test_detection_endpoint_distinguishes_missing_results_and_checks_ownership():
    repository = MemoryRepository()
    repository.evaluations = [evaluation_record("one")]
    with TestClient(create_app(repository_factory=repository.factory)) as client:
        path = "/api/v1/datasets/orders/evaluations/one/detections"
        result = client.get(path)
        assert result.status_code == 200
        assert result.json()["rule"]["status"] == "SKIPPED"
        assert result.json()["ml"]["reason"] == "not_applicable"
        for bad in (path.replace("orders", "empty"), path.replace("one", "unknown")):
            assert client.get(bad).status_code == 404
        repository.detections["one"] = DetectionEvidence(
            dataset_id="orders", batch_id="window-1", evaluation_id="one",
            rule={"detector": "rules", "status": "PASS", "model_version": "r1"},
            ml={"detector": "lag_gradient_boosting", "status": "WARN", "actual": 0,
                "expected": 100, "lower": 80, "upper": 120, "model_version": "m1"},
        )
        assert client.get(path).json()["ml"]["actual"] == 0
