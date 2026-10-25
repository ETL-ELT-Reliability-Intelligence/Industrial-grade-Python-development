from contextlib import contextmanager

import pytest
from fastapi.testclient import TestClient

from services.backend.main import create_app
from services.backend.repository import StorageUnavailable
from tests.application.support import MemoryRepository


@pytest.fixture
def client():
    with TestClient(
        create_app(repository_factory=MemoryRepository().factory)
    ) as client:
        yield client


def test_catalog_pagination_and_empty_dataset(client):
    first = client.get("/api/v1/datasets?limit=1").json()
    assert first == {
        "items": [client.get("/api/v1/datasets/empty").json()],
        "limit": 1,
        "offset": 0,
        "has_more": True,
    }
    second = client.get("/api/v1/datasets?limit=1&offset=1").json()
    assert second["items"][0]["dataset_id"] == "orders"
    assert second["has_more"] is False
    assert client.get("/api/v1/datasets?offset=99").json()["items"] == []
    for suffix in ("batches", "quality-scopes"):
        assert client.get(f"/api/v1/datasets/empty/{suffix}").json()["items"] == []


def test_quality_preserves_evidence_and_scope_meaning(client):
    batches = client.get("/api/v1/datasets/orders/batches").json()["items"]
    assert batches[0]["records"] == 0
    scopes = client.get("/api/v1/datasets/orders/quality-scopes").json()["items"]
    assert scopes[0]["scope_id"] != batches[0]["batch_id"]
    quality = client.get("/api/v1/datasets/orders/quality?scope_id=window-1").json()
    checks = {item["result"]["check_id"]: item["result"] for item in quality["checks"]}
    assert checks["volume"]["observation"]["row_count"] == 0
    assert checks["volume"]["status"] == "FAIL"
    assert checks["nulls"]["observation"]["null_rate"] is None
    assert checks["nulls"]["status"] == "UNKNOWN"
    assert quality["decision"]["status"] == "BLOCK"
    assert quality["decided_at"]


def test_unknown_scope_is_missing_evidence_not_pass(client):
    assert client.get("/api/v1/datasets/orders/quality?scope_id=missing").json() == {
        "dataset_id": "orders",
        "scope_id": "missing",
        "checks": [],
        "decision": None,
        "decided_at": None,
    }


@pytest.mark.parametrize(
    "suffix", ["", "/batches", "/quality-scopes", "/quality?scope_id=window-1"]
)
def test_unknown_dataset(client, suffix):
    response = client.get("/api/v1/datasets/missing" + suffix)
    assert response.status_code == 404
    assert response.json() == {"detail": "Dataset not found"}


@pytest.mark.parametrize("query", ["limit=0", "limit=201", "offset=-1", "limit=abc"])
@pytest.mark.parametrize(
    "path", ["/datasets", "/datasets/orders/batches", "/datasets/orders/quality-scopes"]
)
def test_invalid_pagination(client, path, query):
    assert client.get("/api/v1" + path + "?" + query).status_code == 422


@pytest.mark.parametrize("query", ["", "?scope_id=", "?scope_id=%20%20"])
def test_invalid_scope(client, query):
    assert client.get("/api/v1/datasets/orders/quality" + query).status_code == 422


def test_unconfigured_storage_and_health(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with TestClient(create_app()) as client:
        assert client.get("/health").json() == {"status": "ok"}
        for path in ("/ready", "/api/v1/datasets"):
            assert client.get(path).status_code == 503
        assert client.get("/docs").status_code == 200
        assert client.get("/").status_code == 200


def test_storage_failure_is_sanitized_and_resource_closes():
    events = []

    @contextmanager
    def unavailable():
        try:
            raise StorageUnavailable("secret DSN must not reach HTTP")
            yield  # pragma: no cover
        finally:
            events.append("closed")

    with TestClient(create_app(repository_factory=unavailable)) as client:
        response = client.get("/api/v1/datasets")
        assert response.status_code == 503
        assert response.json() == {"detail": "State store unavailable"}
    assert events == ["closed"]


def test_openapi_and_readiness(client):
    assert client.get("/ready").status_code == 200
    paths = client.get("/openapi.json").json()["paths"]
    assert "/api/v1/datasets/{dataset_id}/quality" in paths
    assert "/api/v1/incidents" not in paths
