"""HTTP contract and bounded SQL reads for evaluation history."""

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from services.backend.evaluations import PostgresEvaluationReader
from services.backend.main import create_app
from services.backend.repository import StorageUnavailable
from tests.application.evaluation_support import evaluation_record
from tests.application.support import MemoryRepository


@pytest.fixture
def history():
    repository = MemoryRepository()
    repository.evaluations = [
        evaluation_record("new"),
        evaluation_record("old", completed=True),
    ]
    with TestClient(create_app(repository_factory=repository.factory)) as client:
        yield client, repository


def test_history_pagination_and_detail(history):
    client, _ = history
    first = client.get(
        "/api/v1/datasets/orders/evaluations?scope_id=window-1&limit=1"
    ).json()
    assert (first["limit"], first["offset"], first["has_more"]) == (1, 0, True)
    assert first["items"][0]["evaluation_id"] == "new"
    assert first["items"][0]["state"] == "PENDING"
    assert first["items"][0]["decision_status"] is None
    assert "checks" not in first["items"][0]
    second = client.get(
        "/api/v1/datasets/orders/evaluations?scope_id=window-1&limit=1&offset=1"
    ).json()
    assert second["has_more"] is False
    assert second["items"][0]["decision_status"] == "BLOCK"
    detail = client.get("/api/v1/datasets/orders/evaluations/old").json()
    assert detail["evaluation"]["state"] == "COMPLETED"
    assert detail["evaluation"]["checks"][0]["result"]["observation"]["row_count"] == 0
    assert detail["evaluation"]["checks"][0]["baseline"]["median"] == 120
    assert detail["created_at"] and detail["updated_at"]
    for query in ("scope_id=unknown", "scope_id=window-1&offset=99"):
        assert (
            client.get(f"/api/v1/datasets/orders/evaluations?{query}").json()["items"]
            == []
        )


@pytest.mark.parametrize(
    "suffix", ["/evaluations?scope_id=window-1", "/evaluations/old"]
)
def test_unknown_dataset(history, suffix):
    assert history[0].get("/api/v1/datasets/missing" + suffix).status_code == 404


def test_unknown_or_foreign_evaluation(history):
    client, _ = history
    for path in ("orders/evaluations/missing", "empty/evaluations/old"):
        response = client.get("/api/v1/datasets/" + path)
        assert response.status_code == 404
        assert response.json()["detail"] == "Evaluation not found"


@pytest.mark.parametrize(
    "query",
    [
        "",
        "scope_id=",
        "scope_id=%20%20",
        "scope_id=x&limit=0",
        "scope_id=x&limit=201",
        "scope_id=x&offset=-1",
        "scope_id=x&limit=abc",
    ],
)
def test_invalid_history_parameters(history, query):
    assert (
        history[0].get(f"/api/v1/datasets/orders/evaluations?{query}").status_code
        == 422
    )


def test_storage_failure_is_not_empty_history(history):
    client, repository = history

    def fail(*args):
        raise StorageUnavailable

    repository.list_evaluations = fail
    repository.get_evaluation = fail
    for path in ("evaluations?scope_id=window-1", "evaluations/old"):
        assert client.get(f"/api/v1/datasets/orders/{path}").status_code == 503


def test_list_sql_is_bounded_and_does_not_load_executions():
    connection = MagicMock()
    connection.execute.return_value.fetchall.return_value = []
    assert (
        PostgresEvaluationReader(connection).list_evaluations("orders", "scope", 3, 7)
        == []
    )
    connection.execute.assert_called_once()
    sql, params = connection.execute.call_args.args
    assert "ORDER BY sequence DESC LIMIT %s OFFSET %s" in sql
    assert "quality_executions" not in sql
    assert params == ("orders", "scope", 3, 7)
