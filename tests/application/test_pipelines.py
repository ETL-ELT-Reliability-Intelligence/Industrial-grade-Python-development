"""Pipeline catalog/history HTTP contracts and bounded parameterized SQL reads."""

from datetime import timedelta
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from contracts.state import PipelineRun, PipelineRunStatus
from services.backend.main import create_app
from services.backend.pipelines import PostgresPipelineReader
from services.backend.repository import StorageUnavailable
from tests.application.support import NOW, MemoryRepository


@pytest.fixture
def history():
    repository = MemoryRepository()
    repository.pipeline_runs = [
        PipelineRun(
            pipeline_id="orders",
            run_id="running",
            status=PipelineRunStatus.RUNNING,
            started_at=NOW + timedelta(hours=1),
        ),
        PipelineRun(
            pipeline_id="orders",
            run_id="a-success",
            status=PipelineRunStatus.SUCCESS,
            started_at=NOW,
            finished_at=NOW + timedelta(minutes=1),
        ),
        PipelineRun(
            pipeline_id="orders",
            run_id="b-failed",
            status=PipelineRunStatus.FAILED,
            started_at=NOW,
            finished_at=NOW + timedelta(minutes=2),
            error="Source unavailable <script>alert(1)</script>",
        ),
        PipelineRun(
            pipeline_id="another-pipeline",
            run_id="foreign",
            status=PipelineRunStatus.RUNNING,
            started_at=NOW + timedelta(days=1),
        ),
    ]
    with TestClient(create_app(repository_factory=repository.factory)) as client:
        yield client, repository


def test_catalog_is_unique_ordered_and_paginated(history):
    client, _ = history
    first = client.get("/api/v1/pipelines?limit=1").json()
    assert first == {
        "items": [{"pipeline_id": "another-pipeline"}],
        "limit": 1,
        "offset": 0,
        "has_more": True,
    }
    assert client.get("/api/v1/pipelines?limit=1&offset=1").json() == {
        "items": [{"pipeline_id": "orders"}],
        "limit": 1,
        "offset": 1,
        "has_more": False,
    }
    assert client.get("/api/v1/pipelines?offset=99").json()["items"] == []


def test_history_order_pagination_and_exact_stored_facts(history):
    client, repository = history
    pages = [
        client.get(
            "/api/v1/pipeline-runs",
            params={"pipeline_id": "orders", "limit": 1, "offset": offset},
        ).json()
        for offset in range(3)
    ]
    assert [page["items"][0]["run_id"] for page in pages] == [
        "running",
        "b-failed",
        "a-success",
    ]
    assert [page["has_more"] for page in pages] == [True, True, False]
    expected = {
        run.run_id: run.model_dump(mode="json") for run in repository.pipeline_runs
    }
    for offset, page in enumerate(pages):
        assert (page["limit"], page["offset"]) == (1, offset)
        assert page["items"][0] == expected[page["items"][0]["run_id"]]
    default_page = client.get(
        "/api/v1/pipeline-runs", params={"pipeline_id": "orders"}
    ).json()
    assert (
        default_page["limit"],
        default_page["offset"],
        default_page["has_more"],
    ) == (
        50,
        0,
        False,
    )
    assert len(default_page["items"]) == 3


def test_missing_pipeline_and_empty_catalog_are_empty_pages(history):
    client, repository = history
    assert client.get(
        "/api/v1/pipeline-runs", params={"pipeline_id": "unknown"}
    ).json() == {"items": [], "limit": 50, "offset": 0, "has_more": False}
    repository.pipeline_runs.clear()
    assert client.get("/api/v1/pipelines").json() == {
        "items": [],
        "limit": 50,
        "offset": 0,
        "has_more": False,
    }


def test_pipeline_id_is_an_opaque_query_parameter(history):
    client, repository = history
    pipeline_id = " pipeline / окно? & # + % ' OR '1'='1 "
    stored = repository.pipeline_runs[0].model_copy(update={"pipeline_id": pipeline_id})
    repository.pipeline_runs.append(stored)
    response = client.get("/api/v1/pipeline-runs", params={"pipeline_id": pipeline_id})
    assert response.status_code == 200
    assert response.json()["items"] == [stored.model_dump(mode="json")]


@pytest.mark.parametrize("path", ["/pipelines", "/pipeline-runs"])
@pytest.mark.parametrize(
    "query", [{"limit": 0}, {"limit": 201}, {"limit": "abc"}, {"offset": -1}]
)
def test_invalid_pagination(history, path, query):
    params = {"pipeline_id": "orders", **query} if path == "/pipeline-runs" else query
    assert history[0].get("/api/v1" + path, params=params).status_code == 422


@pytest.mark.parametrize("params", [{}, {"pipeline_id": ""}, {"pipeline_id": " \t\n"}])
def test_pipeline_id_is_required_and_nonblank(history, params):
    assert history[0].get("/api/v1/pipeline-runs", params=params).status_code == 422


def test_storage_failure_is_503_not_empty_history(history):
    client, repository = history

    def unavailable(*args):
        raise StorageUnavailable("secret connection details")

    repository.list_pipelines = unavailable
    repository.list_pipeline_runs = unavailable
    for path in ("/pipelines", "/pipeline-runs?pipeline_id=orders"):
        response = client.get("/api/v1" + path)
        assert response.status_code == 503
        assert response.json() == {"detail": "State store unavailable"}


def test_queries_fetch_only_requested_page_and_one_extra(history):
    client, repository = history
    repository.list_pipelines = MagicMock(wraps=repository.list_pipelines)
    repository.list_pipeline_runs = MagicMock(wraps=repository.list_pipeline_runs)
    assert client.get("/api/v1/pipelines?limit=2&offset=3").status_code == 200
    repository.list_pipelines.assert_called_once_with(3, 3)
    assert (
        client.get(
            "/api/v1/pipeline-runs?pipeline_id=orders&limit=2&offset=7"
        ).status_code
        == 200
    )
    repository.list_pipeline_runs.assert_called_once_with("orders", 3, 7)


def test_sql_reads_are_bounded_and_pipeline_id_is_parameterized():
    connection = MagicMock()
    connection.execute.return_value.fetchall.return_value = []
    reader = PostgresPipelineReader(connection)
    assert reader.list_pipelines(3, 7) == []
    sql, params = connection.execute.call_args.args
    assert "SELECT DISTINCT pipeline_id" in sql
    assert "ORDER BY pipeline_id ASC LIMIT %s OFFSET %s" in sql
    assert params == (3, 7)
    pipeline_id = "x' OR '1'='1 / ? & #"
    assert reader.list_pipeline_runs(pipeline_id, 3, 7) == []
    sql, params = connection.execute.call_args.args
    assert "ORDER BY started_at DESC, run_id DESC LIMIT %s OFFSET %s" in sql
    assert pipeline_id not in sql
    assert params == (pipeline_id, 3, 7)


def test_read_adapter_restores_common_pipeline_contract():
    connection = MagicMock()
    row = {
        "pipeline_id": "orders",
        "run_id": "scheduled / ? #",
        "status": "failed",
        "started_at": NOW,
        "finished_at": NOW + timedelta(minutes=2),
        "error": "Source unavailable",
    }
    connection.execute.return_value.fetchall.return_value = [row]
    result = PostgresPipelineReader(connection).list_pipeline_runs("orders", 2, 0)
    assert len(result) == 1
    assert isinstance(result[0], PipelineRun)
    assert result[0].status is PipelineRunStatus.FAILED
    assert result[0].model_dump(mode="json") == {
        **row,
        "started_at": NOW.isoformat().replace("+00:00", "Z"),
        "finished_at": (NOW + timedelta(minutes=2)).isoformat().replace("+00:00", "Z"),
    }


def test_pipeline_page_is_available_without_database_and_api_is_read_only(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with TestClient(create_app()) as client:
        response = client.get("/pipelines")
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/html")
        assert "/assets/pipelines.js" in response.text
        paths = client.get("/openapi.json").json()["paths"]
        for path in ("/api/v1/pipelines", "/api/v1/pipeline-runs"):
            assert set(paths[path]) == {"get"}
        for path in ("/api/v1/pipelines", "/api/v1/pipeline-runs?pipeline_id=orders"):
            assert client.get(path).status_code == 503
