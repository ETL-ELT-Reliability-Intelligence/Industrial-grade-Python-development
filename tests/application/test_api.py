from pathlib import Path

from fastapi.testclient import TestClient
import pytest

from services.backend.main import create_app
from services.backend.repository import DemoIncidentRepository


@pytest.fixture
def client():
    with TestClient(create_app()) as client:
        yield client


def test_overview_counts_match_active_incidents(client):
    overview = client.get("/api/v1/overview").json()
    items = client.get("/api/v1/incidents").json()["items"]
    active = [item for item in items if item["status"] != "resolved"]
    assert overview["active_incidents"] == len(active)
    assert overview["critical_active_incidents"] == sum(item["severity"] == "critical" for item in active)
    assert overview["affected_datasets"] == len({id for item in active for id in item["affected_dataset_ids"]})
    assert overview["resolved_incidents"] == len(items) - len(active)
    assert overview["data_mode"] == "demo"


@pytest.mark.parametrize("status", ["open", "investigating", "resolved"])
def test_status_filter_and_detail_agree(client, status):
    response = client.get("/api/v1/incidents", params={"status": status})
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == len(payload["items"]) == 1
    for summary in payload["items"]:
        assert summary["status"] == status
        assert "observations" not in summary
        detail = client.get(f"/api/v1/incidents/{summary['id']}").json()
        assert all(detail[key] == value for key, value in summary.items())
        assert detail["observations"]


def test_demo_exposes_evidence_and_potential_impact(client):
    detail = client.get("/api/v1/incidents/inc-042").json()
    observation = next(item for item in detail["observations"] if item["dataset_id"] == "orders")
    assert observation["actual"] < observation["expected"]
    assert "orders" in detail["affected_dataset_ids"]
    assert "customer_ltv" not in detail["affected_dataset_ids"]
    assert "customer_ltv" in detail["impact"]["potential_dataset_ids"]
    assert detail["root_cause_candidates"][0]["dataset_id"] == "orders"
    times = [event["at"] for event in detail["timeline"]]
    assert times == sorted(times)


def test_unknown_incident_and_invalid_filter(client):
    response = client.get("/api/v1/incidents/missing")
    assert response.status_code == 404
    assert response.json() == {"detail": "Incident not found"}
    assert client.get("/api/v1/incidents?status=unknown").status_code == 422


def test_empty_repository_returns_zero_counts():
    fixture = Path(__file__).with_name("fixtures") / "empty.json"
    with TestClient(create_app(DemoIncidentRepository(fixture))) as client:
        assert client.get("/api/v1/incidents").json()["items"] == []
        assert client.get("/api/v1/overview").json()["active_incidents"] == 0


def test_health_static_assets_and_openapi(client):
    assert client.get("/health").json() == {"status": "ok", "data_mode": "demo"}
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "Демонстрационные данные" in response.text
    for path in ["/assets/app.js", "/assets/styles.css", "/docs"]:
        assert client.get(path).status_code == 200
    assert client.get("/api/v1/nonexistent").status_code == 404
    paths = client.get("/openapi.json").json()["paths"]
    assert "/api/v1/incidents/{incident_id}" in paths
