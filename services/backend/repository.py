"""Replace this read adapter with PostgreSQL when the upstream contract is agreed."""

import json
from datetime import datetime
from pathlib import Path
from typing import Protocol

from .models import Incident


class IncidentRepository(Protocol):
    data_mode: str
    snapshot_at: datetime

    def list_incidents(self) -> list[Incident]: ...

    def get_incident(self, incident_id: str) -> Incident | None: ...


class DemoIncidentRepository:
    data_mode = "demo"

    def __init__(self, fixture_path: Path | None = None) -> None:
        path = fixture_path or Path(__file__).with_name("demo.json")
        payload = json.loads(path.read_text(encoding="utf-8"))
        self.snapshot_at = datetime.fromisoformat(payload["snapshot_at"])
        self._incidents = {
            item["id"]: Incident.model_validate(item) for item in payload["incidents"]
        }

    def list_incidents(self) -> list[Incident]:
        return sorted(self._incidents.values(), key=lambda item: item.detected_at, reverse=True)

    def get_incident(self, incident_id: str) -> Incident | None:
        return self._incidents.get(incident_id)
