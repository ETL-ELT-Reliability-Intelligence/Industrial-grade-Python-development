"""Read incident snapshots from a local fixture for the isolated demo."""

import json
from datetime import datetime
from pathlib import Path
from typing import Protocol

from .models import Incident


class IncidentRepository(Protocol):
    """Read incidents with the origin and timestamp of their snapshot."""

    data_mode: str
    snapshot_at: datetime

    def list_incidents(self) -> list[Incident]:
        """Return incidents ordered by detection time, newest first."""
        ...

    def get_incident(self, incident_id: str) -> Incident | None:
        """Return the identified incident, or None if absent."""
        ...


class DemoIncidentRepository:
    """Load a validated incident snapshot from JSON without accessing shared storage."""

    data_mode = "demo"

    def __init__(self, fixture_path: Path | None = None) -> None:
        path = fixture_path or Path(__file__).with_name("demo.json")
        payload = json.loads(path.read_text(encoding="utf-8"))
        self.snapshot_at = datetime.fromisoformat(payload["snapshot_at"])
        self._incidents = {
            item["id"]: Incident.model_validate(item) for item in payload["incidents"]
        }

    def list_incidents(self) -> list[Incident]:
        return sorted(
            self._incidents.values(), key=lambda item: item.detected_at, reverse=True
        )

    def get_incident(self, incident_id: str) -> Incident | None:
        return self._incidents.get(incident_id)
