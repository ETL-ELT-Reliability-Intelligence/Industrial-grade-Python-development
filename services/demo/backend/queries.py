"""Application queries over the upstream read model, independent of HTTP."""

from .models import Incident, IncidentList, IncidentStatus, IncidentSummary, Overview, Severity
from .repository import IncidentRepository


class IncidentQueries:
    def __init__(self, repository: IncidentRepository) -> None:
        self.repository = repository

    def overview(self) -> Overview:
        incidents = self.repository.list_incidents()
        active = [item for item in incidents if item.status != IncidentStatus.RESOLVED]
        return Overview(
            active_incidents=len(active),
            critical_active_incidents=sum(item.severity == Severity.CRITICAL for item in active),
            resolved_incidents=len(incidents) - len(active),
            affected_datasets=len({dataset for item in active for dataset in item.affected_dataset_ids}),
            data_mode=self.repository.data_mode,
            snapshot_at=self.repository.snapshot_at,
        )

    def list_incidents(self, status: IncidentStatus | None = None) -> IncidentList:
        items = [
            item for item in self.repository.list_incidents()
            if status is None or item.status == status
        ]
        return IncidentList(
            items=[IncidentSummary.model_validate(item.model_dump()) for item in items],
            total=len(items),
            data_mode=self.repository.data_mode,
            snapshot_at=self.repository.snapshot_at,
        )

    def get_incident(self, incident_id: str) -> Incident | None:
        return self.repository.get_incident(incident_id)
