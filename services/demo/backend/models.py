"""Public response contracts; detection and RCA belong to upstream services."""

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class IncidentStatus(StrEnum):
    """Lifecycle state of an incident in the demo snapshot."""

    OPEN = "open"
    INVESTIGATING = "investigating"
    RESOLVED = "resolved"


class Severity(StrEnum):
    """Display severity assigned to an incident by its source."""

    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class IncidentSummary(BaseModel):
    """Incident identity, status and affected datasets for list views."""

    id: str
    title: str
    summary: str
    status: IncidentStatus
    severity: Severity
    detected_at: datetime
    affected_dataset_ids: list[str]


class Observation(BaseModel):
    """Expected and actual metric values with units and observation time."""

    id: str
    dataset_id: str
    metric: str
    expected: float
    actual: float
    unit: str
    observed_at: datetime


class RootCauseCandidate(BaseModel):
    """Candidate explanation with a ranking score, not a causal probability."""

    dataset_id: str
    description: str
    score: float = Field(
        ge=0, le=1, description="Ranking score, not causal probability"
    )


class TimelineEvent(BaseModel):
    """Timestamped event in the incident narrative."""

    at: datetime
    description: str


class Impact(BaseModel):
    """Potentially affected datasets, pipelines and dashboards."""

    potential_dataset_ids: list[str]
    potential_pipeline_ids: list[str]
    potential_dashboard_ids: list[str]


class Incident(IncidentSummary):
    """Incident summary expanded with evidence, candidate causes and impact."""

    observations: list[Observation]
    root_cause_candidates: list[RootCauseCandidate]
    timeline: list[TimelineEvent]
    impact: Impact


class IncidentList(BaseModel):
    """Incident summaries with their count and snapshot provenance."""

    items: list[IncidentSummary]
    total: int
    data_mode: str
    snapshot_at: datetime


class Overview(BaseModel):
    """Aggregate incident counts and the provenance of their snapshot."""

    active_incidents: int
    critical_active_incidents: int
    resolved_incidents: int
    affected_datasets: int
    data_mode: str
    snapshot_at: datetime
