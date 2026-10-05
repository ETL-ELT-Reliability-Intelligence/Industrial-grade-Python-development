"""Public response contracts; detection and RCA belong to upstream services."""

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class IncidentStatus(StrEnum):
    OPEN = "open"
    INVESTIGATING = "investigating"
    RESOLVED = "resolved"


class Severity(StrEnum):
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class IncidentSummary(BaseModel):
    id: str
    title: str
    summary: str
    status: IncidentStatus
    severity: Severity
    detected_at: datetime
    affected_dataset_ids: list[str]


class Observation(BaseModel):
    id: str
    dataset_id: str
    metric: str
    expected: float
    actual: float
    unit: str
    observed_at: datetime


class RootCauseCandidate(BaseModel):
    dataset_id: str
    description: str
    score: float = Field(ge=0, le=1, description="Ranking score, not causal probability")


class TimelineEvent(BaseModel):
    at: datetime
    description: str


class Impact(BaseModel):
    potential_dataset_ids: list[str]
    potential_pipeline_ids: list[str]
    potential_dashboard_ids: list[str]


class Incident(IncidentSummary):
    observations: list[Observation]
    root_cause_candidates: list[RootCauseCandidate]
    timeline: list[TimelineEvent]
    impact: Impact


class IncidentList(BaseModel):
    items: list[IncidentSummary]
    total: int
    data_mode: str
    snapshot_at: datetime


class Overview(BaseModel):
    active_incidents: int
    critical_active_incidents: int
    resolved_incidents: int
    affected_datasets: int
    data_mode: str
    snapshot_at: datetime
