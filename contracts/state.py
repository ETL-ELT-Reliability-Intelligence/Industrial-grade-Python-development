"""Records persisted in the state store and exchanged between components.

These contracts describe stored facts; they do not evaluate or decide anything.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from contracts._validation import (
    _count, _number, _optional_text, _rate, _text, _timestamp, _version,
)


class PipelineRunStatus(str, Enum):
    """Lifecycle of one pipeline run."""

    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass(frozen=True)
class Batch:
    """One ingested batch; object_key points to its raw data in object storage."""

    dataset_id: str
    batch_id: str
    records: int
    received_at: datetime
    schema_version: int
    object_key: str | None = None

    def __post_init__(self) -> None:
        """Validate identifiers, counts, versions and the optional object key."""
        _text(self.dataset_id, "dataset_id")
        _text(self.batch_id, "batch_id")
        _count(self.records, "records")
        _timestamp(self.received_at, "received_at")
        _version(self.schema_version, "schema_version")
        _optional_text(self.object_key, "object_key")


@dataclass(frozen=True)
class PipelineRun:
    """One run of a pipeline; finished_at is set exactly for finished runs."""

    pipeline_id: str
    run_id: str
    status: PipelineRunStatus
    started_at: datetime
    finished_at: datetime | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        """Validate the status against finish time and error consistency."""
        _text(self.pipeline_id, "pipeline_id")
        _text(self.run_id, "run_id")
        if not isinstance(self.status, PipelineRunStatus):
            raise ValueError("status must be a PipelineRunStatus")
        _timestamp(self.started_at, "started_at")
        if self.status is PipelineRunStatus.RUNNING:
            if self.finished_at is not None:
                raise ValueError("a running pipeline has no finished_at")
        else:
            if self.finished_at is None:
                raise ValueError("a finished pipeline requires finished_at")
            _timestamp(self.finished_at, "finished_at")
            if self.finished_at < self.started_at:
                raise ValueError("finished_at must not be before started_at")
        _optional_text(self.error, "error")
        if self.error is not None and self.status is not PipelineRunStatus.FAILED:
            raise ValueError("error is only allowed for a failed run")


@dataclass(frozen=True)
class Anomaly:
    """One detected deviation. score ranks anomalies and is not a probability."""

    anomaly_id: str
    dataset_id: str
    metric: str
    expected: float
    actual: float
    score: float
    detector: str
    detected_at: datetime
    batch_id: str | None = None
    model_version: str | None = None

    def __post_init__(self) -> None:
        """Validate identifiers, numeric evidence and the detection time."""
        for name in ("anomaly_id", "dataset_id", "metric", "detector"):
            _text(getattr(self, name), name)
        _optional_text(self.batch_id, "batch_id")
        _optional_text(self.model_version, "model_version")
        _number(self.expected, "expected")
        _number(self.actual, "actual")
        _rate(self.score, "score")
        _timestamp(self.detected_at, "detected_at")


@dataclass(frozen=True)
class UserAction:
    """An action of a person or a system on an entity, e.g. an incident."""

    action_id: str
    actor: str
    action_type: str
    target_type: str
    target_id: str
    performed_at: datetime
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate identifiers, the aware time and that details is an object."""
        for name in ("action_id", "actor", "action_type", "target_type", "target_id"):
            _text(getattr(self, name), name)
        _timestamp(self.performed_at, "performed_at")
        if not isinstance(self.details, dict):
            raise ValueError("details must be a dict")
