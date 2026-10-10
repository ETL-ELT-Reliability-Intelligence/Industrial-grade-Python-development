"""Records persisted in the state store and exchanged between components.

These contracts describe stored facts; they do not evaluate or decide anything.
"""

from enum import Enum
from typing import Any, Self

from pydantic import AwareDatetime, Field, model_validator

from contracts.base import Contract, Count, NonBlankText, Rate


class PipelineRunStatus(str, Enum):
    """Lifecycle of one pipeline run."""

    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class Batch(Contract):
    """One ingested batch; object_key points to its raw data in object storage."""

    dataset_id: NonBlankText
    batch_id: NonBlankText
    records: Count
    received_at: AwareDatetime
    schema_version: int = Field(ge=1)
    object_key: NonBlankText | None = None


class PipelineRun(Contract):
    """One run of a pipeline; finished_at is set exactly for finished runs."""

    pipeline_id: NonBlankText
    run_id: NonBlankText
    status: PipelineRunStatus
    started_at: AwareDatetime
    finished_at: AwareDatetime | None = None
    error: NonBlankText | None = None

    @model_validator(mode="after")
    def consistent_status(self) -> Self:
        if self.status is PipelineRunStatus.RUNNING:
            if self.finished_at is not None:
                raise ValueError("a running pipeline has no finished_at")
        else:
            if self.finished_at is None:
                raise ValueError("a finished pipeline requires finished_at")
            if self.finished_at < self.started_at:
                raise ValueError("finished_at must not be before started_at")
        if self.error is not None and self.status is not PipelineRunStatus.FAILED:
            raise ValueError("error is only allowed for a failed run")
        return self


class Anomaly(Contract):
    """One detected deviation. score ranks anomalies and is not a probability."""

    anomaly_id: NonBlankText
    dataset_id: NonBlankText
    metric: NonBlankText
    expected: float = Field(allow_inf_nan=False)
    actual: float = Field(allow_inf_nan=False)
    score: Rate
    detector: NonBlankText
    detected_at: AwareDatetime
    batch_id: NonBlankText | None = None
    model_version: NonBlankText | None = None


class UserAction(Contract):
    """An action of a person or a system on an entity, e.g. an incident."""

    action_id: NonBlankText
    actor: NonBlankText
    action_type: NonBlankText
    target_type: NonBlankText
    target_id: NonBlankText
    performed_at: AwareDatetime
    details: dict[str, Any] = Field(default_factory=dict)
