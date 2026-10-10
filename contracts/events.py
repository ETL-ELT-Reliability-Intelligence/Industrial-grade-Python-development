"""Event contracts shared between components over Kafka.

Producers and consumers depend on these models, not on each other.
"""

import json
from enum import Enum
from typing import Literal, Self
from uuid import UUID, uuid4

from pydantic import AwareDatetime, Field, model_validator

from contracts.base import Contract, NonBlankText, Rate

CONTRACT_VERSION = 1


class EventType(str, Enum):
    DATASET_INGESTED = "dataset.ingested"
    PIPELINE_STARTED = "pipeline.started"
    PIPELINE_FINISHED = "pipeline.finished"
    PIPELINE_FAILED = "pipeline.failed"
    SCHEMA_CHANGED = "schema.changed"
    DATA_QUALITY_FAILED = "data.quality.failed"
    ANOMALY_DETECTED = "anomaly.detected"


class Event(Contract):
    event_id: UUID = Field(default_factory=uuid4)
    contract_version: int = Field(default=CONTRACT_VERSION, ge=1, le=1)
    event_type: EventType

    @property
    def topic(self) -> str:
        """One topic per event type, named after it."""
        return self.event_type.value

    @property
    def key(self) -> str:
        """Partition key; keeps events of one dataset/pipeline ordered."""
        raise NotImplementedError


class DatasetIngestedEvent(Event):
    event_type: Literal[EventType.DATASET_INGESTED] = EventType.DATASET_INGESTED
    dataset_id: NonBlankText
    batch_id: NonBlankText
    records: int = Field(ge=0)
    received_at: AwareDatetime
    schema_version: int = Field(ge=1)
    raw_uri: NonBlankText | None = None

    @property
    def key(self) -> str:
        return self.dataset_id


class SchemaChangedEvent(Event):
    event_type: Literal[EventType.SCHEMA_CHANGED] = EventType.SCHEMA_CHANGED
    dataset_id: NonBlankText
    batch_id: NonBlankText
    previous_version: int = Field(ge=1)
    new_version: int = Field(ge=1)
    detected_at: AwareDatetime

    @model_validator(mode="after")
    def changed_version(self) -> Self:
        if self.previous_version == self.new_version:
            raise ValueError("new_version must differ from previous_version")
        return self

    @property
    def key(self) -> str:
        return self.dataset_id


class PipelineEvent(Event):
    pipeline_id: NonBlankText
    run_id: NonBlankText
    started_at: AwareDatetime

    @property
    def key(self) -> str:
        return self.pipeline_id


class PipelineStartedEvent(PipelineEvent):
    event_type: Literal[EventType.PIPELINE_STARTED] = EventType.PIPELINE_STARTED


class PipelineFinishedEvent(PipelineEvent):
    event_type: Literal[EventType.PIPELINE_FINISHED] = EventType.PIPELINE_FINISHED
    finished_at: AwareDatetime

    @property
    def duration_seconds(self) -> float:
        """Elapsed time, derived locally and omitted from the wire contract."""
        return (self.finished_at - self.started_at).total_seconds()

    @model_validator(mode="after")
    def consistent_times(self) -> Self:
        if self.finished_at < self.started_at:
            raise ValueError("finished_at must not be before started_at")
        return self


class PipelineFailedEvent(PipelineEvent):
    event_type: Literal[EventType.PIPELINE_FAILED] = EventType.PIPELINE_FAILED
    failed_at: AwareDatetime
    error: NonBlankText | None

    @model_validator(mode="after")
    def chronological(self) -> Self:
        if self.failed_at < self.started_at:
            raise ValueError("failed_at must not be before started_at")
        return self


class DataQualityFailedEvent(Event):
    """One deterministic check failed; evidence remains in the check result."""

    event_type: Literal[EventType.DATA_QUALITY_FAILED] = EventType.DATA_QUALITY_FAILED
    dataset_id: NonBlankText
    scope_id: NonBlankText
    check_id: NonBlankText
    reason_code: NonBlankText
    failed_at: AwareDatetime

    @property
    def key(self) -> str:
        return self.dataset_id


class AnomalyDetectedEvent(Event):
    """Detector evidence with a finite ranking score in [0, 1]."""

    event_type: Literal[EventType.ANOMALY_DETECTED] = EventType.ANOMALY_DETECTED
    anomaly_id: NonBlankText
    dataset_id: NonBlankText
    batch_id: NonBlankText | None
    metric: NonBlankText
    expected: float = Field(allow_inf_nan=False)
    actual: float = Field(allow_inf_nan=False)
    score: Rate
    detector: NonBlankText
    model_version: NonBlankText | None
    detected_at: AwareDatetime

    @property
    def key(self) -> str:
        return self.dataset_id


_MODELS: dict[EventType, type[Event]] = {
    EventType.DATASET_INGESTED: DatasetIngestedEvent,
    EventType.SCHEMA_CHANGED: SchemaChangedEvent,
    EventType.PIPELINE_STARTED: PipelineStartedEvent,
    EventType.PIPELINE_FINISHED: PipelineFinishedEvent,
    EventType.PIPELINE_FAILED: PipelineFailedEvent,
    EventType.DATA_QUALITY_FAILED: DataQualityFailedEvent,
    EventType.ANOMALY_DETECTED: AnomalyDetectedEvent,
}


def parse_event(payload: str | bytes) -> Event:
    """Deserialize a Kafka message into its concrete event model."""
    data = json.loads(payload)
    if not isinstance(data, dict):
        raise ValueError("event payload must be a JSON object")
    try:
        model = _MODELS[EventType(data.get("event_type"))]
    except (ValueError, TypeError, KeyError) as error:
        raise ValueError("unknown event_type") from error
    # Keep JSON validation mode: strict UUID/datetime fields accept their wire
    # strings here while Python construction still requires native objects.
    return model.model_validate_json(payload)
