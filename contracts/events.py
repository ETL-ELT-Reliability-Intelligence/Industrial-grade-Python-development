"""Event contracts shared between components over Kafka.

Producers and consumers depend on these models, not on each other.
"""

import json
from enum import Enum

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class EventType(str, Enum):
    DATASET_INGESTED = "dataset.ingested"
    PIPELINE_STARTED = "pipeline.started"
    PIPELINE_FINISHED = "pipeline.finished"
    PIPELINE_FAILED = "pipeline.failed"
    SCHEMA_CHANGED = "schema.changed"


class Event(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

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
    event_type: EventType = EventType.DATASET_INGESTED
    dataset_id: str = Field(min_length=1)
    batch_id: str = Field(min_length=1)
    records: int = Field(ge=0)
    received_at: AwareDatetime
    schema_version: int = Field(ge=1)
    raw_uri: str | None = None

    @property
    def key(self) -> str:
        return self.dataset_id


class SchemaChangedEvent(Event):
    event_type: EventType = EventType.SCHEMA_CHANGED
    dataset_id: str = Field(min_length=1)
    previous_version: int = Field(ge=1)
    new_version: int = Field(ge=1)
    detected_at: AwareDatetime

    @property
    def key(self) -> str:
        return self.dataset_id


class PipelineEvent(Event):
    pipeline_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    occurred_at: AwareDatetime

    @property
    def key(self) -> str:
        return self.pipeline_id


class PipelineStartedEvent(PipelineEvent):
    event_type: EventType = EventType.PIPELINE_STARTED


class PipelineFinishedEvent(PipelineEvent):
    event_type: EventType = EventType.PIPELINE_FINISHED
    duration_seconds: float = Field(ge=0)


class PipelineFailedEvent(PipelineEvent):
    event_type: EventType = EventType.PIPELINE_FAILED
    error: str


_MODELS: dict[EventType, type[Event]] = {
    EventType.DATASET_INGESTED: DatasetIngestedEvent,
    EventType.SCHEMA_CHANGED: SchemaChangedEvent,
    EventType.PIPELINE_STARTED: PipelineStartedEvent,
    EventType.PIPELINE_FINISHED: PipelineFinishedEvent,
    EventType.PIPELINE_FAILED: PipelineFailedEvent,
}


def parse_event(payload: str | bytes) -> Event:
    """Deserialize a Kafka message into its concrete event model."""
    data = json.loads(payload)
    return _MODELS[EventType(data["event_type"])].model_validate(data)
