"""Kafka event contracts shared by producers and consumers.

Events are transport messages, not the source of truth: consumers persist their
state in PostgreSQL. Timestamps are timezone-aware and serialized as ISO 8601.
Every event carries a unique event_id so that redelivery can be detected.
Incident events belong to the Incident Engine and have no contract here yet.
"""

import json
from dataclasses import dataclass, fields
from datetime import datetime
from enum import Enum
from typing import Any, ClassVar, get_args, get_type_hints

from contracts._validation import (
    _count, _number, _optional_text, _rate, _text, _timestamp, _version,
)

CONTRACT_VERSION = 1


class EventType(str, Enum):
    """Event names used as Kafka message types (ARCHITECTURE.md, section 5.3)."""

    DATASET_INGESTED = "dataset.ingested"
    PIPELINE_STARTED = "pipeline.started"
    PIPELINE_FINISHED = "pipeline.finished"
    PIPELINE_FAILED = "pipeline.failed"
    SCHEMA_CHANGED = "schema.changed"
    DATA_QUALITY_FAILED = "data.quality.failed"
    ANOMALY_DETECTED = "anomaly.detected"
    INCIDENT_CREATED = "incident.created"
    INCIDENT_UPDATED = "incident.updated"


@dataclass(frozen=True)
class DatasetIngested:
    """Ingestion stored one batch; this says nothing about the batch quality."""

    event_id: str
    dataset_id: str
    batch_id: str
    records: int
    received_at: datetime
    schema_version: int

    event_type: ClassVar[EventType] = EventType.DATASET_INGESTED

    def __post_init__(self) -> None:
        """Validate identifiers, record count, schema version and receive time."""
        for name in ("event_id", "dataset_id", "batch_id"):
            _text(getattr(self, name), name)
        _count(self.records, "records")
        _timestamp(self.received_at, "received_at")
        _version(self.schema_version, "schema_version")


@dataclass(frozen=True)
class PipelineStarted:
    """A pipeline run started."""

    event_id: str
    pipeline_id: str
    run_id: str
    started_at: datetime

    event_type: ClassVar[EventType] = EventType.PIPELINE_STARTED

    def __post_init__(self) -> None:
        """Validate identifiers and the aware start time."""
        for name in ("event_id", "pipeline_id", "run_id"):
            _text(getattr(self, name), name)
        _timestamp(self.started_at, "started_at")


@dataclass(frozen=True)
class PipelineFinished:
    """A pipeline run finished successfully."""

    event_id: str
    pipeline_id: str
    run_id: str
    started_at: datetime
    finished_at: datetime

    event_type: ClassVar[EventType] = EventType.PIPELINE_FINISHED

    def __post_init__(self) -> None:
        """Validate identifiers and require finishing not before the start."""
        for name in ("event_id", "pipeline_id", "run_id"):
            _text(getattr(self, name), name)
        _timestamp(self.started_at, "started_at")
        _timestamp(self.finished_at, "finished_at")
        if self.finished_at < self.started_at:
            raise ValueError("finished_at must not be before started_at")


@dataclass(frozen=True)
class PipelineFailed:
    """A pipeline run failed; error is None when no message is available."""

    event_id: str
    pipeline_id: str
    run_id: str
    started_at: datetime
    failed_at: datetime
    error: str | None

    event_type: ClassVar[EventType] = EventType.PIPELINE_FAILED

    def __post_init__(self) -> None:
        """Validate identifiers, chronological order and the optional error text."""
        for name in ("event_id", "pipeline_id", "run_id"):
            _text(getattr(self, name), name)
        _timestamp(self.started_at, "started_at")
        _timestamp(self.failed_at, "failed_at")
        if self.failed_at < self.started_at:
            raise ValueError("failed_at must not be before started_at")
        _optional_text(self.error, "error")


@dataclass(frozen=True)
class SchemaChanged:
    """A batch arrived with a schema version different from the previous one."""

    event_id: str
    dataset_id: str
    batch_id: str
    previous_schema_version: int
    schema_version: int
    changed_at: datetime

    event_type: ClassVar[EventType] = EventType.SCHEMA_CHANGED

    def __post_init__(self) -> None:
        """Validate identifiers, versions and require an actual version change."""
        for name in ("event_id", "dataset_id", "batch_id"):
            _text(getattr(self, name), name)
        _version(self.previous_schema_version, "previous_schema_version")
        _version(self.schema_version, "schema_version")
        if self.previous_schema_version == self.schema_version:
            raise ValueError("schema_version must differ from previous_schema_version")
        _timestamp(self.changed_at, "changed_at")


@dataclass(frozen=True)
class DataQualityFailed:
    """One deterministic check failed; details are stored with the check result."""

    event_id: str
    dataset_id: str
    scope_id: str
    check_id: str
    reason_code: str
    failed_at: datetime

    event_type: ClassVar[EventType] = EventType.DATA_QUALITY_FAILED

    def __post_init__(self) -> None:
        """Validate identifiers, the stable reason code and the aware time."""
        for name in ("event_id", "dataset_id", "scope_id", "check_id", "reason_code"):
            _text(getattr(self, name), name)
        _timestamp(self.failed_at, "failed_at")


@dataclass(frozen=True)
class AnomalyDetected:
    """A detector found a deviation; score is a ranking value in [0, 1]."""

    event_id: str
    anomaly_id: str
    dataset_id: str
    batch_id: str | None
    metric: str
    expected: float
    actual: float
    score: float
    detector: str
    model_version: str | None
    detected_at: datetime

    event_type: ClassVar[EventType] = EventType.ANOMALY_DETECTED

    def __post_init__(self) -> None:
        """Validate identifiers, numeric evidence and the detector description."""
        for name in ("event_id", "anomaly_id", "dataset_id", "metric", "detector"):
            _text(getattr(self, name), name)
        _optional_text(self.batch_id, "batch_id")
        _optional_text(self.model_version, "model_version")
        _number(self.expected, "expected")
        _number(self.actual, "actual")
        _rate(self.score, "score")
        _timestamp(self.detected_at, "detected_at")


Event = (
    DatasetIngested | PipelineStarted | PipelineFinished | PipelineFailed
    | SchemaChanged | DataQualityFailed | AnomalyDetected
)

_EVENT_CLASSES = (
    DatasetIngested, PipelineStarted, PipelineFinished, PipelineFailed,
    SchemaChanged, DataQualityFailed, AnomalyDetected,
)
_REGISTRY = {cls.event_type: cls for cls in _EVENT_CLASSES}


def _is_datetime(hint: Any) -> bool:
    """Return True for datetime and optional datetime annotations."""
    return hint is datetime or datetime in get_args(hint)


def _parse_datetime(value: Any, name: str) -> datetime:
    """Parse an ISO 8601 string, raising ValueError for anything else."""
    if not isinstance(value, str):
        raise ValueError(f"{name} must be an ISO 8601 string")
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        raise ValueError(f"{name} must be an ISO 8601 timestamp") from None


def event_to_dict(event: Event) -> dict[str, Any]:
    """Return the JSON-compatible wire representation of an event."""
    if not isinstance(event, _EVENT_CLASSES):
        raise ValueError("event must be one of the supported event contracts")
    payload: dict[str, Any] = {
        "event_type": event.event_type.value,
        "contract_version": CONTRACT_VERSION,
    }
    for field in fields(event):
        value = getattr(event, field.name)
        payload[field.name] = value.isoformat() if isinstance(value, datetime) else value
    return payload


def event_from_dict(payload: dict[str, Any]) -> Event:
    """Build a validated event from its wire representation.

    Missing and unexpected fields, unknown types and unsupported contract
    versions raise ValueError, as do values rejected by the event contract.
    """
    if not isinstance(payload, dict):
        raise ValueError("event payload must be a JSON object")
    data = dict(payload)
    version = data.pop("contract_version", None)
    if version != CONTRACT_VERSION:
        raise ValueError(f"unsupported contract_version: {version!r}")
    try:
        event_type = EventType(data.pop("event_type", None))
    except ValueError:
        raise ValueError("unknown event_type") from None
    cls = _REGISTRY.get(event_type)
    if cls is None:
        raise ValueError(f"unsupported event_type: {event_type.value}")
    names = {field.name for field in fields(cls)}
    if data.keys() != names:
        raise ValueError(
            f"invalid {event_type.value} fields: missing={sorted(names - data.keys())}, "
            f"unexpected={sorted(data.keys() - names)}"
        )
    hints = get_type_hints(cls)
    for name, value in data.items():
        if value is not None and _is_datetime(hints[name]):
            data[name] = _parse_datetime(value, name)
    return cls(**data)


def encode_event(event: Event) -> bytes:
    """Serialize an event to canonical UTF-8 JSON for a Kafka message value."""
    return json.dumps(
        event_to_dict(event), ensure_ascii=False, allow_nan=False,
        separators=(",", ":"), sort_keys=True,
    ).encode("utf-8")


def decode_event(data: bytes | str) -> Event:
    """Deserialize and validate a Kafka message value."""
    try:
        payload = json.loads(data)
    except (TypeError, ValueError) as error:
        raise ValueError("event is not valid JSON") from error
    return event_from_dict(payload)
