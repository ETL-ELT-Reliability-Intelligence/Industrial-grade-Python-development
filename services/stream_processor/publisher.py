"""Event publishing for stream processor quality failure and anomaly notifications."""

from datetime import datetime
from typing import Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from contracts.events import AnomalyDetectedEvent, DataQualityFailedEvent, Event
from contracts.quality import CheckResult, DecisionReason
from contracts.state import Anomaly


class EventPublisher(Protocol):
    def publish(self, event: Event) -> None: ...


class NoOpPublisher:
    """Discards events when outbound Kafka publishing is not configured."""

    def publish(self, event: Event) -> None:
        return None


class InMemoryPublisher:
    """Collects published events in memory for unit tests."""

    def __init__(self) -> None:
        self.events: list[Event] = []

    def publish(self, event: Event) -> None:
        self.events.append(event)


class KafkaPublisher:
    """Publishes events to Kafka; confluent-kafka is imported lazily."""

    def __init__(self, bootstrap_servers: str) -> None:
        from confluent_kafka import Producer

        self._producer = Producer({"bootstrap.servers": bootstrap_servers, "enable.idempotence": True})

    def publish(self, event: Event) -> None:
        errors: list[object] = []

        def on_delivery(err: object, _msg: object) -> None:
            if err:
                errors.append(err)

        self._producer.produce(
            event.topic, key=event.key, value=event.model_dump_json(), on_delivery=on_delivery,
        )
        if self._producer.flush(10) or errors:
            error = errors[0] if errors else None
            if error is None or getattr(error, "retriable", lambda: False)():
                raise ConnectionError(f"Kafka delivery failed: {error or 'timeout'}")
            raise RuntimeError(f"Kafka delivery failed: {error}")


def quality_failed_event_id(dataset_id: str, scope_id: str, check_id: str, evaluation_id: str | None = None) -> UUID:
    """Deterministic UUID so redelivered batches emit the same failure event ID."""
    suffix = f":{evaluation_id}" if evaluation_id else ""
    return uuid5(NAMESPACE_URL, f"data.quality.failed:{dataset_id}:{scope_id}:{check_id}{suffix}")


def build_quality_failed_event(result: CheckResult, failed_at: datetime, *, evaluation_id: str | None = None) -> DataQualityFailedEvent:
    """Build a DataQualityFailedEvent from a failed CheckResult with context."""
    if result.context is None:
        raise ValueError("CheckResult must have a QualityContext to emit DataQualityFailedEvent")
    dataset_id = result.context.dataset_id
    scope_id = result.context.scope_id
    return DataQualityFailedEvent(
        event_id=quality_failed_event_id(dataset_id, scope_id, result.check_id, evaluation_id),
        dataset_id=dataset_id,
        scope_id=scope_id,
        check_id=result.check_id,
        reason_code=result.reason_code,
        failed_at=failed_at,
    )


def build_anomaly_event(anomaly: Anomaly) -> AnomalyDetectedEvent:
    """Собирает AnomalyDetectedEvent с детерминированным event_id."""
    return AnomalyDetectedEvent(
        event_id=uuid5(NAMESPACE_URL, f"anomaly.detected:{anomaly.anomaly_id}"),
        anomaly_id=anomaly.anomaly_id,
        dataset_id=anomaly.dataset_id,
        batch_id=anomaly.batch_id,
        metric=anomaly.metric,
        expected=anomaly.expected,
        actual=anomaly.actual,
        score=anomaly.score,
        detector=anomaly.detector,
        model_version=anomaly.model_version,
        detected_at=anomaly.detected_at,
    )


def build_quality_failed_from_reason(
    reason: DecisionReason,
    failed_at: datetime,
    *,
    evaluation_id: str | None = None,
    dataset_id: str | None = None,
    scope_id: str | None = None,
) -> DataQualityFailedEvent:
    """Build a DataQualityFailedEvent from a DecisionReason for BLOCK decisions.

    Uses reason.check_id when available; falls back to dataset_id and scope_id
    only when reason.check_id is None. The event_id is deterministic based on
    dataset_id, scope_id, check_id, reason_code, and evaluation_id.
    """
    if reason.check_id is not None:
        check_id = reason.check_id
    elif dataset_id is not None and scope_id is not None:
        check_id = f"{dataset_id}:{scope_id}"
    else:
        check_id = "unknown"
    event_id = quality_failed_event_id(
        dataset_id or "unknown",
        scope_id or "unknown",
        check_id,
        evaluation_id,
    )
    return DataQualityFailedEvent(
        event_id=event_id,
        dataset_id=dataset_id or "unknown",
        scope_id=scope_id or "unknown",
        check_id=check_id,
        reason_code=reason.code,
        failed_at=failed_at,
    )
