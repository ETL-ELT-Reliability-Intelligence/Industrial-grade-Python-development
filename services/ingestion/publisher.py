"""Event publishing. Kafka client is imported lazily so tests need no broker."""

from typing import Protocol

from contracts.events import Event


class EventPublisher(Protocol):
    """Boundary for publishing ingestion events to a configured destination."""

    def publish(self, event: Event) -> None:
        """Publish one event or raise when the destination reports a delivery failure."""
        ...


class InMemoryPublisher:
    """Collect events in publication order for local execution and tests."""

    def __init__(self) -> None:
        self.events: list[Event] = []

    def publish(self, event: Event) -> None:
        self.events.append(event)


class KafkaPublisher:
    """Publish keyed Kafka events and wait for delivery.

    The idempotent producer first acquires a producer id from the broker. A freshly started broker answers
    "coordinator load in progress" for 10-20 seconds, so the default wait is longer than that.

    Args:
        bootstrap_servers: Kafka bootstrap servers.
        flush_timeout: Seconds to wait for the delivery of each event.
    """

    def __init__(self, bootstrap_servers: str, flush_timeout: float = 45.0):
        from confluent_kafka import Producer

        self._producer = Producer({"bootstrap.servers": bootstrap_servers, "enable.idempotence": True})
        self._flush_timeout = flush_timeout

    def publish(self, event: Event) -> None:
        errors: list[object] = []

        def on_delivery(err: object, _msg: object) -> None:
            if err:
                errors.append(err)

        try:
            self._producer.produce(event.topic, key=event.key, value=event.model_dump_json(), on_delivery=on_delivery)
        except BufferError as exc:  # local producer queue is full
            raise RuntimeError(f"Kafka delivery failed: {exc}") from exc
        if self._producer.flush(self._flush_timeout) or errors:
            raise RuntimeError(f"Kafka delivery failed: {errors[0] if errors else 'timeout'}")
