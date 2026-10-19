"""Reference consumer for the event contracts in `events.py`.

Any component reading from Kafka (stream processor, incident engine, ...) can reuse it. Delivery is
at-least-once, so consumers must tolerate duplicates: `Deduplicator` skips events seen before.
Messages that violate the contract are collected in `invalid` instead of crashing the consumer.
"""

from .events import Event, EventType, parse_event


class Deduplicator:
    def __init__(self) -> None:
        self._seen: set[str] = set()

    def is_new(self, event: Event) -> bool:
        if event.dedup_key in self._seen:
            return False
        self._seen.add(event.dedup_key)
        return True


class KafkaEventConsumer:
    """Kafka client is imported lazily so tests need no broker."""

    def __init__(self, bootstrap_servers: str, group_id: str, topics: list[str] | None = None):
        from confluent_kafka import Consumer

        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self._consumer.subscribe(topics or [t.value for t in EventType])
        self.invalid: list[tuple[bytes, Exception]] = []

    def poll(self, timeout: float = 1.0) -> Event | None:
        """Next valid event, or None if nothing arrived. Call `commit()` after handling it."""
        message = self._consumer.poll(timeout)
        if message is None:
            return None
        if message.error():
            from confluent_kafka import KafkaError

            if message.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                return None  # a topic appears with its first event; not an error for a subscriber
            raise RuntimeError(f"Kafka consume failed: {message.error()}")
        try:
            return parse_event(message.value())
        except Exception as exc:  # malformed JSON, unknown event_type, failed validation
            self.invalid.append((message.value(), exc))
            self.commit()  # a poison message must not block the partition
            return None

    def commit(self) -> None:
        self._consumer.commit(asynchronous=False)

    def close(self) -> None:
        self._consumer.close()
