"""Event publishing. Kafka client is imported lazily so tests need no broker."""

from typing import Protocol

from contracts.events import Event


class EventPublisher(Protocol):
    def publish(self, event: Event) -> None: ...


class InMemoryPublisher:
    def __init__(self) -> None:
        self.events: list[Event] = []

    def publish(self, event: Event) -> None:
        self.events.append(event)


class KafkaPublisher:
    def __init__(self, bootstrap_servers: str):
        from confluent_kafka import Producer

        self._producer = Producer({"bootstrap.servers": bootstrap_servers, "enable.idempotence": True})

    def publish(self, event: Event) -> None:
        errors: list[object] = []

        def on_delivery(err: object, _msg: object) -> None:
            if err:
                errors.append(err)

        self._producer.produce(event.topic, key=event.key, value=event.model_dump_json(), on_delivery=on_delivery)
        if self._producer.flush(10) or errors:
            raise RuntimeError(f"Kafka delivery failed: {errors[0] if errors else 'timeout'}")
