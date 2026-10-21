"""Kafka consumer loop and runtime configuration for the stream processor."""

from dataclasses import dataclass
import os
from typing import Mapping, Protocol

from contracts.consumer import KafkaEventConsumer
from contracts.events import Event, EventType

from .handlers import StreamEventHandler

DEFAULT_TOPICS: tuple[str, ...] = (
    EventType.DATASET_INGESTED.value,
    EventType.PIPELINE_STARTED.value,
    EventType.PIPELINE_FINISHED.value,
    EventType.PIPELINE_FAILED.value,
)


class EventSource(Protocol):
    """Minimal consumer interface so tests can drive the loop without Kafka."""

    def poll(self, timeout: float = 1.0) -> Event | None: ...

    def commit(self) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class StreamProcessorConfig:
    """Connection and consumer settings read from environment or CLI."""

    database_url: str
    kafka_bootstrap_servers: str
    group_id: str = "stream-processor"
    topics: tuple[str, ...] = DEFAULT_TOPICS
    poll_timeout: float = 1.0

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        database_url: str | None = None,
        kafka_bootstrap_servers: str | None = None,
        group_id: str | None = None,
    ) -> "StreamProcessorConfig":
        source = os.environ if env is None else env
        dsn = database_url or source.get("DATABASE_URL")
        if not dsn:
            raise ValueError("DATABASE_URL is required")
        bootstrap = kafka_bootstrap_servers or source.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        resolved_group = group_id or source.get("KAFKA_GROUP_ID", "stream-processor")
        return cls(
            database_url=dsn,
            kafka_bootstrap_servers=bootstrap,
            group_id=resolved_group,
        )


class StreamProcessorRunner:
    """Polls events from Kafka, delegates to the handler, and commits offsets."""

    def __init__(
        self,
        consumer: EventSource,
        handler: StreamEventHandler,
        *,
        poll_timeout: float = 1.0,
    ) -> None:
        self._consumer = consumer
        self._handler = handler
        self._poll_timeout = poll_timeout

    def step(self) -> Event | None:
        """Process at most one event and commit its offset only on success."""
        event = self._consumer.poll(self._poll_timeout)
        if event is None:
            return None
        self._handler.handle(event)
        self._consumer.commit()
        return event

    def run(self, *, max_messages: int | None = None) -> int:
        """Run the consumer loop until max_messages are processed or interrupted."""
        if max_messages is not None and max_messages < 1:
            raise ValueError("max_messages must be a positive integer")
        processed = 0
        while max_messages is None or processed < max_messages:
            if self.step() is not None:
                processed += 1
        return processed

    def close(self) -> None:
        self._consumer.close()


def create_kafka_consumer(config: StreamProcessorConfig) -> KafkaEventConsumer:
    """Instantiate KafkaEventConsumer subscribed to the configured topics."""
    return KafkaEventConsumer(
        config.kafka_bootstrap_servers,
        group_id=config.group_id,
        topics=list(config.topics),
    )
