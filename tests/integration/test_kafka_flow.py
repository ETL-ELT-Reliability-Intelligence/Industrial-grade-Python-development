"""ingestion -> Kafka -> consumer against a real broker.

Run: docker compose up -d kafka; KAFKA_BOOTSTRAP_SERVERS=localhost:9092 pytest -m integration
"""

import os
import time
import uuid

import pytest

from contracts.consumer import Deduplicator, KafkaEventConsumer
from contracts.events import DatasetIngestedEvent
from services.ingestion.publisher import KafkaPublisher
from services.ingestion.service import IngestionService
from services.ingestion.sources import MockApiSource
from services.ingestion.storage import LocalRawStore

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not BOOTSTRAP, reason="KAFKA_BOOTSTRAP_SERVERS not set"),
]


class UniqueOrders(MockApiSource):
    """Own dataset_id so reruns against a long-lived broker do not see old events."""

    def __init__(self, dataset_id: str, **kwargs):
        super().__init__(**kwargs)
        self.dataset_id = dataset_id


def drain(consumer: KafkaEventConsumer, dataset_id: str, expected: int, timeout: float = 30.0):
    events = []
    deadline = time.monotonic() + timeout
    while len(events) < expected and time.monotonic() < deadline:
        event = consumer.poll(1.0)
        if event is not None and getattr(event, "dataset_id", None) == dataset_id:
            events.append(event)
    return events


@pytest.fixture
def consumer():
    c = KafkaEventConsumer(BOOTSTRAP, group_id=f"test-{uuid.uuid4()}", topics=["dataset.ingested"])
    yield c
    c.close()


def test_event_reaches_consumer_and_retry_is_deduplicated(tmp_path, consumer):
    dataset_id = f"orders-{uuid.uuid4().hex[:8]}"
    service = IngestionService(LocalRawStore(tmp_path), KafkaPublisher(BOOTSTRAP))

    # the same Airflow run is executed twice (task retry) -> the event is delivered twice
    first = service.ingest(UniqueOrders(dataset_id, base_rows=10), run_id="run-1")
    service.ingest(UniqueOrders(dataset_id, base_rows=10), run_id="run-1")

    delivered = drain(consumer, dataset_id, expected=2)
    assert len(delivered) == 2
    assert all(isinstance(e, DatasetIngestedEvent) for e in delivered)
    assert delivered[0] == first

    dedup = Deduplicator()
    assert [e for e in delivered if dedup.is_new(e)] == [first]
