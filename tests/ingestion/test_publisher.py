import sys
from datetime import datetime, timezone
from types import ModuleType

import pytest

from contracts.events import DatasetIngestedEvent
from services.ingestion.publisher import KafkaPublisher

EVENT = DatasetIngestedEvent(
    dataset_id="orders",
    batch_id="2026-09-26-001",
    records=1,
    received_at=datetime(2026, 9, 26, tzinfo=timezone.utc),
    schema_version=1,
    raw_uri="file:///x",
)


class FakeProducer:
    """Delivers (or fails) synchronously on flush, like the real client's callbacks."""

    def __init__(self, config, *, error=None, pending=0, produce_raises=None):
        self.config, self.error, self.pending, self.produce_raises = config, error, pending, produce_raises
        self.sent, self._callbacks = [], []

    def produce(self, topic, key, value, on_delivery):
        if self.produce_raises:
            raise self.produce_raises
        self.sent.append((topic, key, value))
        self._callbacks.append(on_delivery)

    def flush(self, timeout):
        for callback in self._callbacks:
            callback(self.error, None)
        self._callbacks.clear()
        return self.pending


@pytest.fixture
def kafka(monkeypatch):
    def make(**options):
        module = ModuleType("confluent_kafka")
        module.Producer = lambda config: FakeProducer(config, **options)
        monkeypatch.setitem(sys.modules, "confluent_kafka", module)
        return KafkaPublisher("broker:9092")

    return make


def test_publishes_event_json_keyed_by_dataset_with_idempotence(kafka):
    publisher = kafka()
    publisher.publish(EVENT)

    assert publisher._producer.config == {"bootstrap.servers": "broker:9092", "enable.idempotence": True}
    assert publisher._producer.sent == [("dataset.ingested", "orders", EVENT.model_dump_json())]


def test_delivery_error_raises(kafka):
    with pytest.raises(RuntimeError, match="delivery failed: broker down"):
        kafka(error="broker down").publish(EVENT)


def test_flush_timeout_raises(kafka):
    with pytest.raises(RuntimeError, match="timeout"):
        kafka(pending=1).publish(EVENT)


def test_full_producer_queue_raises_runtime_error(kafka):
    with pytest.raises(RuntimeError, match="queue is full"):
        kafka(produce_raises=BufferError("queue is full")).publish(EVENT)
