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
        self.sent, self._callbacks, self.flush_timeouts = [], [], []

    def produce(self, topic, key, value, on_delivery):
        if self.produce_raises:
            raise self.produce_raises
        self.sent.append((topic, key, value))
        self._callbacks.append(on_delivery)

    def flush(self, timeout):
        self.flush_timeouts.append(timeout)
        for callback in self._callbacks:
            callback(self.error, None)
        self._callbacks.clear()
        return self.pending


@pytest.fixture
def kafka(monkeypatch):
    def make(*, flush_timeout=None, **options):
        module = ModuleType("confluent_kafka")
        module.Producer = lambda config: FakeProducer(config, **options)
        monkeypatch.setitem(sys.modules, "confluent_kafka", module)
        extra = {} if flush_timeout is None else {"flush_timeout": flush_timeout}
        return KafkaPublisher("broker:9092", **extra)

    return make


def test_publishes_event_json_keyed_by_dataset_with_idempotence(kafka):
    publisher = kafka()
    publisher.publish(EVENT)

    assert publisher._producer.config == {"bootstrap.servers": "broker:9092", "enable.idempotence": True}
    assert publisher._producer.sent == [("dataset.ingested", "orders", EVENT.model_dump_json())]


def test_waits_longer_than_a_cold_broker_needs_for_the_producer_id(kafka):
    # a freshly started broker answers "coordinator load in progress" for 10-20 seconds
    publisher = kafka()
    publisher.publish(EVENT)
    assert publisher._producer.flush_timeouts == [45.0]


def test_flush_timeout_is_configurable(kafka):
    publisher = kafka(flush_timeout=5)
    publisher.publish(EVENT)
    assert publisher._producer.flush_timeouts == [5]


def test_delivery_error_raises(kafka):
    with pytest.raises(RuntimeError, match="delivery failed: broker down"):
        kafka(error="broker down").publish(EVENT)


def test_flush_timeout_raises(kafka):
    with pytest.raises(RuntimeError, match="timeout"):
        kafka(pending=1).publish(EVENT)


def test_full_producer_queue_raises_runtime_error(kafka):
    with pytest.raises(RuntimeError, match="queue is full"):
        kafka(produce_raises=BufferError("queue is full")).publish(EVENT)
