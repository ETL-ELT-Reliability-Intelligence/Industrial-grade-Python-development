"""Real S3, Kafka and PostgreSQL: process death, connection loss and publication retry."""

from concurrent.futures import ThreadPoolExecutor
import os
import subprocess
import sys
import time
from uuid import uuid4

import pytest

pytestmark = pytest.mark.skipif(
    not all(os.environ.get(key) for key in ("TEST_DATABASE_URL", "KAFKA_BOOTSTRAP_SERVERS", "S3_ENDPOINT_URL")),
    reason="requires isolated PostgreSQL, Kafka and S3",
)


@pytest.fixture
def scenario():
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka import Producer
    from services.ingestion.service import IngestionService
    from services.ingestion.publisher import InMemoryPublisher
    from services.ingestion.storage import store_from_env
    from services.ingestion.sources import SourceBatch
    from tests.storage.support import migrate

    migrate(os.environ["TEST_DATABASE_URL"])
    identity = uuid4().hex
    topic, group, dataset = f"recovery-{identity}", f"group-{identity}", f"dataset-{identity}"
    admin = AdminClient({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]})
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result(timeout=20)

    class Source:
        dataset_id = dataset

        def fetch(self):
            return SourceBatch(dataset, [{"id": 1}, {"id": 2}])

    event = IngestionService(store_from_env(), InMemoryPublisher()).ingest(Source(), run_id=identity)
    producer = Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]})
    producer.produce(topic, value=event.model_dump_json())
    assert producer.flush(15) == 0

    def worker(mode):
        return subprocess.run([sys.executable, "-m", "tests.integration.recovery_worker", topic, group, dataset, mode],
                              capture_output=True, text=True, timeout=120)

    yield event, worker
    admin.delete_topics([topic])[topic].result(timeout=20)


def evidence(event):
    from storage.postgres import connect, PostgresQualityRepository
    with connect(os.environ["TEST_DATABASE_URL"]) as connection:
        evaluations = PostgresQualityRepository(connection).list_evaluations(event.dataset_id, event.batch_id)
        assert len(evaluations) == 1
        counts = connection.execute(
            "SELECT (SELECT count(*) FROM batches WHERE dataset_id=%s) AS batches, "
            "(SELECT count(*) FROM quality_decisions WHERE dataset_id=%s) AS decisions",
            (event.dataset_id, event.dataset_id),
        ).fetchone()
        assert counts == {"batches": 1, "decisions": 1}
        return evaluations[0]


def test_crash_after_result_before_commit_redelivers_without_duplicates(scenario):
    event, worker = scenario
    first = worker("crash")
    assert first.returncode == 73, first.stderr
    before = evidence(event)
    assert before.published
    second = worker("normal")
    assert second.returncode == 0, second.stderr
    assert evidence(event) == before


def test_database_disconnect_leaves_offset_uncommitted_and_recovers(scenario):
    event, worker = scenario
    failed = worker("disconnect")
    assert failed.returncode != 0
    assert "connection" in failed.stderr.lower() or "terminated" in failed.stderr.lower()
    resumed = worker("normal")
    assert resumed.returncode == 0, resumed.stderr
    assert evidence(event).published


def test_notification_outage_and_lost_ack_preserve_duplicate_identity(scenario):
    from contracts.consumer import KafkaEventConsumer
    event, worker = scenario
    result = worker("publish-failure")
    assert result.returncode == 0, result.stderr
    saved = evidence(event)
    assert saved.published and len(saved.delivery_attempts) == 3
    consumer = KafkaEventConsumer(os.environ["KAFKA_BOOTSTRAP_SERVERS"], uuid4().hex, ["data.quality.failed"])
    found = []
    try:
        deadline = time.monotonic() + 20
        while len(found) < 2 and time.monotonic() < deadline:
            item = consumer.poll(1)
            if item is not None and item.dataset_id == event.dataset_id:
                found.append(item)
    finally:
        consumer.close()
    assert len(found) == 2 and found[0].event_id == found[1].event_id
    assert found[0].dedup_key == found[1].dedup_key


def test_real_s3_concurrent_same_run_returns_one_manifest():
    from services.ingestion.service import IngestionService
    from services.ingestion.publisher import InMemoryPublisher
    from services.ingestion.storage import store_from_env
    from services.ingestion.sources import SourceBatch
    from threading import Barrier

    dataset = f"concurrent-{uuid4().hex}"
    barrier = Barrier(8)

    class Source:
        dataset_id = dataset

        def fetch(self):
            barrier.wait(timeout=20)
            return SourceBatch(dataset, [{"id": uuid4().hex}])

    store = store_from_env()
    with ThreadPoolExecutor(max_workers=8) as pool:
        events = list(pool.map(lambda _: IngestionService(store, InMemoryPublisher()).ingest(Source(), "same"), range(8)))
    assert all(event == events[0] for event in events)
    original = store.load_raw(dataset, events[0].batch_id)
    with pytest.raises(FileExistsError):
        store.save_raw(dataset, events[0].batch_id, [{"id": "replacement"}])
    assert store.load_raw(dataset, events[0].batch_id) == original
