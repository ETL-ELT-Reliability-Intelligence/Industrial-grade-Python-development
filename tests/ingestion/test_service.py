from datetime import datetime, timezone

import pytest

from contracts.events import DatasetIngestedEvent, SchemaChangedEvent, parse_event
from services.ingestion.normalize import ValidationError
from services.ingestion.publisher import InMemoryPublisher
from services.ingestion.service import IngestionService
from services.ingestion.sources import MockApiSource, SourceBatch
from services.ingestion.storage import LocalRawStore

NOW = datetime(2026, 9, 26, 10, 1, tzinfo=timezone.utc)


class FixedSource:
    dataset_id = "orders"

    def __init__(self, records):
        self._records = records

    def fetch(self):
        return SourceBatch(self.dataset_id, self._records)


@pytest.fixture
def publisher():
    return InMemoryPublisher()


@pytest.fixture
def service(tmp_path, publisher):
    return IngestionService(LocalRawStore(tmp_path), publisher, clock=lambda: NOW)


def test_ingest_publishes_dataset_ingested(service, publisher):
    event = service.ingest(MockApiSource(base_rows=100))

    assert event.dataset_id == "orders"
    assert event.batch_id == "2026-09-26-001"
    assert event.records == 100
    assert event.received_at == NOW
    assert event.schema_version == 1
    assert publisher.events == [event]


def test_batch_ids_increment_within_a_day(service):
    ids = [service.ingest(MockApiSource(base_rows=5)).batch_id for _ in range(3)]
    assert ids == ["2026-09-26-001", "2026-09-26-002", "2026-09-26-003"]


def test_raw_data_is_saved(service, tmp_path):
    service.ingest(MockApiSource(base_rows=7))
    lines = (tmp_path / "orders" / "2026-09-26-001.jsonl").read_text().splitlines()
    assert len(lines) == 7


def test_partial_upstream_is_ingested_without_judgement(service, publisher):
    service.ingest(MockApiSource(base_rows=1000, volume_factor=0.4))
    assert publisher.events[0].records == 400


def test_schema_change_emits_event_and_bumps_version(service, publisher):
    service.ingest(FixedSource([{"id": 1}]))
    event = service.ingest(FixedSource([{"id": 1, "extra": "x"}]))

    assert event.schema_version == 2
    changed = [e for e in publisher.events if isinstance(e, SchemaChangedEvent)]
    assert [(e.previous_version, e.new_version) for e in changed] == [(1, 2)]


def test_unchanged_schema_emits_no_schema_event(service, publisher):
    service.ingest(FixedSource([{"id": 1}]))
    service.ingest(FixedSource([{"id": 2}]))
    assert not any(isinstance(e, SchemaChangedEvent) for e in publisher.events)


def test_invalid_batch_is_rejected_and_nothing_published(service, publisher):
    with pytest.raises(ValidationError):
        service.ingest(FixedSource(["not-an-object"]))
    assert publisher.events == []


def test_event_json_roundtrip(service):
    event = service.ingest(MockApiSource(base_rows=3))
    parsed = parse_event(event.model_dump_json())
    assert isinstance(parsed, DatasetIngestedEvent)
    assert parsed == event
    assert event.topic == "dataset.ingested"
    assert event.key == "orders"


class FlakyPublisher(InMemoryPublisher):
    """Fails the first publish, like a Kafka outage right after raw data was saved."""

    def __init__(self):
        super().__init__()
        self.fail = True

    def publish(self, event):
        if self.fail:
            self.fail = False
            raise RuntimeError("Kafka delivery failed")
        super().publish(event)


def test_retry_of_same_run_does_not_create_second_batch(tmp_path):
    publisher = FlakyPublisher()
    service = IngestionService(LocalRawStore(tmp_path), publisher, clock=lambda: NOW)

    with pytest.raises(RuntimeError):
        service.ingest(MockApiSource(base_rows=5), run_id="scheduled__2026-09-26T10:00:00+00:00")
    event = service.ingest(MockApiSource(base_rows=5), run_id="scheduled__2026-09-26T10:00:00+00:00")

    assert event.batch_id == "2026-09-26-001"
    assert publisher.events == [event]
    assert len(list((tmp_path / "orders").glob("*.jsonl"))) == 1


def test_retry_republishes_schema_change_too(tmp_path):
    publisher = InMemoryPublisher()
    service = IngestionService(LocalRawStore(tmp_path), publisher, clock=lambda: NOW)
    service.ingest(FixedSource([{"id": 1}]), run_id="r1")
    service.ingest(FixedSource([{"id": 1, "extra": 1}]), run_id="r2")
    publisher.events.clear()

    service.ingest(FixedSource([{"id": 1, "extra": 1}]), run_id="r2")

    assert [type(e) for e in publisher.events] == [SchemaChangedEvent, DatasetIngestedEvent]


def test_republished_events_share_dedup_keys(tmp_path):
    publisher = InMemoryPublisher()
    service = IngestionService(LocalRawStore(tmp_path), publisher, clock=lambda: NOW)
    service.ingest(MockApiSource(base_rows=5), run_id="r1")
    service.ingest(MockApiSource(base_rows=5), run_id="r1")

    first, second = publisher.events
    assert first == second
    assert first.dedup_key == second.dedup_key == "dataset.ingested:orders:2026-09-26-001"


def test_different_runs_get_different_batches(tmp_path):
    service = IngestionService(LocalRawStore(tmp_path), InMemoryPublisher(), clock=lambda: NOW)
    a = service.ingest(MockApiSource(base_rows=5), run_id="r1")
    b = service.ingest(MockApiSource(base_rows=5), run_id="r2")
    assert a.batch_id != b.batch_id


def test_without_run_id_every_call_is_a_new_batch(service):
    ids = {service.ingest(MockApiSource(base_rows=5)).batch_id for _ in range(2)}
    assert len(ids) == 2
