from datetime import datetime, timezone
from uuid import UUID

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
    assert isinstance(event.event_id, UUID)
    assert event.contract_version == 1
    assert event.raw_uri is not None
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
    assert changed[0].batch_id == event.batch_id
    assert changed[0].detected_at == NOW
    assert changed[0].event_id != event.event_id
    assert parse_event(changed[0].model_dump_json()) == changed[0]


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
