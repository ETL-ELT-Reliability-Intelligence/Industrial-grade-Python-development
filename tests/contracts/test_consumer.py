from datetime import datetime, timezone

from contracts.consumer import Deduplicator
from contracts.events import (
    DatasetIngestedEvent,
    PipelineFailedEvent,
    PipelineFinishedEvent,
    PipelineStartedEvent,
    SchemaChangedEvent,
)

NOW = datetime(2026, 9, 26, 10, 1, tzinfo=timezone.utc)


def ingested(batch_id="b1"):
    return DatasetIngestedEvent(dataset_id="orders", batch_id=batch_id, records=1, received_at=NOW, schema_version=1)


def test_deduplicator_skips_redelivery_but_not_new_facts():
    dedup = Deduplicator()
    assert dedup.is_new(ingested("b1"))
    assert not dedup.is_new(ingested("b1"))
    assert dedup.is_new(ingested("b2"))


def test_every_event_type_has_a_dedup_key():
    events = [
        ingested(),
        SchemaChangedEvent(dataset_id="orders", previous_version=1, new_version=2, detected_at=NOW),
        PipelineStartedEvent(pipeline_id="p", run_id="r", occurred_at=NOW),
        PipelineFinishedEvent(pipeline_id="p", run_id="r", occurred_at=NOW, duration_seconds=1),
        PipelineFailedEvent(pipeline_id="p", run_id="r", occurred_at=NOW, error="boom"),
    ]
    assert len({e.dedup_key for e in events}) == len(events)
