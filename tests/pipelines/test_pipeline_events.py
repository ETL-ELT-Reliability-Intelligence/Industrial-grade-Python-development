from datetime import datetime, timedelta, timezone

import pytest

from contracts.events import PipelineFailedEvent, PipelineFinishedEvent, PipelineStartedEvent, parse_event
from pipelines.airflow.pipeline_events import PipelineRunReporter
from services.ingestion.publisher import InMemoryPublisher

START = datetime(2026, 9, 26, 10, 0, tzinfo=timezone.utc)


def make():
    publisher = InMemoryPublisher()
    return publisher, PipelineRunReporter(publisher, clock=lambda: START + timedelta(seconds=90))


def test_started():
    publisher, reporter = make()
    reporter.started("p", "r1")
    assert isinstance(publisher.events[0], PipelineStartedEvent)
    assert publisher.events[0].key == "p"
    assert publisher.events[0].started_at == START + timedelta(seconds=90)
    assert parse_event(publisher.events[0].model_dump_json()) == publisher.events[0]


def test_finished_has_duration():
    publisher, reporter = make()
    reporter.finished("p", "r1", START)
    event = publisher.events[0]
    assert isinstance(event, PipelineFinishedEvent)
    assert event.duration_seconds == 90
    assert event.started_at == START
    assert event.finished_at == START + timedelta(seconds=90)
    assert parse_event(event.model_dump_json()) == event


def test_failed_carries_error():
    publisher, reporter = make()
    reporter.failed("p", "r1", "boom", started_at=START)
    event = publisher.events[0]
    assert isinstance(event, PipelineFailedEvent)
    assert event.error == "boom"
    assert event.topic == "pipeline.failed"
    assert event.started_at == START
    assert event.failed_at == START + timedelta(seconds=90)
    assert parse_event(event.model_dump_json()) == event


@pytest.mark.parametrize('operation', ['finished', 'failed'])
def test_invalid_chronology_does_not_publish(operation):
    publisher, reporter = make()
    future = START + timedelta(seconds=91)
    with pytest.raises(ValueError):
        if operation == 'finished':
            reporter.finished('p', 'r', future)
        else:
            reporter.failed('p', 'r', 'boom', started_at=future)
    assert publisher.events == []
