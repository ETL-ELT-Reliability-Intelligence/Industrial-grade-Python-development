from datetime import datetime, timedelta, timezone

from contracts.events import PipelineFailedEvent, PipelineFinishedEvent, PipelineStartedEvent
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


def test_finished_has_duration():
    publisher, reporter = make()
    reporter.finished("p", "r1", START)
    event = publisher.events[0]
    assert isinstance(event, PipelineFinishedEvent)
    assert event.duration_seconds == 90


def test_failed_carries_error():
    publisher, reporter = make()
    reporter.failed("p", "r1", "boom")
    event = publisher.events[0]
    assert isinstance(event, PipelineFailedEvent)
    assert event.error == "boom"
    assert event.topic == "pipeline.failed"
