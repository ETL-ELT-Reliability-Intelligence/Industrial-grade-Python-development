"""Airflow-independent reporting of pipeline run lifecycle as events.

Kept free of Airflow imports so it can be unit-tested without Airflow.
"""

from datetime import datetime, timezone
from typing import Callable

from contracts.events import PipelineFailedEvent, PipelineFinishedEvent, PipelineStartedEvent
from services.ingestion.publisher import EventPublisher


class PipelineRunReporter:
    def __init__(
        self,
        publisher: EventPublisher,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ):
        self._publisher = publisher
        self._clock = clock

    def started(self, pipeline_id: str, run_id: str, *, started_at: datetime) -> None:
        self._publisher.publish(
            PipelineStartedEvent(pipeline_id=pipeline_id, run_id=run_id, started_at=started_at)
        )

    def finished(self, pipeline_id: str, run_id: str, started_at: datetime) -> None:
        now = self._clock()
        self._publisher.publish(
            PipelineFinishedEvent(
                pipeline_id=pipeline_id,
                run_id=run_id,
                started_at=started_at,
                finished_at=now,
            )
        )

    def failed(self, pipeline_id: str, run_id: str, error: str | None, *, started_at: datetime) -> None:
        self._publisher.publish(
            PipelineFailedEvent(
                pipeline_id=pipeline_id, run_id=run_id, started_at=started_at,
                failed_at=self._clock(), error=error,
            )
        )
