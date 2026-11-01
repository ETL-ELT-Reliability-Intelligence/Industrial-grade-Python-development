"""Handlers translating ingestion and pipeline events into state store records."""

from datetime import datetime, timezone
from collections.abc import Mapping
from pathlib import Path
import time
from typing import Callable

from contracts.events import (
    DatasetIngestedEvent,
    Event,
    PipelineEvent,
    PipelineFailedEvent,
    PipelineFinishedEvent,
    PipelineStartedEvent,
)
from contracts.metrics import MetricKind, MetricPoint
from contracts.quality import (
    CheckStatus,
    SchemaField,
)
from contracts.state import Batch, PipelineRun, PipelineRunStatus
from storage.repositories import (
    DatasetRepository,
    MetricRepository,
    PipelineRunRepository,
    QualityRepository,
)

from data_quality.config import load_suites, CheckSuite
from data_quality.execution import prepare_check
from .evaluation import EvaluationRunner
from .publisher import EventPublisher, NoOpPublisher, build_quality_failed_event
from .raw import read_raw
from .statistics import DatasetStatistics, compute_statistics

STATS_VERSION = "v1"


class StreamEventHandler:
    """Processes DatasetIngestedEvent and PipelineEvent messages idempotently."""

    def __init__(
        self,
        datasets: DatasetRepository,
        pipelines: PipelineRunRepository,
        metrics: MetricRepository,
        quality: QualityRepository,
        publisher: EventPublisher | None = None,
        suites: Mapping[str, CheckSuite] | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._datasets = datasets
        self._pipelines = pipelines
        self._metrics = metrics
        self._quality = quality
        self._publisher = publisher or NoOpPublisher()
        self._suites = suites if suites is not None else load_suites(Path(__file__).resolve().parents[2] / "config/quality")
        self._clock = clock
        self._runner = EvaluationRunner(quality, clock=clock, sleep=sleep)

    def handle(self, event: Event) -> None:
        """Dispatch a parsed event to its specific handler; ignore unhandled types."""
        if isinstance(event, DatasetIngestedEvent):
            self.handle_dataset_ingested(event)
        elif isinstance(event, PipelineEvent):
            self.handle_pipeline_event(event)

    def handle_dataset_ingested(self, event: DatasetIngestedEvent, *, request_id: str | None = None):
        """Resume the initial evaluation, or use an explicit request ID to reevaluate."""
        with self._quality.evaluation_lock(event.dataset_id, event.batch_id):
            evaluation = self._runner.run(event, self._suites.get(event.dataset_id), self._prepare, request_id=request_id)
            return self._runner.deliver(evaluation, self._deliver)

    def _deliver(self, evaluation):
        self._quality.project_evaluation(evaluation)
        for check in evaluation.checks:
            if check.result is not None and check.result.status is CheckStatus.FAIL:
                self._publisher.publish(build_quality_failed_event(check.result, evaluation.evaluated_at, evaluation_id=evaluation.evaluation_id))

    def _prepare(self, evaluation):
        event = evaluation.event
        self._datasets.ensure_dataset(event.dataset_id)
        self._datasets.upsert_batch(Batch(dataset_id=event.dataset_id, batch_id=event.batch_id,
                                         records=event.records, received_at=event.received_at,
                                         schema_version=event.schema_version, object_key=event.raw_uri))
        if event.raw_uri is None:
            raise ValueError("missing_raw_uri: quality evaluation requires raw data")
        stats = compute_statistics(read_raw(event.raw_uri))
        expected_schema = self._previous_schema(event.dataset_id, event.schema_version)
        history_window = max((c.params.get("history_window", 10) for c in evaluation.suite.checks if c.kind.value == "row_count"), default=10)
        history = self._metrics.list_metric_history(event.dataset_id, MetricKind.ROW_COUNT, limit=history_window + 1)
        checks = tuple(prepare_check(c, stats, event, evaluation.evaluated_at, expected_schema, history) for c in evaluation.checks)
        self._datasets.save_schema(event.dataset_id, event.schema_version, stats.schema, event.received_at)
        self._datasets.upsert_batch(Batch(dataset_id=event.dataset_id, batch_id=event.batch_id,
                                         records=stats.row_count, received_at=event.received_at,
                                         schema_version=event.schema_version, object_key=event.raw_uri))
        self._save_metrics(event, stats, evaluation.evaluated_at)
        return checks

    def handle_pipeline_event(self, event: PipelineEvent) -> None:
        """Persist pipeline run lifecycle transitions in pipeline_runs."""
        if isinstance(event, PipelineStartedEvent):
            run = PipelineRun(
                pipeline_id=event.pipeline_id,
                run_id=event.run_id,
                status=PipelineRunStatus.RUNNING,
                started_at=event.started_at,
            )
        elif isinstance(event, PipelineFinishedEvent):
            run = PipelineRun(
                pipeline_id=event.pipeline_id,
                run_id=event.run_id,
                status=PipelineRunStatus.SUCCESS,
                started_at=event.started_at,
                finished_at=event.finished_at,
            )
        elif isinstance(event, PipelineFailedEvent):
            run = PipelineRun(
                pipeline_id=event.pipeline_id,
                run_id=event.run_id,
                status=PipelineRunStatus.FAILED,
                started_at=event.started_at,
                finished_at=event.failed_at,
                error=event.error,
            )
        else:
            return
        self._pipelines.upsert_pipeline_run(run)

    def _previous_schema(
        self, dataset_id: str, schema_version: int,
    ) -> tuple[SchemaField, ...] | None:
        for version in range(schema_version - 1, 0, -1):
            stored = self._datasets.get_schema(dataset_id, version)
            if stored is not None:
                return stored
        return self._datasets.get_schema(dataset_id, schema_version)

    def _save_metrics(
        self,
        event: DatasetIngestedEvent,
        stats: DatasetStatistics,
        evaluated_at: datetime,
    ) -> None:
        freshness_seconds = (
            evaluated_at.astimezone(timezone.utc) - event.received_at.astimezone(timezone.utc)
        ).total_seconds()
        points = [
            MetricPoint(
                dataset_id=event.dataset_id,
                scope_id=event.batch_id,
                metric=MetricKind.ROW_COUNT,
                observed_at=event.received_at,
                stats_version=STATS_VERSION,
                value=stats.row_count,
            ),
            MetricPoint(
                dataset_id=event.dataset_id,
                scope_id=event.batch_id,
                metric=MetricKind.FRESHNESS_SECONDS,
                observed_at=event.received_at,
                stats_version=STATS_VERSION,
                value=freshness_seconds,
            ),
        ]
        for column, rate in sorted(stats.null_rates.items()):
            points.append(
                MetricPoint(
                    dataset_id=event.dataset_id,
                    scope_id=event.batch_id,
                    metric=MetricKind.NULL_RATE,
                    observed_at=event.received_at,
                    stats_version=STATS_VERSION,
                    value=rate,
                    column=column,
                )
            )
        for column, count in sorted(stats.distinct_counts.items()):
            points.append(
                MetricPoint(
                    dataset_id=event.dataset_id,
                    scope_id=event.batch_id,
                    metric=MetricKind.DISTINCT_COUNT,
                    observed_at=event.received_at,
                    stats_version=STATS_VERSION,
                    value=count,
                    column=column,
                )
            )
        for point in points:
            self._metrics.save_metric(point)
