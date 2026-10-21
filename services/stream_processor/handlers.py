"""Handlers translating ingestion and pipeline events into state store records."""

from datetime import datetime, timezone
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
    CheckResult,
    CheckStatus,
    FreshnessObservation,
    NullRateObservation,
    QualityContext,
    RowCountObservation,
    SchemaField,
    SchemaObservation,
)
from contracts.state import Batch, PipelineRun, PipelineRunStatus
from data_quality.checks import check_freshness, check_null_rate, check_row_count, check_schema
from data_quality.policies import evaluate_quality
from storage.repositories import (
    DatasetRepository,
    MetricRepository,
    PipelineRunRepository,
    QualityRepository,
)

from .policy import QualityThresholds
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
        thresholds: QualityThresholds | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self._datasets = datasets
        self._pipelines = pipelines
        self._metrics = metrics
        self._quality = quality
        self._publisher = publisher or NoOpPublisher()
        self._thresholds = thresholds or QualityThresholds()
        self._clock = clock

    def handle(self, event: Event) -> None:
        """Dispatch a parsed event to its specific handler; ignore unhandled types."""
        if isinstance(event, DatasetIngestedEvent):
            self.handle_dataset_ingested(event)
        elif isinstance(event, PipelineEvent):
            self.handle_pipeline_event(event)

    def handle_dataset_ingested(self, event: DatasetIngestedEvent) -> None:
        """Persist batch, statistics, quality checks and policy decision."""
        self._datasets.ensure_dataset(event.dataset_id)
        if event.raw_uri is None:
            self._datasets.upsert_batch(
                Batch(
                    dataset_id=event.dataset_id,
                    batch_id=event.batch_id,
                    records=event.records,
                    received_at=event.received_at,
                    schema_version=event.schema_version,
                    object_key=None,
                )
            )
            return

        records = read_raw(event.raw_uri)
        stats = compute_statistics(records)
        evaluated_at = max(self._clock(), event.received_at)

        expected_schema = self._previous_schema(event.dataset_id, event.schema_version)
        self._datasets.save_schema(
            event.dataset_id, event.schema_version, stats.schema, event.received_at,
        )
        self._datasets.upsert_batch(
            Batch(
                dataset_id=event.dataset_id,
                batch_id=event.batch_id,
                records=stats.row_count,
                received_at=event.received_at,
                schema_version=event.schema_version,
                object_key=event.raw_uri,
            )
        )

        history = self._metrics.list_metric_history(
            event.dataset_id,
            MetricKind.ROW_COUNT,
            limit=self._thresholds.row_count_history_window + 1,
        )
        self._save_metrics(event, stats, evaluated_at)

        results = self._run_checks(event, stats, expected_schema, history, evaluated_at)
        for result in results:
            self._quality.save_check_result(result)
        decision = evaluate_quality(results)
        self._quality.save_quality_decision(
            event.dataset_id, event.batch_id, decision, evaluated_at,
        )

        for result in results:
            if result.status is CheckStatus.FAIL:
                self._publisher.publish(build_quality_failed_event(result, evaluated_at))

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
        return None

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

    def _run_checks(
        self,
        event: DatasetIngestedEvent,
        stats: DatasetStatistics,
        expected_schema: tuple[SchemaField, ...] | None,
        history: list[MetricPoint],
        evaluated_at: datetime,
    ) -> list[CheckResult]:
        dataset_id = event.dataset_id
        context = QualityContext(dataset_id=dataset_id, scope_id=event.batch_id)
        results: list[CheckResult] = []

        if expected_schema is not None:
            results.append(
                check_schema(
                    SchemaObservation(fields=stats.schema),
                    self._thresholds.schema_constraint(expected_schema),
                    check_id=f"{dataset_id}.schema",
                    context=context,
                )
            )

        results.append(
            check_freshness(
                FreshnessObservation(
                    last_updated_at=event.received_at, evaluated_at=evaluated_at,
                ),
                self._thresholds.freshness_constraint(),
                check_id=f"{dataset_id}.freshness",
                context=context,
            )
        )

        null_constraint = self._thresholds.null_rate_constraint()
        for column, rate in sorted(stats.null_rates.items()):
            results.append(
                check_null_rate(
                    NullRateObservation(null_rate=rate),
                    null_constraint,
                    check_id=f"{dataset_id}.{column}.null_rate",
                    context=QualityContext(
                        dataset_id=dataset_id, scope_id=event.batch_id, column=column,
                    ),
                )
            )

        row_constraint = self._thresholds.row_count_constraint(
            history, exclude_scope=event.batch_id,
        )
        if row_constraint is not None:
            results.append(
                check_row_count(
                    RowCountObservation(row_count=stats.row_count),
                    row_constraint,
                    check_id=f"{dataset_id}.row_count",
                    context=context,
                )
            )

        return results
