from datetime import datetime, timedelta, timezone
import json


from contracts.events import (
    DataQualityFailedEvent,
    DatasetIngestedEvent,
    PipelineFailedEvent,
    PipelineFinishedEvent,
    PipelineStartedEvent,
    SchemaChangedEvent,
)
from contracts.metrics import MetricKind, MetricPoint
from contracts.quality import (
    CheckKind,
    CheckResult,
    CheckStatus,
    QualityDecision,
    QualityStatus,
    SchemaField,
)
from contracts.state import Batch, PipelineRun, PipelineRunStatus
from services.stream_processor.handlers import StreamEventHandler
from contextlib import nullcontext
from contracts.evaluation import Evaluation
from contracts.quality_config import CheckSuite
from services.stream_processor.publisher import (
    InMemoryPublisher,
    quality_failed_event_id,
)

NOW = datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc)
MINUTE = timedelta(minutes=1)


class FakeDatasets:
    def __init__(self) -> None:
        self.datasets: set[str] = set()
        self.schemas: dict[tuple[str, int], tuple[SchemaField, ...]] = {}
        self.batches: dict[tuple[str, str], Batch] = {}

    def ensure_dataset(self, dataset_id: str) -> None:
        self.datasets.add(dataset_id)

    def save_schema(
        self,
        dataset_id: str,
        schema_version: int,
        fields: tuple[SchemaField, ...],
        observed_at: datetime,
    ) -> None:
        self.ensure_dataset(dataset_id)
        key = (dataset_id, schema_version)
        if key in self.schemas and set(self.schemas[key]) != set(fields):
            raise ValueError("schema version already stored with different fields")
        self.schemas[key] = fields

    def get_schema(
        self, dataset_id: str, schema_version: int
    ) -> tuple[SchemaField, ...] | None:
        return self.schemas.get((dataset_id, schema_version))

    def upsert_batch(self, batch: Batch) -> None:
        self.ensure_dataset(batch.dataset_id)
        existing = self.batches.get((batch.dataset_id, batch.batch_id))
        object_key = (
            batch.object_key
            if batch.object_key is not None
            else (existing.object_key if existing else None)
        )
        self.batches[(batch.dataset_id, batch.batch_id)] = Batch(
            dataset_id=batch.dataset_id,
            batch_id=batch.batch_id,
            records=batch.records,
            received_at=batch.received_at,
            schema_version=batch.schema_version,
            object_key=object_key,
        )

    def get_batch(self, dataset_id: str, batch_id: str) -> Batch | None:
        return self.batches.get((dataset_id, batch_id))

    def list_batches(self, dataset_id: str, limit: int = 100) -> list[Batch]:
        items = [b for b in self.batches.values() if b.dataset_id == dataset_id]
        items.sort(key=lambda b: (b.received_at, b.batch_id), reverse=True)
        return items[:limit]


class FakePipelines:
    def __init__(self) -> None:
        self.runs: dict[tuple[str, str], PipelineRun] = {}

    def upsert_pipeline_run(self, run: PipelineRun) -> None:
        key = (run.pipeline_id, run.run_id)
        existing = self.runs.get(key)
        if existing is not None and existing.status is not PipelineRunStatus.RUNNING:
            return
        self.runs[key] = run

    def get_pipeline_run(self, pipeline_id: str, run_id: str) -> PipelineRun | None:
        return self.runs.get((pipeline_id, run_id))

    def list_pipeline_runs(
        self, pipeline_id: str, limit: int = 100
    ) -> list[PipelineRun]:
        items = [r for r in self.runs.values() if r.pipeline_id == pipeline_id]
        items.sort(key=lambda r: (r.started_at, r.run_id), reverse=True)
        return items[:limit]


class FakeMetrics:
    def __init__(self) -> None:
        self.profiles = {}
        self.feature_inputs = {}
        self.feature_profiles = {}
        self.profiling_attempts = {}
        self.feature_links = {}
        self.points: dict[
            tuple[str, str, MetricKind, str | None, str], MetricPoint
        ] = {}

    def register_feature_input(self, dataset_id, batch_id, sha256):
        from storage.feature_profiles import InputConflict

        key = (dataset_id, batch_id)
        if self.feature_inputs.setdefault(key, sha256) != sha256:
            raise InputConflict("batch identity already refers to different raw bytes")

    def get_feature_profile(self, dataset_id, batch_id, sha256, definition_hash, implementation):
        return self.feature_profiles.get((dataset_id, batch_id, sha256, definition_hash, implementation))

    def save_feature_profile(self, profile):
        self.register_feature_input(profile.dataset_id, profile.batch_id, profile.raw_sha256)
        key = (profile.dataset_id, profile.batch_id, profile.raw_sha256,
               profile.definition_hash, profile.implementation_version)
        return self.feature_profiles.setdefault(key, profile)

    def save_profiling_attempt(self, attempt):
        self.profiling_attempts[attempt.attempt_id] = attempt

    def link_feature_evaluation(self, evaluation_id, profile_id, eligible):
        self.feature_links.setdefault((evaluation_id, profile_id), eligible)

    def save_profile(self, profile):
        self.profiles[(profile.event.dataset_id, profile.event.batch_id)] = profile

    def list_profiles(self, dataset_id):
        return [p for (d, _), p in self.profiles.items() if d == dataset_id]

    def set_profile_eligible(self, dataset_id, scope_id, eligible):
        key = (dataset_id, scope_id)
        if key in self.profiles:
            self.profiles[key] = self.profiles[key].model_copy(
                update={"eligible": eligible}
            )

    def save_metric(self, point: MetricPoint) -> None:
        key = (
            point.dataset_id,
            point.scope_id,
            point.metric,
            point.column,
            point.stats_version,
        )
        self.points[key] = point

    def list_metric_history(
        self,
        dataset_id: str,
        metric: MetricKind,
        column: str | None = None,
        limit: int = 100,
    ) -> list[MetricPoint]:
        items = [
            p
            for p in self.points.values()
            if p.dataset_id == dataset_id and p.metric is metric and p.column == column
        ]
        items.sort(key=lambda p: (p.observed_at, p.scope_id), reverse=True)
        return items[:limit]


class FakeQuality:
    def __init__(self) -> None:
        self.results: dict[tuple[str, str, str, str | None], CheckResult] = {}
        self.decisions: dict[tuple[str, str], QualityDecision] = {}
        self.evaluations = {}

    def evaluation_lock(self, dataset_id, scope_id):
        return nullcontext()

    def get_evaluation(self, evaluation_id):
        return self.evaluations.get(evaluation_id)

    def save_evaluation(self, evaluation):
        if evaluation.evaluation_id not in self.evaluations:
            self.decisions.pop(
                (evaluation.event.dataset_id, evaluation.event.batch_id), None
            )
        self.evaluations[evaluation.evaluation_id] = Evaluation.model_validate_json(
            evaluation.model_dump_json()
        )

    def save_execution(self, evaluation_id, execution):
        evaluation = self.evaluations[evaluation_id]
        checks = tuple(
            execution if c.check_id == execution.check_id else c
            for c in evaluation.checks
        )
        self.save_evaluation(evaluation.model_copy(update={"checks": checks}))

    def list_evaluations(self, dataset_id, scope_id):
        return [
            e
            for e in reversed(list(self.evaluations.values()))
            if e.event.dataset_id == dataset_id and e.event.batch_id == scope_id
        ]

    def project_evaluation(self, evaluation):
        event = evaluation.event
        if (
            self.list_evaluations(event.dataset_id, event.batch_id)[0].evaluation_id
            != evaluation.evaluation_id
        ):
            return
        self.results = {
            k: v
            for k, v in self.results.items()
            if (k[1], k[2]) != (event.dataset_id, event.batch_id)
        }
        for check in evaluation.checks:
            if check.result is not None:
                self.save_check_result(check.result)
        self.save_quality_decision(
            event.dataset_id,
            event.batch_id,
            evaluation.decision,
            evaluation.evaluated_at,
        )

    def save_check_result(self, result: CheckResult) -> None:
        if result.context is None:
            raise ValueError("context is required")
        key = (
            result.check_id,
            result.context.dataset_id,
            result.context.scope_id,
            result.context.column,
        )
        self.results[key] = result

    def list_check_results(self, dataset_id: str, scope_id: str) -> list[CheckResult]:
        items = [
            r
            for r in self.results.values()
            if r.context
            and r.context.dataset_id == dataset_id
            and r.context.scope_id == scope_id
        ]
        items.sort(
            key=lambda r: (r.check_id, r.context.column or "" if r.context else "")
        )
        return items

    def save_quality_decision(
        self,
        dataset_id: str,
        scope_id: str,
        decision: QualityDecision,
        decided_at: datetime,
    ) -> None:
        self.decisions[(dataset_id, scope_id)] = decision

    def get_quality_decision(
        self, dataset_id: str, scope_id: str
    ) -> QualityDecision | None:
        return self.decisions.get((dataset_id, scope_id))


def write_batch(tmp_path, name: str, records: list[dict]) -> str:
    path = tmp_path / f"{name}.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")
    return path.as_uri()


def test_suite(dataset_id="orders"):
    return CheckSuite.model_validate_json(
        json.dumps(
            {
                "dataset_id": dataset_id,
                "retry": {"max_attempts": 3, "delays_seconds": [0, 0]},
                "checks": [
                    {"key": "schema", "kind": "schema"},
                    {
                        "key": "id.null_rate",
                        "kind": "null_rate",
                        "column": "id",
                        "params": {"mode": "fixed"},
                    },
                    {
                        "key": "amount.null_rate",
                        "kind": "null_rate",
                        "column": "amount",
                        "params": {"mode": "fixed"},
                    },
                    {"key": "row_count", "kind": "row_count"},
                ],
            }
        )
    )


test_suite.__test__ = False


def make_handler(clock=lambda: NOW + MINUTE, suites=None):
    datasets = FakeDatasets()
    pipelines = FakePipelines()
    metrics = FakeMetrics()
    quality = FakeQuality()
    publisher = InMemoryPublisher()
    handler = StreamEventHandler(
        datasets,
        pipelines,
        metrics,
        quality,
        publisher=publisher,
        suites=suites if suites is not None else {"orders": test_suite()},
        clock=clock,
        sleep=lambda _: None,
    )
    return handler, datasets, pipelines, metrics, quality, publisher


def test_first_batch_saves_metrics_and_warns_about_missing_baselines(tmp_path):
    handler, datasets, _, metrics, quality, publisher = make_handler()
    uri = write_batch(
        tmp_path, "b1", [{"id": 1, "amount": 10.0}, {"id": 2, "amount": 20.0}]
    )
    event = DatasetIngestedEvent(
        dataset_id="orders",
        batch_id="b1",
        records=2,
        received_at=NOW,
        schema_version=1,
        raw_uri=uri,
    )

    handler.handle(event)

    assert datasets.get_batch("orders", "b1") == Batch(
        dataset_id="orders",
        batch_id="b1",
        records=2,
        received_at=NOW,
        schema_version=1,
        object_key=uri,
    )
    assert datasets.get_schema("orders", 1) == (
        SchemaField(name="amount", data_type="number"),
        SchemaField(name="id", data_type="integer"),
    )
    assert metrics.list_metric_history("orders", MetricKind.ROW_COUNT)[0].value == 2
    assert (
        metrics.list_metric_history("orders", MetricKind.NULL_RATE, "id")[0].value
        == 0.0
    )
    assert (
        metrics.list_metric_history("orders", MetricKind.DISTINCT_COUNT, "id")[0].value
        == 2
    )

    results = quality.list_check_results("orders", "b1")
    assert {r.kind for r in results} == {CheckKind.NULL_RATE}
    assert quality.get_quality_decision("orders", "b1").status is QualityStatus.WARN
    assert publisher.events == []


def test_redelivered_event_is_idempotent_without_duplicates(tmp_path):
    handler, datasets, _, metrics, quality, _ = make_handler()
    uri = write_batch(tmp_path, "b1", [{"id": 1}, {"id": 2}, {"id": 3}])
    event = DatasetIngestedEvent(
        dataset_id="orders",
        batch_id="b1",
        records=3,
        received_at=NOW,
        schema_version=1,
        raw_uri=uri,
    )

    handler.handle(event)
    handler.handle(event)

    assert len(datasets.list_batches("orders")) == 1
    assert len(metrics.list_metric_history("orders", MetricKind.ROW_COUNT)) == 1
    assert len(quality.list_check_results("orders", "b1")) == 2


def test_row_count_drop_after_history_fails_and_blocks(tmp_path):
    handler, _, _, _, quality, publisher = make_handler()
    rows_10 = [{"id": i, "amount": float(i)} for i in range(10)]
    for idx in range(1, 4):
        uri = write_batch(tmp_path, f"b{idx}", rows_10)
        handler.handle(
            DatasetIngestedEvent(
                dataset_id="orders",
                batch_id=f"b{idx}",
                records=10,
                received_at=NOW + idx * MINUTE,
                schema_version=1,
                raw_uri=uri,
            )
        )

    uri_drop = write_batch(tmp_path, "b4", rows_10[:4])
    handler.handle(
        DatasetIngestedEvent(
            dataset_id="orders",
            batch_id="b4",
            records=4,
            received_at=NOW + 4 * MINUTE,
            schema_version=1,
            raw_uri=uri_drop,
        )
    )

    results = {r.check_id: r for r in quality.list_check_results("orders", "b4")}
    assert results["orders.row_count"].status is CheckStatus.FAIL
    assert results["orders.row_count"].reason_code == "below_minimum"
    assert quality.get_quality_decision("orders", "b4").status is QualityStatus.BLOCK
    assert len(publisher.events) == 1
    failed = publisher.events[0]
    assert isinstance(failed, DataQualityFailedEvent)
    assert failed.check_id == "orders.row_count"
    assert failed.event_id == quality_failed_event_id(
        "orders",
        "b4",
        "orders.row_count",
        quality.list_evaluations("orders", "b4")[0].evaluation_id,
    )


def test_schema_change_fails_check_and_saves_new_version(tmp_path):
    handler, datasets, _, _, quality, publisher = make_handler()
    uri1 = write_batch(tmp_path, "b1", [{"id": 1, "amount": 10.0}])
    handler.handle(
        DatasetIngestedEvent(
            dataset_id="orders",
            batch_id="b1",
            records=1,
            received_at=NOW,
            schema_version=1,
            raw_uri=uri1,
        )
    )

    uri2 = write_batch(tmp_path, "b2", [{"id": 1, "amount": 10.0, "extra": "x"}])
    handler.handle(
        DatasetIngestedEvent(
            dataset_id="orders",
            batch_id="b2",
            records=1,
            received_at=NOW + MINUTE,
            schema_version=2,
            raw_uri=uri2,
        )
    )

    results = {r.check_id: r for r in quality.list_check_results("orders", "b2")}
    assert results["orders.schema"].status is CheckStatus.FAIL
    assert results["orders.schema"].reason_code == "schema_mismatch"
    assert quality.get_quality_decision("orders", "b2").status is QualityStatus.BLOCK
    assert datasets.get_schema("orders", 2) is not None
    assert [e.check_id for e in publisher.events] == ["orders.schema"]


def test_event_without_raw_uri_saves_batch_only():
    handler, datasets, _, metrics, quality, publisher = make_handler()
    event = DatasetIngestedEvent(
        dataset_id="orders",
        batch_id="b1",
        records=10,
        received_at=NOW,
        schema_version=1,
        raw_uri=None,
    )

    handler.handle(event)

    assert datasets.get_batch("orders", "b1") is not None
    assert metrics.list_metric_history("orders", MetricKind.ROW_COUNT) == []
    assert quality.list_check_results("orders", "b1") == []
    assert quality.get_quality_decision("orders", "b1").status is QualityStatus.BLOCK
    assert publisher.events == []


def test_pipeline_events_update_pipeline_runs_and_keep_terminal_state():
    handler, _, pipelines, _, _, _ = make_handler()
    started = PipelineStartedEvent(
        pipeline_id="orders_daily", run_id="r1", started_at=NOW
    )
    finished = PipelineFinishedEvent(
        pipeline_id="orders_daily",
        run_id="r1",
        started_at=NOW,
        finished_at=NOW + MINUTE,
    )
    failed = PipelineFailedEvent(
        pipeline_id="orders_daily",
        run_id="r2",
        started_at=NOW,
        failed_at=NOW + MINUTE,
        error="boom",
    )

    handler.handle(started)
    assert (
        pipelines.get_pipeline_run("orders_daily", "r1").status
        is PipelineRunStatus.RUNNING
    )

    handler.handle(finished)
    assert (
        pipelines.get_pipeline_run("orders_daily", "r1").status
        is PipelineRunStatus.SUCCESS
    )
    assert pipelines.get_pipeline_run("orders_daily", "r1").finished_at == NOW + MINUTE

    # Late started event must not overwrite terminal status
    handler.handle(started)
    assert (
        pipelines.get_pipeline_run("orders_daily", "r1").status
        is PipelineRunStatus.SUCCESS
    )

    handler.handle(failed)
    run2 = pipelines.get_pipeline_run("orders_daily", "r2")
    assert run2.status is PipelineRunStatus.FAILED
    assert run2.error == "boom"


def test_unhandled_event_is_ignored():
    handler, datasets, pipelines, _, _, _ = make_handler()
    handler.handle(
        SchemaChangedEvent(
            dataset_id="orders",
            batch_id="b1",
            previous_version=1,
            new_version=2,
            detected_at=NOW,
        )
    )
    assert datasets.datasets == set()
    assert pipelines.runs == {}
