"""Explicit test double; never imported by the running application."""

from contextlib import contextmanager
from datetime import datetime, timezone

from contracts.quality import (
    NullRateConstraint,
    NullRateObservation,
    QualityContext,
    RowCountConstraint,
    RowCountObservation,
)
from contracts.state import Batch, PipelineRun
from data_quality.checks.null_rate import check_null_rate
from data_quality.checks.row_count import check_row_count
from data_quality.policies import evaluate_quality
from services.backend.models import (
    Dataset,
    EvaluationDetail,
    EvaluationSummary,
    Pipeline,
    QualityScope,
    ScopeQuality,
    StoredCheck,
)

NOW = datetime(2026, 10, 12, 10, tzinfo=timezone.utc)


def evidence(dataset_id="orders", scope_id="window-1"):
    context = QualityContext(dataset_id=dataset_id, scope_id=scope_id)
    return [
        check_row_count(
            RowCountObservation(row_count=0),
            RowCountConstraint(min_count=100),
            check_id="volume",
            context=context,
        ),
        check_null_rate(
            NullRateObservation(null_rate=None),
            NullRateConstraint(max_null_rate=0.1),
            check_id="nulls",
            context=QualityContext(
                dataset_id=dataset_id, scope_id=scope_id, column="email"
            ),
        ),
    ]


class MemoryRepository:
    def __init__(self):
        self.datasets = [
            Dataset(dataset_id=name, created_at=NOW) for name in ("empty", "orders")
        ]
        self.batches = [
            Batch(
                dataset_id="orders",
                batch_id="batch-1",
                records=0,
                received_at=NOW,
                schema_version=1,
            )
        ]
        results = evidence()
        self.qualities = {
            ("orders", "window-1"): ScopeQuality(
                dataset_id="orders",
                scope_id="window-1",
                checks=[StoredCheck(result=item, recorded_at=NOW) for item in results],
                decision=evaluate_quality(results),
                decided_at=NOW,
            )
        }
        # Newest first, matching the SQL sequence order rather than timestamps.
        self.evaluations: list[EvaluationDetail] = []
        self.pipeline_runs: list[PipelineRun] = []

    @contextmanager
    def factory(self):
        yield self

    def ready(self):
        pass

    def list_pipelines(self, limit, offset):
        ids = sorted({run.pipeline_id for run in self.pipeline_runs})
        return [Pipeline(pipeline_id=value) for value in ids[offset : offset + limit]]

    def list_pipeline_runs(self, pipeline_id, limit, offset):
        runs = sorted(
            (run for run in self.pipeline_runs if run.pipeline_id == pipeline_id),
            key=lambda run: (run.started_at, run.run_id),
            reverse=True,
        )
        return runs[offset : offset + limit]

    def list_datasets(self, limit, offset):
        return self.datasets[offset : offset + limit]

    def get_dataset(self, dataset_id):
        return next(
            (item for item in self.datasets if item.dataset_id == dataset_id), None
        )

    def list_batches(self, dataset_id, limit, offset):
        return [item for item in self.batches if item.dataset_id == dataset_id][
            offset : offset + limit
        ]

    def list_quality_scopes(self, dataset_id, limit, offset):
        times = {
            scope: NOW for dataset, scope in self.qualities if dataset == dataset_id
        }
        for record in self.evaluations:
            if record.dataset_id == dataset_id:
                times[record.scope_id] = max(
                    times.get(record.scope_id, record.updated_at),
                    record.created_at,
                    record.updated_at,
                )
        items = sorted(
            [
                QualityScope(scope_id=scope, last_saved_at=saved)
                for scope, saved in times.items()
            ],
            key=lambda item: (-item.last_saved_at.timestamp(), item.scope_id),
        )
        return items[offset : offset + limit]

    def list_evaluations(self, dataset_id, scope_id, limit, offset):
        records = [
            item
            for item in self.evaluations
            if item.dataset_id == dataset_id and item.scope_id == scope_id
        ]
        return [
            EvaluationSummary(
                dataset_id=item.dataset_id,
                scope_id=item.scope_id,
                created_at=item.created_at,
                updated_at=item.updated_at,
                evaluation_id=item.evaluation.evaluation_id,
                state=item.evaluation.state,
                evaluated_at=item.evaluation.evaluated_at,
                config_hash=item.evaluation.config_hash,
                decision_status=item.evaluation.decision.status
                if item.evaluation.decision
                else None,
            )
            for item in records[offset : offset + limit]
        ]

    def get_evaluation(self, dataset_id, evaluation_id):
        return next(
            (
                item
                for item in self.evaluations
                if item.dataset_id == dataset_id
                and item.evaluation.evaluation_id == evaluation_id
            ),
            None,
        )

    def get_quality(self, dataset_id, scope_id):
        return self.qualities.get(
            (dataset_id, scope_id),
            ScopeQuality(
                dataset_id=dataset_id,
                scope_id=scope_id,
                checks=[],
                decision=None,
                decided_at=None,
            ),
        )
