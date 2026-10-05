"""Storage contracts used by processing components and the application layer.

Write methods are idempotent so that redelivered events can be processed again.
Implementations decide how; see storage.postgres for PostgreSQL.
"""

from datetime import datetime
from typing import Protocol

from contracts.metrics import MetricKind, MetricPoint
from contracts.quality import CheckResult, QualityDecision, SchemaField
from contracts.state import Anomaly, Batch, PipelineRun, UserAction


class DatasetRepository(Protocol):
    """Datasets, their schema versions and ingested batches."""

    def ensure_dataset(self, dataset_id: str) -> None:
        """Register a dataset if it is not known yet."""
        ...

    def save_schema(
        self, dataset_id: str, schema_version: int,
        fields: tuple[SchemaField, ...], observed_at: datetime,
    ) -> None:
        """Store a schema version; a version never changes once stored.

        Storing the same version with different fields raises ValueError.
        """
        ...

    def get_schema(self, dataset_id: str, schema_version: int) -> tuple[SchemaField, ...] | None: ...

    def upsert_batch(self, batch: Batch) -> None:
        """Insert or update a batch; a missing object_key keeps the stored one."""
        ...

    def get_batch(self, dataset_id: str, batch_id: str) -> Batch | None: ...

    def list_batches(self, dataset_id: str, limit: int = 100) -> list[Batch]:
        """Return batches, newest received first."""
        ...


class PipelineRunRepository(Protocol):
    """History of pipeline runs."""

    def upsert_pipeline_run(self, run: PipelineRun) -> None:
        """Store a run; a finished run is never overwritten by a late event."""
        ...

    def get_pipeline_run(self, pipeline_id: str, run_id: str) -> PipelineRun | None: ...

    def list_pipeline_runs(self, pipeline_id: str, limit: int = 100) -> list[PipelineRun]:
        """Return runs, newest started first."""
        ...


class MetricRepository(Protocol):
    """Dataset statistics, the input of baselines and anomaly detection."""

    def save_metric(self, point: MetricPoint) -> None:
        """Insert or update the point with the same dataset, scope, metric, column and version."""
        ...

    def list_metric_history(
        self, dataset_id: str, metric: MetricKind,
        column: str | None = None, limit: int = 100,
    ) -> list[MetricPoint]:
        """Return points of one metric and column, newest observed first."""
        ...


class QualityRepository(Protocol):
    """Results of deterministic checks and policy decisions."""

    def save_check_result(self, result: CheckResult) -> None:
        """Store a result; the latest result per check, scope and column wins.

        The result must carry a QualityContext, otherwise ValueError is raised.
        """
        ...

    def list_check_results(self, dataset_id: str, scope_id: str) -> list[CheckResult]:
        """Return results of one scope ordered by check_id and column."""
        ...

    def save_quality_decision(
        self, dataset_id: str, scope_id: str, decision: QualityDecision, decided_at: datetime,
    ) -> None:
        """Store the decision for a scope, replacing an earlier one."""
        ...

    def get_quality_decision(self, dataset_id: str, scope_id: str) -> QualityDecision | None: ...


class AnomalyRepository(Protocol):
    """Detected anomalies."""

    def save_anomaly(self, anomaly: Anomaly) -> None:
        """Store an anomaly; an already stored anomaly_id is left unchanged."""
        ...

    def get_anomaly(self, anomaly_id: str) -> Anomaly | None: ...

    def list_anomalies(self, dataset_id: str | None = None, limit: int = 100) -> list[Anomaly]:
        """Return anomalies, newest detected first, optionally of one dataset."""
        ...


class UserActionRepository(Protocol):
    """Audit trail of user actions."""

    def record_action(self, action: UserAction) -> None:
        """Store an action; an already stored action_id is left unchanged."""
        ...

    def list_actions(self, target_type: str, target_id: str, limit: int = 100) -> list[UserAction]:
        """Return actions on one entity, newest first."""
        ...
