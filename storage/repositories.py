"""Storage contracts used by processing components and the application layer.

Write methods are idempotent so that redelivered events can be processed again.
Implementations decide how; see storage.postgres for PostgreSQL.
"""

from datetime import datetime
from typing import Protocol
from contextlib import AbstractContextManager

from contracts.evaluation import Evaluation, CheckExecution

from contracts.profiles import BatchProfile
from contracts.metrics import MetricKind, MetricPoint
from contracts.quality import CheckResult, QualityDecision, SchemaField
from contracts.state import Anomaly, Batch, PipelineRun, UserAction
from storage.feature_profiles import FeatureRepository


class DatasetRepository(Protocol):
    """Datasets, their schema versions and ingested batches."""

    def ensure_dataset(self, dataset_id: str) -> None:
        """Register a dataset if it is not known yet."""
        ...

    def save_schema(
        self,
        dataset_id: str,
        schema_version: int,
        fields: tuple[SchemaField, ...],
        observed_at: datetime,
    ) -> None:
        """Store a schema version; a version never changes once stored.

        Storing the same version with different fields raises ValueError.
        """
        ...

    def get_schema(
        self, dataset_id: str, schema_version: int
    ) -> tuple[SchemaField, ...] | None:
        """Return the stored schema version, or None if absent."""
        ...

    def upsert_batch(self, batch: Batch) -> None:
        """Insert or update a batch; a missing object_key keeps the stored one."""
        ...

    def get_batch(self, dataset_id: str, batch_id: str) -> Batch | None:
        """Return the batch identified within a dataset, or None if absent."""
        ...

    def list_batches(self, dataset_id: str, limit: int = 100) -> list[Batch]:
        """Return batches, newest received first."""
        ...


class PipelineRunRepository(Protocol):
    """History of pipeline runs."""

    def upsert_pipeline_run(self, run: PipelineRun) -> None:
        """Store a run; a finished run is never overwritten by a late event."""
        ...

    def get_pipeline_run(self, pipeline_id: str, run_id: str) -> PipelineRun | None:
        """Return the identified pipeline run, or None if absent."""
        ...

    def list_pipeline_runs(
        self, pipeline_id: str, limit: int = 100
    ) -> list[PipelineRun]:
        """Return runs, newest started first."""
        ...


class MetricRepository(FeatureRepository, Protocol):
    """Dataset statistics, the input of baselines and anomaly detection."""

    def save_profile(self, profile: BatchProfile) -> None:
        """Store a batch profile, replacing the snapshot for the same dataset and scope."""
        ...

    def get_profile(self, dataset_id: str, scope_id: str) -> BatchProfile | None:
        """Return the dataset profile for a scope, or None if absent."""
        ...

    def list_profiles(self, dataset_id: str) -> list[BatchProfile]:
        """Return all stored dataset profiles without guaranteeing their order."""
        ...

    def list_profiles_between(
        self,
        dataset_id: str,
        *,
        source_id: str,
        start: datetime,
        end: datetime,
        as_of: datetime,
        limit: int = 1000,
    ) -> list[BatchProfile]:
        """Return profiles within a period range, received on or before as_of, ordered chronologically."""
        ...

    def list_comparable_profiles(
        self,
        dataset_id: str,
        *,
        source_id: str,
        granularity: str,
        schema_version: int,
        stats_version: str,
        config_hash: str,
        limit: int = 100,
    ) -> list[BatchProfile]:
        """Return comparable profiles, newest first, bounded by `limit`.

        Matches the identity used by history selection: source_id, granularity,
        schema_version, stats_version and config_hash.  Only eligible profiles
        with `complete` not explicitly false are returned.  Period windowing and
        de-duplication stay in data_quality.history.select_history.
        """
        ...

    def set_profile_eligible(
        self, dataset_id: str, scope_id: str, eligible: bool
    ) -> None:
        """Update whether a stored profile may contribute to quality history."""
        ...

    def save_metric(self, point: MetricPoint) -> None:
        """Insert or update the point with the same dataset, scope, metric, column and version."""
        ...

    def list_metric_history(
        self,
        dataset_id: str,
        metric: MetricKind,
        column: str | None = None,
        limit: int = 100,
    ) -> list[MetricPoint]:
        """Return points of one metric and column, newest observed first."""
        ...


class QualityRepository(Protocol):
    """Results of deterministic checks and policy decisions."""

    def evaluation_lock(
        self, dataset_id: str, scope_id: str
    ) -> AbstractContextManager:
        """Hold an exclusive lock for evaluation work on one dataset and scope."""
        ...

    def get_evaluation(self, evaluation_id: str) -> Evaluation | None:
        """Return an evaluation with its latest saved executions, or None if absent."""
        ...

    def save_evaluation(self, evaluation: Evaluation) -> None:
        """Persist an evaluation while preserving its immutable plan.

        A completed evaluation may only change delivery state. Creating a new
        evaluation clears the scope's previously projected quality results.
        Raise ValueError for invalid snapshots or conflicting updates.
        """
        ...

    def save_execution(self, evaluation_id: str, execution: CheckExecution) -> None:
        """Update execution progress without changing frozen evidence or finished results.

        Raise ValueError for unknown or completed evaluations and conflicting updates.
        """
        ...

    def list_evaluations(self, dataset_id: str, scope_id: str) -> list[Evaluation]:
        """Return evaluations for a scope in reverse creation sequence."""
        ...

    def project_evaluation(self, evaluation: Evaluation) -> None:
        """Atomically update legacy read tables from the newest evaluation only."""
        ...

    def save_check_result(self, result: CheckResult) -> None:
        """Store a result; the latest result per check, scope and column wins.

        The result must carry a QualityContext, otherwise ValueError is raised.
        """
        ...

    def list_check_results(self, dataset_id: str, scope_id: str) -> list[CheckResult]:
        """Return results of one scope ordered by check_id and column."""
        ...

    def save_quality_decision(
        self,
        dataset_id: str,
        scope_id: str,
        decision: QualityDecision,
        decided_at: datetime,
    ) -> None:
        """Store the decision for a scope, replacing an earlier one."""
        ...

    def get_quality_decision(
        self, dataset_id: str, scope_id: str
    ) -> QualityDecision | None:
        """Return the projected decision for a scope, or None if absent."""
        ...


class AnomalyRepository(Protocol):
    """Detected anomalies."""

    def save_anomaly(self, anomaly: Anomaly) -> None:
        """Store an anomaly; an already stored anomaly_id is left unchanged."""
        ...

    def get_anomaly(self, anomaly_id: str) -> Anomaly | None:
        """Return the anomaly identified by anomaly_id, or None if absent."""
        ...

    def list_anomalies(
        self, dataset_id: str | None = None, limit: int = 100
    ) -> list[Anomaly]:
        """Return anomalies, newest detected first, optionally of one dataset."""
        ...


class UserActionRepository(Protocol):
    """Audit trail of user actions."""

    def record_action(self, action: UserAction) -> None:
        """Store an action; an already stored action_id is left unchanged."""
        ...

    def list_actions(
        self, target_type: str, target_id: str, limit: int = 100
    ) -> list[UserAction]:
        """Return actions on one entity, newest first."""
        ...
