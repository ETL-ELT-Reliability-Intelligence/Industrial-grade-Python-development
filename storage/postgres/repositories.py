"""PostgreSQL implementations of the repositories in storage.repositories."""

from datetime import datetime
from typing import Any

from psycopg import Connection
from psycopg.types.json import Jsonb

from contracts.metrics import MetricKind, MetricPoint
from contracts.quality import (
    CheckResult, QualityDecision, QualityStatus, SchemaField,
)
from contracts.state import Anomaly, Batch, PipelineRun, PipelineRunStatus, UserAction
from storage.serialization import (
    check_result_from_row, check_result_to_row, fields_from_json, fields_to_json,
    reasons_from_json, reasons_to_json,
)

Conn = Connection[dict[str, Any]]
_COUNT_METRICS = frozenset({MetricKind.ROW_COUNT, MetricKind.DISTINCT_COUNT})


def _limit(value: int) -> int:
    """Return value if it is a positive integer, otherwise raise ValueError."""
    if type(value) is not int or value < 1:
        raise ValueError("limit must be a positive integer")
    return value


def _aware(value: datetime, name: str) -> None:
    """Raise ValueError unless value is a timezone-aware datetime."""
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError(f"{name} must be a timezone-aware datetime")


class _PostgresRepository:
    """Common access to the connection and dataset registration."""

    def __init__(self, connection: Conn) -> None:
        self._connection = connection

    def _ensure_dataset(self, dataset_id: str) -> None:
        self._connection.execute(
            "INSERT INTO datasets (dataset_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (dataset_id,),
        )


class PostgresDatasetRepository(_PostgresRepository):
    """Datasets, schema versions and batches."""

    def ensure_dataset(self, dataset_id: str) -> None:
        self._ensure_dataset(dataset_id)

    def save_schema(
        self, dataset_id: str, schema_version: int,
        fields: tuple[SchemaField, ...], observed_at: datetime,
    ) -> None:
        _aware(observed_at, "observed_at")
        with self._connection.transaction():
            self._ensure_dataset(dataset_id)
            cursor = self._connection.execute(
                "INSERT INTO dataset_schemas (dataset_id, schema_version, fields, observed_at) "
                "VALUES (%s, %s, %s, %s) ON CONFLICT (dataset_id, schema_version) DO NOTHING",
                (dataset_id, schema_version, Jsonb(fields_to_json(fields)), observed_at),
            )
            if cursor.rowcount == 0:
                stored = self.get_schema(dataset_id, schema_version)
                if set(stored or ()) != set(fields):
                    raise ValueError(
                        f"schema version {schema_version} of {dataset_id} "
                        "is already stored with different fields"
                    )

    def get_schema(self, dataset_id: str, schema_version: int) -> tuple[SchemaField, ...] | None:
        row = self._connection.execute(
            "SELECT fields FROM dataset_schemas WHERE dataset_id = %s AND schema_version = %s",
            (dataset_id, schema_version),
        ).fetchone()
        return None if row is None else fields_from_json(row["fields"])

    def upsert_batch(self, batch: Batch) -> None:
        with self._connection.transaction():
            self._ensure_dataset(batch.dataset_id)
            self._connection.execute(
                "INSERT INTO batches "
                "(dataset_id, batch_id, records, received_at, schema_version, object_key) "
                "VALUES (%s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (dataset_id, batch_id) DO UPDATE SET "
                "records = EXCLUDED.records, received_at = EXCLUDED.received_at, "
                "schema_version = EXCLUDED.schema_version, "
                "object_key = COALESCE(EXCLUDED.object_key, batches.object_key)",
                (batch.dataset_id, batch.batch_id, batch.records, batch.received_at,
                 batch.schema_version, batch.object_key),
            )

    def get_batch(self, dataset_id: str, batch_id: str) -> Batch | None:
        row = self._connection.execute(
            "SELECT * FROM batches WHERE dataset_id = %s AND batch_id = %s",
            (dataset_id, batch_id),
        ).fetchone()
        return None if row is None else _batch(row)

    def list_batches(self, dataset_id: str, limit: int = 100) -> list[Batch]:
        rows = self._connection.execute(
            "SELECT * FROM batches WHERE dataset_id = %s "
            "ORDER BY received_at DESC, batch_id DESC LIMIT %s",
            (dataset_id, _limit(limit)),
        ).fetchall()
        return [_batch(row) for row in rows]


def _batch(row: dict[str, Any]) -> Batch:
    return Batch(
        dataset_id=row["dataset_id"], batch_id=row["batch_id"], records=row["records"],
        received_at=row["received_at"], schema_version=row["schema_version"],
        object_key=row["object_key"],
    )


class PostgresPipelineRunRepository(_PostgresRepository):
    """History of pipeline runs."""

    def upsert_pipeline_run(self, run: PipelineRun) -> None:
        self._connection.execute(
            "INSERT INTO pipeline_runs (pipeline_id, run_id, status, started_at, finished_at, error) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (pipeline_id, run_id) DO UPDATE SET "
            "status = EXCLUDED.status, started_at = EXCLUDED.started_at, "
            "finished_at = EXCLUDED.finished_at, error = EXCLUDED.error "
            "WHERE pipeline_runs.status = 'running'",
            (run.pipeline_id, run.run_id, run.status.value, run.started_at,
             run.finished_at, run.error),
        )

    def get_pipeline_run(self, pipeline_id: str, run_id: str) -> PipelineRun | None:
        row = self._connection.execute(
            "SELECT * FROM pipeline_runs WHERE pipeline_id = %s AND run_id = %s",
            (pipeline_id, run_id),
        ).fetchone()
        return None if row is None else _pipeline_run(row)

    def list_pipeline_runs(self, pipeline_id: str, limit: int = 100) -> list[PipelineRun]:
        rows = self._connection.execute(
            "SELECT * FROM pipeline_runs WHERE pipeline_id = %s "
            "ORDER BY started_at DESC, run_id DESC LIMIT %s",
            (pipeline_id, _limit(limit)),
        ).fetchall()
        return [_pipeline_run(row) for row in rows]


def _pipeline_run(row: dict[str, Any]) -> PipelineRun:
    return PipelineRun(
        pipeline_id=row["pipeline_id"], run_id=row["run_id"],
        status=PipelineRunStatus(row["status"]), started_at=row["started_at"],
        finished_at=row["finished_at"], error=row["error"],
    )


class PostgresMetricRepository(_PostgresRepository):
    """Dataset statistics."""

    def save_metric(self, point: MetricPoint) -> None:
        with self._connection.transaction():
            self._ensure_dataset(point.dataset_id)
            self._connection.execute(
                "INSERT INTO dataset_metrics "
                "(dataset_id, scope_id, metric, column_name, value, observed_at, stats_version) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (dataset_id, scope_id, metric, (COALESCE(column_name, '')), "
                "stats_version) DO UPDATE SET value = EXCLUDED.value, "
                "observed_at = EXCLUDED.observed_at, recorded_at = now()",
                (point.dataset_id, point.scope_id, point.metric.value, point.column,
                 point.value, point.observed_at, point.stats_version),
            )

    def list_metric_history(
        self, dataset_id: str, metric: MetricKind,
        column: str | None = None, limit: int = 100,
    ) -> list[MetricPoint]:
        rows = self._connection.execute(
            "SELECT * FROM dataset_metrics WHERE dataset_id = %s AND metric = %s "
            "AND column_name IS NOT DISTINCT FROM %s::text "
            "ORDER BY observed_at DESC, id DESC LIMIT %s",
            (dataset_id, metric.value, column, _limit(limit)),
        ).fetchall()
        return [_metric(row) for row in rows]


def _metric(row: dict[str, Any]) -> MetricPoint:
    metric = MetricKind(row["metric"])
    value = row["value"]
    if value is not None and metric in _COUNT_METRICS:
        value = int(value)
    return MetricPoint(
        dataset_id=row["dataset_id"], scope_id=row["scope_id"], metric=metric,
        observed_at=row["observed_at"], stats_version=row["stats_version"],
        value=value, column=row["column_name"],
    )


class PostgresQualityRepository(_PostgresRepository):
    """Check results and policy decisions."""

    def save_check_result(self, result: CheckResult) -> None:
        row = check_result_to_row(result)
        with self._connection.transaction():
            self._ensure_dataset(row["dataset_id"])
            self._connection.execute(
                "INSERT INTO quality_check_results "
                "(check_id, dataset_id, scope_id, column_name, kind, status, reason_code, "
                "message, observation, constraint_spec, details) "
                "VALUES (%(check_id)s, %(dataset_id)s, %(scope_id)s, %(column_name)s, "
                "%(kind)s, %(status)s, %(reason_code)s, %(message)s, %(observation)s, "
                "%(constraint_spec)s, %(details)s) "
                "ON CONFLICT (check_id, dataset_id, scope_id, (COALESCE(column_name, ''))) "
                "DO UPDATE SET kind = EXCLUDED.kind, status = EXCLUDED.status, "
                "reason_code = EXCLUDED.reason_code, message = EXCLUDED.message, "
                "observation = EXCLUDED.observation, constraint_spec = EXCLUDED.constraint_spec, "
                "details = EXCLUDED.details, recorded_at = now()",
                {
                    **row,
                    "observation": Jsonb(row["observation"]),
                    "constraint_spec": Jsonb(row["constraint_spec"]),
                    "details": Jsonb(row["details"]),
                },
            )

    def list_check_results(self, dataset_id: str, scope_id: str) -> list[CheckResult]:
        rows = self._connection.execute(
            "SELECT * FROM quality_check_results WHERE dataset_id = %s AND scope_id = %s "
            "ORDER BY check_id, COALESCE(column_name, '')",
            (dataset_id, scope_id),
        ).fetchall()
        return [check_result_from_row(row) for row in rows]

    def save_quality_decision(
        self, dataset_id: str, scope_id: str, decision: QualityDecision, decided_at: datetime,
    ) -> None:
        _aware(decided_at, "decided_at")
        with self._connection.transaction():
            self._ensure_dataset(dataset_id)
            self._connection.execute(
                "INSERT INTO quality_decisions (dataset_id, scope_id, status, reasons, decided_at) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (dataset_id, scope_id) DO UPDATE SET "
                "status = EXCLUDED.status, reasons = EXCLUDED.reasons, "
                "decided_at = EXCLUDED.decided_at",
                (dataset_id, scope_id, decision.status.value,
                 Jsonb(reasons_to_json(decision.reasons)), decided_at),
            )

    def get_quality_decision(self, dataset_id: str, scope_id: str) -> QualityDecision | None:
        row = self._connection.execute(
            "SELECT status, reasons FROM quality_decisions WHERE dataset_id = %s AND scope_id = %s",
            (dataset_id, scope_id),
        ).fetchone()
        if row is None:
            return None
        return QualityDecision(status=QualityStatus(row["status"]), reasons=reasons_from_json(row["reasons"]))


class PostgresAnomalyRepository(_PostgresRepository):
    """Detected anomalies."""

    def save_anomaly(self, anomaly: Anomaly) -> None:
        with self._connection.transaction():
            self._ensure_dataset(anomaly.dataset_id)
            self._connection.execute(
                "INSERT INTO anomalies (anomaly_id, dataset_id, batch_id, metric, expected, "
                "actual, score, detector, model_version, detected_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (anomaly_id) DO NOTHING",
                (anomaly.anomaly_id, anomaly.dataset_id, anomaly.batch_id, anomaly.metric,
                 anomaly.expected, anomaly.actual, anomaly.score, anomaly.detector,
                 anomaly.model_version, anomaly.detected_at),
            )

    def get_anomaly(self, anomaly_id: str) -> Anomaly | None:
        row = self._connection.execute(
            "SELECT * FROM anomalies WHERE anomaly_id = %s", (anomaly_id,),
        ).fetchone()
        return None if row is None else _anomaly(row)

    def list_anomalies(self, dataset_id: str | None = None, limit: int = 100) -> list[Anomaly]:
        rows = self._connection.execute(
            "SELECT * FROM anomalies WHERE (%s::text IS NULL OR dataset_id = %s::text) "
            "ORDER BY detected_at DESC, anomaly_id LIMIT %s",
            (dataset_id, dataset_id, _limit(limit)),
        ).fetchall()
        return [_anomaly(row) for row in rows]


def _anomaly(row: dict[str, Any]) -> Anomaly:
    return Anomaly(
        anomaly_id=row["anomaly_id"], dataset_id=row["dataset_id"], metric=row["metric"],
        expected=row["expected"], actual=row["actual"], score=row["score"],
        detector=row["detector"], detected_at=row["detected_at"],
        batch_id=row["batch_id"], model_version=row["model_version"],
    )


class PostgresUserActionRepository(_PostgresRepository):
    """Audit trail of user actions."""

    def record_action(self, action: UserAction) -> None:
        self._connection.execute(
            "INSERT INTO user_actions (action_id, actor, action_type, target_type, target_id, "
            "performed_at, details) VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (action_id) DO NOTHING",
            (action.action_id, action.actor, action.action_type, action.target_type,
             action.target_id, action.performed_at, Jsonb(action.details)),
        )

    def list_actions(self, target_type: str, target_id: str, limit: int = 100) -> list[UserAction]:
        rows = self._connection.execute(
            "SELECT * FROM user_actions WHERE target_type = %s AND target_id = %s "
            "ORDER BY performed_at DESC, action_id LIMIT %s",
            (target_type, target_id, _limit(limit)),
        ).fetchall()
        return [
            UserAction(
                action_id=row["action_id"], actor=row["actor"], action_type=row["action_type"],
                target_type=row["target_type"], target_id=row["target_id"],
                performed_at=row["performed_at"], details=row["details"],
            )
            for row in rows
        ]
