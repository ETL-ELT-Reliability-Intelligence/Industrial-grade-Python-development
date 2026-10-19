"""Validate PostgreSQL row adapters independently of a database server."""

from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

pytest.importorskip("psycopg")

from contracts.metrics import MetricKind
from contracts.quality import QualityStatus, SchemaField
from contracts.state import PipelineRunStatus
from storage.postgres.repositories import (
    PostgresDatasetRepository, PostgresPipelineRunRepository, PostgresMetricRepository,
    PostgresQualityRepository, PostgresAnomalyRepository, PostgresUserActionRepository,
)

NOW = datetime(2026, 10, 11, tzinfo=timezone.utc)


def connection_with(row):
    connection = Mock()
    connection.execute.return_value.fetchone.return_value = row
    connection.execute.return_value.fetchall.return_value = [row]
    return connection


def test_batch_and_schema_rows():
    repository = PostgresDatasetRepository(connection_with(dict(
        dataset_id="d", batch_id="b", records=1, received_at=NOW, schema_version=1, object_key=None,
    )))
    assert repository.get_batch("d", "b") == repository.list_batches("d")[0]
    field = SchemaField(name="id", data_type="integer")
    repository = PostgresDatasetRepository(connection_with({"fields": [field.model_dump()]}))
    assert repository.get_schema("d", 1) == (field,)
    assert set(repository.get_schema("d", 1)) == {field}


@pytest.mark.parametrize("kind,value", [(MetricKind.ROW_COUNT, 12.0),
                                        (MetricKind.DISTINCT_COUNT, 12.0),
                                        (MetricKind.NULL_RATE, 0.2),
                                        (MetricKind.FRESHNESS_SECONDS, None)])
def test_postgres_numeric_values_restore_metric_types(kind, value):
    column = "id" if kind in (MetricKind.DISTINCT_COUNT, MetricKind.NULL_RATE) else None
    row = dict(dataset_id="d", scope_id="b", metric=kind.value, value=value,
               column_name=column, observed_at=NOW, stats_version="v1")
    result = PostgresMetricRepository(connection_with(row)).list_metric_history("d", kind, column)[0]
    assert result.metric is kind
    assert result.value == value
    if kind in (MetricKind.ROW_COUNT, MetricKind.DISTINCT_COUNT):
        assert type(result.value) is int


def test_pipeline_and_quality_restore_enums_and_nested_models():
    row = dict(pipeline_id="p", run_id="r", status="failed", started_at=NOW,
               finished_at=NOW, error="boom")
    repository = PostgresPipelineRunRepository(connection_with(row))
    assert repository.get_pipeline_run("p", "r").status is PipelineRunStatus.FAILED
    assert repository.list_pipeline_runs("p")[0] == repository.get_pipeline_run("p", "r")
    row = dict(status="WARN", reasons=[dict(code="no_checks", message="No checks.", check_id=None)])
    decision = PostgresQualityRepository(connection_with(row)).get_quality_decision("d", "b")
    assert decision.status is QualityStatus.WARN
    assert decision.reasons[0].code == "no_checks"


def test_anomaly_and_audit_rows():
    row = dict(anomaly_id="a", dataset_id="d", metric="rows", expected=10.0, actual=1.0,
               score=0.9, detector="rule", detected_at=NOW, batch_id=None, model_version=None)
    repository = PostgresAnomalyRepository(connection_with(row))
    assert repository.get_anomaly("a") == repository.list_anomalies()[0]
    row = dict(action_id="u", actor="a", action_type="ack", target_type="incident", target_id="i",
               performed_at=NOW, details={"comment": "hello"})
    action = PostgresUserActionRepository(connection_with(row)).list_actions("incident", "i")[0]
    assert action.details == {"comment": "hello"}
