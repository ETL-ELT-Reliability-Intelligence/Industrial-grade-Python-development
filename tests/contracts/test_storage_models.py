"""Pydantic boundaries shared by the migrated metric and state contracts."""

from datetime import datetime, timezone
import json

import pytest
from pydantic import ValidationError

from contracts.base import Contract
from contracts.metrics import COLUMN_METRICS, MetricKind, MetricPoint
from contracts.state import Anomaly, Batch, PipelineRun, PipelineRunStatus, UserAction

NOW = datetime(2026, 10, 11, tzinfo=timezone.utc)
SAMPLES = [
    Batch(dataset_id="orders", batch_id="b1", records=0, received_at=NOW, schema_version=1),
    PipelineRun(pipeline_id="p", run_id="r", status=PipelineRunStatus.RUNNING, started_at=NOW),
    PipelineRun(pipeline_id="p", run_id="r", status=PipelineRunStatus.SUCCESS,
                started_at=NOW, finished_at=NOW),
    PipelineRun(pipeline_id="p", run_id="r", status=PipelineRunStatus.FAILED,
                started_at=NOW, finished_at=NOW, error="boom"),
    Anomaly(anomaly_id="a", dataset_id="orders", metric="rows", expected=10, actual=1,
            score=1, detector="rule", detected_at=NOW),
    UserAction(action_id="u", actor="alice", action_type="ack", target_type="incident",
               target_id="i", performed_at=NOW, details={"nested": [1, None, True]}),
    *[MetricPoint(dataset_id="orders", scope_id="b1", metric=kind, observed_at=NOW,
                  stats_version="v1", column="id" if kind in COLUMN_METRICS else None, value=value)
      for kind in MetricKind for value in (None, 0, 1)],
]


@pytest.mark.parametrize("item", SAMPLES)
def test_roundtrip_immutability_and_extra_fields(item):
    model = type(item)
    assert isinstance(item, Contract)
    assert model.model_validate_json(item.model_dump_json()) == item
    with pytest.raises(ValidationError):
        model.model_validate(dict(item) | {"unexpected": 1})
    with pytest.raises(ValidationError):
        model.model_validate_json(json.dumps(item.model_dump(mode="json") | {"unexpected": 1}))
    name = next(iter(model.model_fields))
    with pytest.raises(ValidationError):
        setattr(item, name, getattr(item, name))


@pytest.mark.parametrize("kind", list(MetricKind))
@pytest.mark.parametrize("value", [True, "1", -1, float("nan"), float("inf")])
def test_metrics_reject_invalid_values_in_python_and_json(kind, value):
    data = dict(dataset_id="d", scope_id="b", metric=kind, observed_at=NOW,
                stats_version="v", column="id" if kind in COLUMN_METRICS else None, value=value)
    with pytest.raises(ValidationError):
        MetricPoint(**data)
    with pytest.raises(ValidationError):
        MetricPoint.model_validate_json(json.dumps(data | {"observed_at": NOW.isoformat()}))


@pytest.mark.parametrize("kind", [MetricKind.ROW_COUNT, MetricKind.DISTINCT_COUNT])
def test_counts_do_not_coerce_integral_floats(kind):
    data = dict(dataset_id="d", scope_id="b", metric=kind, observed_at=NOW,
                stats_version="v", column="id" if kind in COLUMN_METRICS else None)
    with pytest.raises(ValidationError):
        MetricPoint(**data, value=1.0)
    point = MetricPoint(**data, value=1)
    assert type(MetricPoint.model_validate_json(point.model_dump_json()).value) is int


def test_details_defaults_are_independent_and_validated():
    data = dict(action_id="u", actor="a", action_type="ack", target_type="incident",
                target_id="i", performed_at=NOW)
    first, second = UserAction(**data), UserAction(**data)
    first.details["note"] = "first"
    assert second.details == {}

    class InvalidDefault(UserAction):
        actor: str = 123

    with pytest.raises(ValidationError):
        InvalidDefault(**{k: v for k, v in data.items() if k != "actor"})


@pytest.mark.parametrize("status", list(PipelineRunStatus))
def test_pipeline_error_and_missing_finish_rules(status):
    data = dict(pipeline_id="p", run_id="r", status=status, started_at=NOW)
    if status is not PipelineRunStatus.RUNNING:
        with pytest.raises(ValidationError):
            PipelineRun(**data)
        data["finished_at"] = NOW
    if status is not PipelineRunStatus.FAILED:
        with pytest.raises(ValidationError):
            PipelineRun(**data, error="boom")
