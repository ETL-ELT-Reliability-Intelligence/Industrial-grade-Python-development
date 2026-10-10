"""Strict event boundaries and the Kafka JSON representation."""

from datetime import datetime, timedelta, timezone
import json
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from contracts.events import (
    AnomalyDetectedEvent, DataQualityFailedEvent, DatasetIngestedEvent,
    EventType, PipelineFailedEvent, PipelineFinishedEvent,
    PipelineStartedEvent, SchemaChangedEvent, parse_event,
)

NOW = datetime(2026, 10, 10, tzinfo=timezone.utc)
LATER = NOW + timedelta(seconds=90)
CASES = [
    (DatasetIngestedEvent, dict(dataset_id="orders", batch_id="b1", records=0,
                               received_at=NOW, schema_version=1, raw_uri="raw/orders/b1")),
    (SchemaChangedEvent, dict(dataset_id="orders", batch_id="b1", previous_version=1,
                             new_version=2, detected_at=NOW)),
    (PipelineStartedEvent, dict(pipeline_id="pipeline", run_id="run", started_at=NOW)),
    (PipelineFinishedEvent, dict(pipeline_id="pipeline", run_id="run", started_at=NOW,
                                finished_at=LATER, duration_seconds=90)),
    (PipelineFailedEvent, dict(pipeline_id="pipeline", run_id="run", started_at=NOW,
                              failed_at=LATER, error="boom")),
    (DataQualityFailedEvent, dict(dataset_id="orders", scope_id="b1", check_id="rows",
                                 reason_code="below_minimum", failed_at=NOW)),
    (AnomalyDetectedEvent, dict(anomaly_id="a1", dataset_id="orders", batch_id="b1",
                               metric="row_count", expected=100, actual=10, score=0.9,
                               detector="rule", model_version=None, detected_at=NOW)),
]


@pytest.mark.parametrize("model,values", CASES)
def test_round_trip_and_routing(model, values):
    event = model(**values)
    assert isinstance(event.event_id, UUID)
    assert event.event_id != model(**values).event_id
    assert event.contract_version == 1
    assert event.topic == event.event_type.value
    assert event.key == values.get("dataset_id", values.get("pipeline_id"))
    wire = event.model_dump_json()
    assert json.loads(wire)["event_id"] == str(event.event_id)
    for payload in (wire, wire.encode("utf-8")):
        restored = parse_event(payload)
        assert type(restored) is model
        assert restored == event
        assert model.model_validate_json(payload) == event
    explicit_id = uuid4()
    assert model(**values, event_id=explicit_id).event_id == explicit_id


@pytest.mark.parametrize("model,values", CASES)
@pytest.mark.parametrize("changes", [
    {"contract_version": True}, {"contract_version": "1"}, {"contract_version": 1.0},
    {"contract_version": 0}, {"contract_version": 2}, {"contract_version": None},
    {"event_id": "invalid"}, {"event_id": 42}, {"event_id": None}, {"extra": 1},
    {"event_type": "unknown"},
])
def test_invalid_envelope_in_python_and_json(model, values, changes):
    with pytest.raises(ValidationError):
        model(**(values | changes))
    payload = model(**values).model_dump(mode="json") | changes
    with pytest.raises(ValueError):
        parse_event(json.dumps(payload))


@pytest.mark.parametrize("model,values", CASES)
def test_frozen_and_concrete_event_type(model, values):
    event = model(**values)
    with pytest.raises(ValidationError):
        event.event_id = uuid4()
    other_type = (EventType.PIPELINE_STARTED if model is DatasetIngestedEvent
                  else EventType.DATASET_INGESTED)
    with pytest.raises(ValidationError):
        model(**values, event_type=other_type)


@pytest.mark.parametrize("model,values", CASES)
def test_required_fields_cannot_be_omitted(model, values):
    for name, field in model.model_fields.items():
        if not field.is_required():
            continue
        incomplete = {key: value for key, value in values.items() if key != name}
        with pytest.raises(ValidationError):
            model(**incomplete)
        payload = model(**values).model_dump(mode="json")
        del payload[name]
        with pytest.raises(ValidationError):
            parse_event(json.dumps(payload))


@pytest.mark.parametrize("model,values", CASES)
def test_strict_fields_in_python_and_json(model, values):
    event = model(**values)
    for name, value in values.items():
        if isinstance(value, datetime):
            invalid = [value.replace(tzinfo=None), 0]
        elif isinstance(value, str) or name == "model_version":
            invalid = ["", " \t\n", 123]
        elif isinstance(value, (int, float)):
            invalid = [True, str(value), float("inf"), float("nan")]
        else:
            continue
        for bad in invalid:
            with pytest.raises(ValidationError):
                model(**(values | {name: bad}))
            wire_value = bad.isoformat() if isinstance(bad, datetime) else bad
            payload = event.model_dump(mode="json") | {name: wire_value}
            with pytest.raises(ValueError):
                parse_event(json.dumps(payload))


@pytest.mark.parametrize("payload", ['null', '[]', '1', '"text"', '{}', '{',
                                     '{"event_type": []}', '{"event_type": "unknown"}'])
def test_invalid_payload(payload):
    with pytest.raises(ValueError):
        parse_event(payload)


@pytest.mark.parametrize("index,changes", [
    (0, {"records": -1}), (0, {"schema_version": 0}), (0, {"records": 1.5}),
    (1, {"new_version": 1}), (1, {"previous_version": 0}), (1, {"new_version": 0}),
    (3, {"finished_at": NOW - timedelta(seconds=1)}),
    (3, {"duration_seconds": -1}), (3, {"duration_seconds": 91}),
    (4, {"failed_at": NOW - timedelta(seconds=1)}),
    (6, {"score": -0.1}), (6, {"score": 1.1}),
])
def test_ranges_and_consistency(index, changes):
    model, values = CASES[index]
    with pytest.raises(ValidationError):
        model(**(values | changes))
    payload = model(**values).model_dump(mode="json")
    payload.update({k: v.isoformat() if isinstance(v, datetime) else v for k, v in changes.items()})
    with pytest.raises(ValueError):
        parse_event(json.dumps(payload))


def test_optional_values_boundaries_and_equal_instants():
    dataset_model, dataset_values = CASES[0]
    dataset = dataset_model(**(dataset_values | {"raw_uri": None}))
    assert parse_event(dataset.model_dump_json()) == dataset
    model, values = CASES[6]
    for score in (0, 1):
        event = model(**(values | dict(score=score, batch_id=None, model_version=None)))
        assert parse_event(event.model_dump_json()) == event
    event = PipelineFailedEvent(
        pipeline_id="p", run_id="r", started_at=NOW,
        failed_at=NOW.astimezone(timezone(timedelta(hours=3))), error=None,
    )
    assert parse_event(event.model_dump_json()) == event
    event = PipelineFinishedEvent(
        pipeline_id="p", run_id="r", started_at=NOW, finished_at=NOW, duration_seconds=0,
    )
    assert parse_event(event.model_dump_json()) == event
