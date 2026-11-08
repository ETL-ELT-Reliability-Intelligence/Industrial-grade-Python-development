from datetime import datetime, timedelta, timezone

import pytest

from contracts.events import DatasetIngestedEvent
from contracts.profiles import BatchProfile
from services.ml.volume import (
    HOUR, OBSERVATION_VERSION, FEATURES, observe_hour, training_rows, train, Predictor,
    lag_features, calendar_features,
)

START = datetime(2026, 1, 5, tzinfo=timezone.utc)
CUTOFF = START + timedelta(days=28, hours=1)


def event(start=START, **overrides):
    values = dict(dataset_id="mock_orders", batch_id=start.isoformat(), source_id="mock",
                  granularity="hour", period_start=start, period_end=start + HOUR,
                  received_at=start + HOUR + timedelta(minutes=5), records=1, schema_version=1)
    values.update(overrides)
    return DatasetIngestedEvent(**values)


def profiles():
    return [BatchProfile(event=event(START + i * HOUR), config_hash="test", row_count=100,
                         order_volume={"version": OBSERVATION_VERSION, "count": 100}) for i in range(672)]


def test_observation_uses_creation_time_and_complete_hours():
    assert observe_hour(event(), [{"created_at": START.isoformat()}])["count"] == 1
    assert observe_hour(event(records=0), [])["count"] == 0
    assert observe_hour(event(complete=False), []) ["reason"] == "incomplete_hour"
    assert observe_hour(event(received_at=START), []) ["reason"] == "open_hour"
    assert observe_hour(event(), [{"created_at": (START + HOUR).isoformat()}])["reason"] == "orders_outside_hour"
    assert observe_hour(event(), [{}])["reason"] == "invalid_created_at"
    delayed = event(received_at=START + 5 * HOUR)
    assert observe_hour(delayed, [{"created_at": START.isoformat()}])["count"] == 1


def test_history_excludes_missing_hours_future_deliveries_and_bad_revisions():
    history = profiles()
    assert len(training_rows(history, CUTOFF)) == 504
    assert training_rows(history[1:], CUTOFF) == []
    bad = history[0].model_copy(update={
        "event": event(received_at=START + 3 * HOUR, batch_id="replacement"),
        "order_volume": {"version": OBSERVATION_VERSION, "reason": "invalid_created_at"},
    })
    assert training_rows([*history, bad], CUTOFF) == []
    future = bad.model_copy(update={"event": event(received_at=CUTOFF + HOUR)})
    assert training_rows([*history, future], CUTOFF) == training_rows(history, CUTOFF)


def test_features_use_only_history_available_at_start():
    history = profiles()
    origin = START + 7 * 24 * HOUR
    inputs = lag_features(origin, history)
    assert len(inputs["values"]) == len(FEATURES)
    assert inputs["values"][FEATURES.index("lag_1")] is None
    assert inputs["values"][FEATURES.index("lag_24")] == 100
    assert inputs["values"][FEATURES.index("rolling_168_available")] == 167
    changed = [p.model_copy(update={"order_volume": {"version": OBSERVATION_VERSION, "count": 9999}})
               if p.event.period_start >= origin else p for p in history]
    assert lag_features(origin, changed) == inputs
    assert calendar_features([START]) == calendar_features([START.astimezone(timezone(timedelta(hours=3)))])


def test_trained_model_handles_spikes_drops_and_empty_schema():
    pytest.importorskip("sklearn")
    rows = training_rows(profiles(), CUTOFF)
    stored = train(rows, CUTOFF)
    assert stored["metadata"]["training_hours"] == 336
    assert stored["metadata"]["calibration_hours"] == 168
    predictor = Predictor()
    inputs = lag_features(CUTOFF, profiles())
    normal = predictor.predict(stored, CUTOFF, 100, inputs)
    assert normal["status"] == "PASS" and normal["expected"] == 100
    assert predictor.predict(stored, CUTOFF, 1000, inputs)["status"] == "WARN"
    assert predictor.predict(stored, CUTOFF, 0, inputs)["direction"] == "decrease"
    assert 0 <= normal["lower"] <= normal["expected"] <= normal["upper"]
    assert train(rows, CUTOFF)["model_version"] == stored["model_version"]
    assert observe_hour(event(records=0, schema_version=2), [])["count"] == 0


def test_calibration_does_not_refit_on_later_volume():
    pytest.importorskip("sklearn")
    rows = training_rows(profiles(), CUTOFF)
    changed = [(hour, count if i < 336 else 200, batch, inputs)
               for i, (hour, count, batch, inputs) in enumerate(rows)]
    stored = train(changed, CUTOFF)
    result = Predictor().predict(stored, CUTOFF, 100, lag_features(CUTOFF, profiles()))
    assert result["expected"] == 100
    assert stored["metadata"]["interval_padding"] == pytest.approx(0.8)


def test_regression_learns_source_day_night_pattern():
    pytest.importorskip("sklearn")
    history = [p.model_copy(update={"order_volume": {"version": OBSERVATION_VERSION,
                "count": 50 if p.event.period_start.hour < 7 else 300}}) for p in profiles()]
    rows = training_rows(history, CUTOFF)
    model = train(rows, CUTOFF)
    predictor = Predictor()
    origin = CUTOFF.replace(hour=2)
    night = predictor.predict(model, origin, 50, lag_features(origin, history))
    origin = CUTOFF.replace(hour=14)
    day = predictor.predict(model, origin, 300, lag_features(origin, history))
    assert night["status"] == day["status"] == "PASS"
    assert night["expected"] < day["expected"] / 2
    origin = CUTOFF.replace(hour=2)
    assert predictor.predict(model, origin, 1000, lag_features(origin, history))["status"] == "WARN"


def test_zero_is_a_value_but_missing_and_late_revisions_are_not():
    origin = START + 8 * 24 * HOUR
    history = profiles()
    previous = next(p for p in history if p.event.period_start == origin - 24 * HOUR)
    zero = previous.model_copy(update={
        "event": previous.event.model_copy(update={"batch_id": "zero", "received_at": origin - HOUR}),
        "order_volume": {"version": OBSERVATION_VERSION, "count": 0},
    })
    values = lag_features(origin, [*history, zero])["values"]
    assert values[FEATURES.index("lag_24")] == 0
    invalid = zero.model_copy(update={"order_volume": {"version": OBSERVATION_VERSION, "reason": "invalid"}})
    assert lag_features(origin, [*history, invalid]) is None
    late = invalid.model_copy(update={"event": invalid.event.model_copy(update={"received_at": origin + HOUR})})
    assert lag_features(origin, [*history, late]) == lag_features(origin, history)


def test_weekly_history_gap_is_explicit():
    assert lag_features(CUTOFF, profiles()[-100:]) is None


def test_forecast_changes_with_lags_at_the_same_calendar_hour():
    pytest.importorskip("sklearn")
    history = []
    for index, profile in enumerate(profiles()):
        count = 50 if (index // 24) % 4 < 2 else 300
        history.append(profile.model_copy(update={
            "order_volume": {"version": OBSERVATION_VERSION, "count": count},
        }))
    stored = train(training_rows(history, CUTOFF), CUTOFF)
    low = lag_features(START + timedelta(days=21, hours=12), history)
    high = lag_features(START + timedelta(days=14, hours=12), history)
    assert low["values"][:5] == high["values"][:5]
    predictor = Predictor()
    assert predictor.predict(stored, CUTOFF, 100, high)["expected"] > (
        predictor.predict(stored, CUTOFF, 100, low)["expected"] + 100
    )
