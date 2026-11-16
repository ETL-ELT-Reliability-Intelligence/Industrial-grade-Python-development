from datetime import timedelta

import pytest

from tests.data_quality.test_statistical_checks import profile, suite, evaluate
from tests.stream_processor.test_handlers import NOW
from services.stream_processor.profiling import build_profile


@pytest.mark.parametrize("value,invalid", [
    ("2026-01-01T12:00:00Z", 0),
    ("2026-01-01T15:00:00+03:00", 0),
    ("2026-01-01T12:00:00", 1),
    ("2026-01-01", 1),
    ("not-a-date", 1),
    (None, 1),
])
def test_created_at_requires_aware_datetime(value, invalid):
    config = suite("value_rule", "created_at", rule="aware_datetime")
    current = profile(config, [{"created_at": value}])
    assert current.checks["test"]["invalid_count"] == invalid


def test_not_future_uses_saved_received_at_including_equal_boundary():
    config = suite("value_rule", "created_at", rule="not_future")
    received = NOW + timedelta(hours=4)
    rows = [
        {"created_at": (received - timedelta(seconds=1)).isoformat()},
        {"created_at": received.isoformat()},
        {"created_at": (received + timedelta(microseconds=1)).isoformat()},
    ]
    current = profile(config, rows)
    assert current.checks["test"]["invalid_count"] == 1
    repeated = build_profile(current.event, rows, config)
    assert repeated.checks == current.checks


@pytest.mark.parametrize("rows", [[], [{}]])
def test_missing_dates_never_pass(rows):
    config = suite("value_rule", "created_at", rule="aware_datetime")
    observation, _, _, _ = evaluate(config, profile(config, rows), [])
    assert observation.value is None
