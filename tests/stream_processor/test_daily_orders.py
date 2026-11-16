from datetime import datetime, timezone
import logging

import pytest

from services.stream_processor.daily_orders import check_daily_orders, previous_day

CUTOFF = datetime(2026, 11, 9, 1, tzinfo=timezone.utc)


@pytest.mark.parametrize("value,expected", [
    ("2026-11-08T00:00:00Z", "PASS"),
    ("2026-11-08T23:59:59Z", "PASS"),
    ("2026-11-09T00:00:00Z", "WARN"),
    ("2026-11-07T23:59:59Z", "WARN"),
    ("2026-11-09T02:00:00+03:00", "PASS"),
    ("2026-11-08", "WARN"),
    (None, "WARN"),
    ("bad", "WARN"),
])
def test_presence_uses_creation_date(value, expected):
    batches = [{"received_at": CUTOFF, "object_key": "raw"}]
    result = check_daily_orders(batches, CUTOFF, reader=lambda _: [{"created_at": value}])
    assert result["status"] == expected


def test_absent_and_empty_batches_warn(caplog):
    with caplog.at_level(logging.WARNING):
        assert check_daily_orders([], CUTOFF)["status"] == "WARN"
        assert check_daily_orders(
            [{"received_at": CUTOFF, "object_key": "raw"}], CUTOFF, reader=lambda _: []
        )["status"] == "WARN"
    assert "WARN" in caplog.text


def test_unreadable_batch_is_not_reported_as_absent():
    def fail(_):
        raise OSError("unavailable")
    with pytest.raises(OSError):
        check_daily_orders([{"received_at": CUTOFF, "object_key": "raw"}], CUTOFF, reader=fail)


def test_previous_day_uses_utc_and_calendar_boundary():
    start, end = previous_day(datetime(2027, 1, 1, 1, tzinfo=timezone.utc))
    assert start.isoformat() == "2026-12-31T00:00:00+00:00"
    assert end.isoformat() == "2027-01-01T00:00:00+00:00"


def test_orders_received_after_cutoff_are_not_used():
    from datetime import timedelta
    batches = [{"received_at": CUTOFF + timedelta(seconds=1), "object_key": "raw"}]
    assert check_daily_orders(
        batches, CUTOFF, reader=lambda _: [{"created_at": "2026-11-08T12:00:00Z"}]
    )["status"] == "WARN"


def test_future_relative_to_receipt_is_not_a_valid_daily_order():
    batches = [{"received_at": datetime(2026, 11, 8, 12, tzinfo=timezone.utc), "object_key": "raw"}]
    assert check_daily_orders(
        batches, CUTOFF, reader=lambda _: [{"created_at": "2026-11-08T13:00:00Z"}]
    )["status"] == "WARN"
