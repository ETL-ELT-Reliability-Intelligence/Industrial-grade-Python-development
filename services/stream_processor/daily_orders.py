"""Check that received batches contain an order created on the previous UTC day."""

from datetime import datetime, timedelta, timezone
import logging

from data_quality.dates import aware_datetime
from services.stream_processor.raw import read_raw

logger = logging.getLogger(__name__)


def previous_day(cutoff: datetime) -> tuple[datetime, datetime]:
    """Return the previous UTC day as a half-open interval.

    Raise ValueError when cutoff lacks a timezone.
    """
    if cutoff.utcoffset() is None:
        raise ValueError("cutoff must include timezone")
    end = cutoff.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return end - timedelta(days=1), end


def check_daily_orders(batches, cutoff: datetime, *, reader=read_raw) -> dict:
    """Check for an order created during the previous UTC day.

    Inspect batches received between that day's start and cutoff, inclusive.
    Ignore malformed order timestamps. Return PASS on presence and WARN on
    absence; unreadable raw data propagates as a technical failure.
    """
    start, end = previous_day(cutoff)
    found = False
    for batch in batches:
        if not start <= batch["received_at"] <= cutoff:
            continue
        if not batch["object_key"]:
            raise ValueError("batch has no raw URI")
        for record in reader(batch["object_key"]):
            try:
                created = aware_datetime(record.get("created_at"))
            except (ValueError, TypeError):
                continue
            if start <= created < end and created <= batch["received_at"]:
                found = True
                break
        if found:
            break
    result = {
        "dataset_id": "mock_orders",
        "period_start": start.isoformat(),
        "period_end": end.isoformat(),
        "cutoff": cutoff.isoformat(),
        "status": "PASS" if found else "WARN",
        "has_orders": found,
    }
    log = logger.info if found else logger.warning
    log("Daily order presence: %s", result)
    return result


def run_daily_orders(connection, cutoff: datetime) -> dict:
    """Read recent mock-order batches and check the previous UTC day's presence."""
    start, _ = previous_day(cutoff)
    batches = connection.execute(
        "SELECT received_at, object_key FROM batches "
        "WHERE dataset_id = %s AND received_at >= %s AND received_at <= %s "
        "ORDER BY received_at DESC, batch_id DESC",
        ("mock_orders", start, cutoff),
    )
    return check_daily_orders(batches, cutoff)
