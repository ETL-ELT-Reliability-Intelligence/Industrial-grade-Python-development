"""Parsing shared by timestamp checks and daily order presence checks."""

from datetime import datetime


def aware_datetime(value: object) -> datetime:
    """Parse an ISO timestamp string with an explicit UTC offset.

    Preserve the supplied offset. Raise ValueError for non-string, malformed
    or timezone-naive input.
    """
    if not isinstance(value, str):
        raise ValueError("timestamp must be a string")
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() is None:
        raise ValueError("timestamp must include time and timezone")
    return parsed
