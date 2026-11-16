"""Compute configured summaries without changing raw records."""

from bisect import bisect_right
from collections import Counter
from datetime import datetime
from decimal import Decimal, InvalidOperation
import json

from contracts.profiles import BatchProfile
from contracts.quality_config import PARAMETERS
from data_quality.dates import aware_datetime
from services.ml.volume import observe_hour
from .statistics import field_type


def number(value):
    """Parse a finite decimal, rejecting booleans and invalid values with ValueError."""
    if isinstance(value, bool):
        raise ValueError("boolean is not numeric")
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("invalid number") from exc
    if not result.is_finite():
        raise ValueError("non-finite number")
    return result


def date(value):
    """Parse an ISO date or datetime string without requiring a timezone."""
    if not isinstance(value, str):
        raise ValueError("date must be a string")
    return datetime.fromisoformat(value)


def summarize(records, column):
    """Summarize a column, treating missing keys as null and undefined ratios as None."""
    values = [r.get(column) for r in records]
    present = [v for v in values if v is not None]
    distinct = len({json.dumps(v, sort_keys=True) for v in present})
    return {
        "sample_count": len(values),
        "non_null_count": len(present),
        "null_count": len(values) - len(present),
        "null_rate": (len(values) - len(present)) / len(values) if values else None,
        "distinct_count": distinct,
        "distinct_ratio": distinct / len(present) if present else None,
        "data_type": field_type(values),
    }


def build_profile(event, records, suite):
    """Build batch measurements and check-specific summaries without evaluating policy."""
    columns = sorted({key for r in records for key in r})
    summaries = {column: summarize(records, column) for column in columns}
    checks = {}
    summary_cache: dict[tuple[str, str | None], dict] = {}
    for spec in suite.checks:
        if spec.column is None:
            continue
        params = PARAMETERS[spec.kind].model_validate_json(json.dumps(spec.params))
        condition = getattr(params, "where_column", None)
        cache_key = (spec.column, condition)
        if cache_key in summary_cache:
            summary = summary_cache[cache_key]
        elif condition:
            subset = [r for r in records if r.get(condition) == params.where_value]
            summary = summarize(subset, spec.column)
            summary_cache[cache_key] = summary
        else:
            # Колонки может не быть в батче: это missing_column, а не сбой профилирования.
            summary = summaries.get(spec.column, summarize(records, spec.column))
        summary = dict(summary)
        summary["missing_column"] = spec.column not in columns or bool(
            condition and condition not in columns
        )
        if spec.kind.value == "distribution":
            counts = Counter()
            errors = 0
            if condition:
                subset = [r for r in records if r.get(condition) == params.where_value]
            else:
                subset = records
            for record in subset:
                value = record.get(spec.column)
                if value is None:
                    continue
                try:
                    key = (
                        str(bisect_right(params.bins, number(value)))
                        if params.data_type == "numeric"
                        else json.dumps(value, sort_keys=True)
                    )
                    counts[key] += 1
                except (ValueError, TypeError):
                    errors += 1
            summary.update(
                frequencies=dict(counts),
                parse_errors=errors,
                valid_count=sum(counts.values()),
            )
        if spec.kind.value == "value_rule":
            invalid = evaluated = 0
            if params.other_column and params.other_column not in columns:
                summary["missing_column"] = True
            if condition:
                subset = [r for r in records if r.get(condition) == params.where_value]
            else:
                subset = records
            for record in subset:
                value = record.get(spec.column)
                other = record.get(params.other_column) if params.other_column else None
                if value is None or (params.rule == "date_order" and other is None):
                    if params.allow_null:
                        continue
                    evaluated += 1
                    invalid += 1
                    continue
                evaluated += 1
                try:
                    valid = True
                    if params.rule == "non_negative":
                        valid = number(value) >= 0
                    elif params.rule == "datetime":
                        date(value)
                    elif params.rule == "aware_datetime":
                        aware_datetime(value)
                    elif params.rule == "not_future":
                        valid = aware_datetime(value) <= event.received_at
                    else:
                        valid = date(value) >= date(other)
                    invalid += int(not valid)
                except (ValueError, TypeError, OverflowError):
                    invalid += 1
            summary.update(
                invalid_count=invalid,
                evaluated_count=evaluated,
                invalid_rate=invalid / evaluated if evaluated else None,
            )
        checks[spec.key] = summary
    return BatchProfile(
        event=event,
        config_hash=suite.fingerprint,
        row_count=len(records),
        columns=summaries,
        checks=checks,
        order_volume=observe_hour(event, records) if event.dataset_id == "mock_orders" else None,
    )
