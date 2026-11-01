"""Build frozen evidence and bounds for statistical and value checks."""

from math import ceil, floor, log2, sqrt
from statistics import median

from contracts.quality import (
    CheckKind,
    NullRateObservation,
    NullRateConstraint,
    RowCountObservation,
    RowCountConstraint,
    ScalarObservation,
    ScalarConstraint,
)


def spread(values, multiplier, floor_value):
    center = median(values)
    mad = median(abs(x - center) for x in values)
    return center, max(multiplier * 1.4826 * mad, floor_value)


def js_distance(left, right):
    """Base-2 Jensen–Shannon distance in [0, 1]; inputs are probability maps."""
    divergence = 0.0
    for key in left.keys() | right.keys():
        p, q = left.get(key, 0.0), right.get(key, 0.0)
        mid = (p + q) / 2
        if p:
            divergence += p * log2(p / mid) / 2
        if q:
            divergence += q * log2(q / mid) / 2
    return sqrt(max(0.0, min(1.0, divergence)))


def prepare(spec, params, current, history):
    """Return observation, constraint, skip reason, and baseline provenance."""
    baseline = {}
    history = list(history)
    summary = current.checks.get(spec.key, {})
    if spec.kind is CheckKind.VALUE_RULE:
        value = None if summary.get("missing_column") else summary.get("invalid_rate")
        return (
            ScalarObservation(value=value, measurements=summary),
            ScalarConstraint(maximum=params.max_invalid_rate),
            None,
            baseline,
        )
    if spec.kind is CheckKind.ROW_COUNT and params.mode == "fixed":
        return (
            RowCountObservation(row_count=current.row_count),
            RowCountConstraint(min_count=params.min_count, max_count=params.max_count),
            None,
            baseline,
        )
    if spec.kind is CheckKind.NULL_RATE and params.mode == "fixed":
        value = None if summary.get("missing_column") else summary.get("null_rate")
        return (
            NullRateObservation(null_rate=value),
            NullRateConstraint(max_null_rate=params.max_null_rate),
            None,
            baseline,
        )
    if not current.event.complete:
        return None, None, "incomplete_window", baseline
    if spec.column:
        if summary.get("missing_column"):
            return None, None, "missing_column", baseline
        if summary.get("parse_errors", 0):
            return None, None, "invalid_numeric_values", baseline
        count_key = (
            "sample_count" if spec.kind is CheckKind.NULL_RATE else "non_null_count"
        )
        if summary.get(count_key, 0) < params.min_samples:
            return None, None, "insufficient_samples", baseline
        history = [
            p
            for p in history
            if p.checks.get(spec.key, {}).get(count_key, 0) >= params.min_samples
        ]
        if spec.kind is CheckKind.CARDINALITY:
            n = summary["non_null_count"]
            history = [
                p
                for p in history
                if abs(p.checks[spec.key]["non_null_count"] - n)
                <= n * params.volume_tolerance
            ]
    history = history[: params.history_window]
    baseline = {
        "batch_ids": [p.event.batch_id for p in history],
        "stats_version": current.stats_version,
        "config_hash": current.config_hash,
    }
    if len(history) < params.min_history:
        return None, None, "insufficient_history", baseline
    if spec.kind is CheckKind.ROW_COUNT:
        values = [p.row_count for p in history]
        center, width = spread(
            values,
            params.mad_multiplier,
            max(params.absolute_tolerance, median(values) * params.relative_tolerance),
        )
        baseline.update(median=center, width=width, values=values)
        return (
            RowCountObservation(row_count=current.row_count),
            RowCountConstraint(
                min_count=max(0, ceil(center - width)), max_count=floor(center + width)
            ),
            None,
            baseline,
        )
    summaries = [p.checks[spec.key] for p in history]
    if spec.kind is CheckKind.NULL_RATE:
        values = [p["null_rate"] for p in summaries]
        center, width = spread(values, params.mad_multiplier, params.min_increase)
        # A conservative sampling margin also protects small batches near zero.
        margin = 3 * sqrt(
            max(center * (1 - center), 1 / summary["sample_count"])
            / summary["sample_count"]
        )
        maximum = min(1.0, center + max(width, margin))
        baseline.update(
            median=center,
            width=width,
            sampling_margin=margin,
            sample_count=summary["sample_count"],
            values=values,
        )
        return (
            NullRateObservation(null_rate=summary["null_rate"]),
            NullRateConstraint(max_null_rate=maximum),
            None,
            baseline,
        )
    if spec.kind is CheckKind.CARDINALITY:
        scores = []
        for metric, tolerance in (
            ("distinct_count", params.count_tolerance),
            ("distinct_ratio", params.ratio_tolerance),
        ):
            values = [p[metric] for p in summaries]
            minimum_width = (
                max(1.0, median(values) * tolerance)
                if metric == "distinct_count"
                else max(1 / summary["non_null_count"], tolerance)
            )
            center, width = spread(values, params.mad_multiplier, minimum_width)
            scores.append(abs(summary[metric] - center) / width)
            baseline[metric] = {"median": center, "width": width}
        return (
            ScalarObservation(value=max(scores), measurements=summary),
            ScalarConstraint(maximum=1.0),
            None,
            baseline,
        )
    reference = {}
    for item in summaries:
        for key, count in item["frequencies"].items():
            reference[key] = reference.get(key, 0.0) + count / item[
                "valid_count"
            ] / len(summaries)
    actual = {
        key: count / summary["valid_count"]
        for key, count in summary["frequencies"].items()
    }
    baseline.update(
        probabilities=reference, bins=list(params.bins), data_type=params.data_type
    )
    return (
        ScalarObservation(value=js_distance(actual, reference), measurements=summary),
        ScalarConstraint(maximum=params.max_distance),
        None,
        baseline,
    )
