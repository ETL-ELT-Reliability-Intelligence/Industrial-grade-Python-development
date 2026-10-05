"""Dataset statistic contracts produced by batch and stream processing.

None denotes an unavailable measurement, never a measured zero.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from contracts._validation import _count, _number, _rate, _text, _timestamp


class MetricKind(str, Enum):
    """Supported dataset statistics and their stable identifiers."""

    ROW_COUNT = "row_count"
    NULL_RATE = "null_rate"
    DISTINCT_COUNT = "distinct_count"
    FRESHNESS_SECONDS = "freshness_seconds"


COLUMN_METRICS = frozenset({MetricKind.NULL_RATE, MetricKind.DISTINCT_COUNT})


@dataclass(frozen=True)
class MetricPoint:
    """One statistic of one scope (batch, partition or window) of a dataset.

    scope_id has the same meaning as QualityContext.scope_id. stats_version
    names the calculation, so different calculations never overwrite each other.
    Column metrics require a column; dataset-level metrics forbid it.
    """

    dataset_id: str
    scope_id: str
    metric: MetricKind
    observed_at: datetime
    stats_version: str
    value: int | float | None = None
    column: str | None = None

    def __post_init__(self) -> None:
        """Validate identifiers, column usage and the value range of the metric."""
        for name in ("dataset_id", "scope_id", "stats_version"):
            _text(getattr(self, name), name)
        if not isinstance(self.metric, MetricKind):
            raise ValueError("metric must be a MetricKind")
        _timestamp(self.observed_at, "observed_at")
        if self.metric in COLUMN_METRICS:
            _text(self.column, "column")
        elif self.column is not None:
            raise ValueError(f"{self.metric.value} is a dataset-level metric")
        if self.value is None:
            return
        if self.metric in (MetricKind.ROW_COUNT, MetricKind.DISTINCT_COUNT):
            _count(self.value, "value")
        elif self.metric is MetricKind.NULL_RATE:
            _rate(self.value, "value")
        else:
            _number(self.value, "value")
            if self.value < 0:
                raise ValueError("value must not be negative")
