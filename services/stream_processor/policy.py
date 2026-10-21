"""Thresholds and constraint builders for stream quality evaluation."""

from dataclasses import dataclass
from datetime import timedelta
import os
from statistics import median
from typing import Mapping

from contracts.metrics import MetricPoint
from contracts.quality import (
    FreshnessConstraint,
    NullRateConstraint,
    RowCountConstraint,
    SchemaConstraint,
    SchemaField,
)


@dataclass(frozen=True)
class QualityThresholds:
    """Configurable bounds for deterministic stream quality checks."""

    max_freshness_seconds: int = 3600
    max_null_rate: float = 0.05
    row_count_history_window: int = 10
    row_count_min_history: int = 3
    row_count_min_ratio: float = 0.5
    row_count_max_ratio: float = 2.0

    def __post_init__(self) -> None:
        if self.max_freshness_seconds < 0:
            raise ValueError("max_freshness_seconds must be non-negative")
        if not 0.0 <= self.max_null_rate <= 1.0:
            raise ValueError("max_null_rate must be within [0, 1]")
        if self.row_count_history_window < 1 or self.row_count_min_history < 1:
            raise ValueError("history window and min_history must be positive")
        if self.row_count_min_ratio < 0 or self.row_count_max_ratio < self.row_count_min_ratio:
            raise ValueError("invalid row_count ratio bounds")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "QualityThresholds":
        """Read thresholds from environment variables, falling back to defaults."""
        source = os.environ if env is None else env
        return cls(
            max_freshness_seconds=int(source.get("QUALITY_MAX_FRESHNESS_SECONDS", 3600)),
            max_null_rate=float(source.get("QUALITY_MAX_NULL_RATE", 0.05)),
            row_count_history_window=int(source.get("QUALITY_ROW_COUNT_HISTORY_WINDOW", 10)),
            row_count_min_history=int(source.get("QUALITY_ROW_COUNT_MIN_HISTORY", 3)),
            row_count_min_ratio=float(source.get("QUALITY_ROW_COUNT_MIN_RATIO", 0.5)),
            row_count_max_ratio=float(source.get("QUALITY_ROW_COUNT_MAX_RATIO", 2.0)),
        )

    def freshness_constraint(self) -> FreshnessConstraint:
        return FreshnessConstraint(max_age=timedelta(seconds=self.max_freshness_seconds))

    def null_rate_constraint(self) -> NullRateConstraint:
        return NullRateConstraint(max_null_rate=self.max_null_rate)

    def schema_constraint(self, fields: tuple[SchemaField, ...]) -> SchemaConstraint:
        return SchemaConstraint(fields=fields, allow_extra_fields=False)

    def row_count_constraint(
        self, history: list[MetricPoint], *, exclude_scope: str | None = None,
    ) -> RowCountConstraint | None:
        """Build bounds from the median of recent row_count points, if enough exist."""
        values = [
            int(point.value)
            for point in history
            if point.value is not None and point.scope_id != exclude_scope
        ][: self.row_count_history_window]
        if len(values) < self.row_count_min_history:
            return None
        baseline = float(median(values))
        return RowCountConstraint(
            min_count=max(0, int(baseline * self.row_count_min_ratio)),
            max_count=max(0, int(baseline * self.row_count_max_ratio)),
        )
