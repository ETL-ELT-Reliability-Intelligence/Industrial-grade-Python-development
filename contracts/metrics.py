"""Dataset statistic contracts produced by batch and stream processing.

None denotes an unavailable measurement, never a measured zero.
"""

from enum import Enum
from typing import Annotated, Self

from pydantic import AwareDatetime, Field, model_validator

from contracts.base import Contract, NonBlankText

NonNegativeNumber = Annotated[int, Field(ge=0)] | Annotated[float, Field(ge=0, allow_inf_nan=False)]


class MetricKind(str, Enum):
    """Supported dataset statistics and their stable identifiers."""

    ROW_COUNT = "row_count"
    NULL_RATE = "null_rate"
    DISTINCT_COUNT = "distinct_count"
    FRESHNESS_SECONDS = "freshness_seconds"


COLUMN_METRICS = frozenset({MetricKind.NULL_RATE, MetricKind.DISTINCT_COUNT})


class MetricPoint(Contract):
    """One statistic of one scope (batch, partition or window) of a dataset.

    scope_id has the same meaning as QualityContext.scope_id. stats_version
    names the calculation, so different calculations never overwrite each other.
    Column metrics require a column; dataset-level metrics forbid it.
    """

    dataset_id: NonBlankText
    scope_id: NonBlankText
    metric: MetricKind
    observed_at: AwareDatetime
    stats_version: NonBlankText
    value: NonNegativeNumber | None = None
    column: NonBlankText | None = None

    @model_validator(mode="after")
    def consistent_metric(self) -> Self:
        if self.metric in COLUMN_METRICS:
            if self.column is None:
                raise ValueError("column is required for column metrics")
        elif self.column is not None:
            raise ValueError(f"{self.metric.value} is a dataset-level metric")
        if self.value is not None:
            if self.metric in (MetricKind.ROW_COUNT, MetricKind.DISTINCT_COUNT):
                if type(self.value) is not int:
                    raise ValueError("count metrics require an integer value")
            elif self.metric is MetricKind.NULL_RATE and self.value > 1:
                raise ValueError("null_rate must be in [0, 1]")
        return self
