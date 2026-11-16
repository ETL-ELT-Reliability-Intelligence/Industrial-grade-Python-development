"""Dataset statistics computed from raw batch records."""

import json
from dataclasses import dataclass
from typing import Any

from contracts.quality import SchemaField

_TYPE_NAMES = {bool: "boolean", int: "integer", float: "number", str: "string"}


@dataclass(frozen=True)
class DatasetStatistics:
    """Statistics of one batch; missing values are nulls, not errors."""

    row_count: int
    schema: tuple[SchemaField, ...]
    null_rates: dict[str, float | None]
    distinct_counts: dict[str, int]


def compute_statistics(records: list[dict[str, Any]]) -> DatasetStatistics:
    """Compute row count, schema, null rates and distinct counts of a batch."""
    return DatasetStatistics(
        row_count=len(records),
        schema=infer_schema(records),
        null_rates=null_rates(records),
        distinct_counts=distinct_counts(records),
    )


def infer_schema(records: list[dict[str, Any]]) -> tuple[SchemaField, ...]:
    """Infer flat field types from values; mixed or empty columns are unknown."""
    columns = sorted({key for record in records for key in record})
    return tuple(
        SchemaField(name=column, data_type=field_type([record.get(column) for record in records]))
        for column in columns
    )


def null_rates(records: list[dict[str, Any]]) -> dict[str, float | None]:
    """Return the null fraction per column; an empty batch has no defined rate."""
    if not records:
        return {}
    columns = sorted({key for record in records for key in record})
    return {
        column: sum(record.get(column) is None for record in records) / len(records)
        for column in columns
    }


def distinct_counts(records: list[dict[str, Any]]) -> dict[str, int]:
    """Count distinct non-null values per column."""
    columns = sorted({key for record in records for key in record})
    return {
        column: len({_hashable(record.get(column)) for record in records if record.get(column) is not None})
        for column in columns
    }


def field_type(values: list[Any]) -> str:
    """Return canonical type name for a list of values; mixed or unknown for mixed/empty."""
    types = {_TYPE_NAMES.get(type(value), "object") for value in values if value is not None}
    return types.pop() if len(types) == 1 else "unknown"


def _hashable(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def statistics_from_profile(profile) -> DatasetStatistics:
    """Build DatasetStatistics from a BatchProfile's column summaries.

    Uses data_type from each column summary for schema,
    null_rate for null_rates, and distinct_count for distinct_counts.
    """
    columns = sorted(profile.columns)
    schema = tuple(
        SchemaField(name=column, data_type=profile.columns[column].get("data_type", "unknown"))
        for column in columns
    )
    null_rates = {
        column: profile.columns[column].get("null_rate")
        for column in columns
    }
    distinct_counts = {
        column: profile.columns[column].get("distinct_count", 0)
        for column in columns
    }
    return DatasetStatistics(
        row_count=profile.row_count,
        schema=schema,
        null_rates=null_rates,
        distinct_counts=distinct_counts,
    )
