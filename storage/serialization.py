"""JSON form of quality contracts stored in PostgreSQL JSONB columns.

Pure functions without database access. The encoded values contain only JSON
types; timestamps are ISO 8601 strings and durations are seconds.
"""

from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any

from contracts.quality import (
    CheckDetail, CheckKind, CheckResult, CheckStatus, Constraint,
    DecisionReason, FreshnessConstraint, FreshnessObservation,
    NullRateConstraint, NullRateObservation, Observation, QualityContext,
    RowCountConstraint, RowCountObservation, SchemaConstraint, SchemaField,
    SchemaObservation,
)


def _instant(value: str | None) -> datetime | None:
    """Parse an optional ISO 8601 timestamp."""
    return None if value is None else datetime.fromisoformat(value)


def fields_to_json(fields: tuple[SchemaField, ...] | None) -> list[dict[str, str]] | None:
    """Encode schema fields, preserving None for an unavailable schema."""
    if fields is None:
        return None
    return [{"name": item.name, "data_type": item.data_type} for item in fields]


def fields_from_json(data: list[dict[str, str]] | None) -> tuple[SchemaField, ...] | None:
    """Decode schema fields encoded by fields_to_json."""
    if data is None:
        return None
    return tuple(SchemaField(item["name"], item["data_type"]) for item in data)


def observation_to_json(observation: Observation) -> dict[str, Any]:
    """Encode any quality observation as a JSON object."""
    if isinstance(observation, FreshnessObservation):
        last = observation.last_updated_at
        return {
            "last_updated_at": None if last is None else last.isoformat(),
            "evaluated_at": observation.evaluated_at.isoformat(),
        }
    if isinstance(observation, NullRateObservation):
        return {"null_rate": observation.null_rate}
    if isinstance(observation, RowCountObservation):
        return {"row_count": observation.row_count}
    if isinstance(observation, SchemaObservation):
        return {"fields": fields_to_json(observation.fields)}
    raise ValueError("unsupported observation type")


def constraint_to_json(constraint: Constraint) -> dict[str, Any]:
    """Encode any quality constraint as a JSON object."""
    if isinstance(constraint, FreshnessConstraint):
        return {"max_age_seconds": constraint.max_age.total_seconds()}
    if isinstance(constraint, NullRateConstraint):
        return {"max_null_rate": constraint.max_null_rate}
    if isinstance(constraint, RowCountConstraint):
        return {"min_count": constraint.min_count, "max_count": constraint.max_count}
    if isinstance(constraint, SchemaConstraint):
        return {
            "fields": fields_to_json(constraint.fields),
            "allow_extra_fields": constraint.allow_extra_fields,
        }
    raise ValueError("unsupported constraint type")


def observation_from_json(kind: CheckKind, data: Mapping[str, Any]) -> Observation:
    """Decode the observation of a check of the given kind."""
    if kind is CheckKind.FRESHNESS:
        return FreshnessObservation(
            _instant(data["last_updated_at"]), datetime.fromisoformat(data["evaluated_at"]),
        )
    if kind is CheckKind.NULL_RATE:
        return NullRateObservation(data["null_rate"])
    if kind is CheckKind.ROW_COUNT:
        return RowCountObservation(data["row_count"])
    return SchemaObservation(fields_from_json(data["fields"]))


def constraint_from_json(kind: CheckKind, data: Mapping[str, Any]) -> Constraint:
    """Decode the constraint of a check of the given kind."""
    if kind is CheckKind.FRESHNESS:
        return FreshnessConstraint(timedelta(seconds=data["max_age_seconds"]))
    if kind is CheckKind.NULL_RATE:
        return NullRateConstraint(data["max_null_rate"])
    if kind is CheckKind.ROW_COUNT:
        return RowCountConstraint(data["min_count"], data["max_count"])
    return SchemaConstraint(fields_from_json(data["fields"]), data["allow_extra_fields"])


def details_to_json(details: tuple[CheckDetail, ...]) -> list[dict[str, str | None]]:
    """Encode structured check diagnostics."""
    return [
        {"code": item.code, "field": item.field, "expected": item.expected, "actual": item.actual}
        for item in details
    ]


def details_from_json(data: list[Mapping[str, Any]]) -> tuple[CheckDetail, ...]:
    """Decode structured check diagnostics."""
    return tuple(
        CheckDetail(item["code"], item["field"], item["expected"], item["actual"])
        for item in data
    )


def reasons_to_json(reasons: tuple[DecisionReason, ...]) -> list[dict[str, str | None]]:
    """Encode the explanation of a quality decision."""
    return [
        {"code": item.code, "message": item.message, "check_id": item.check_id}
        for item in reasons
    ]


def reasons_from_json(data: list[Mapping[str, Any]]) -> tuple[DecisionReason, ...]:
    """Decode the explanation of a quality decision."""
    return tuple(
        DecisionReason(item["code"], item["message"], item["check_id"]) for item in data
    )


def check_result_to_row(result: CheckResult) -> dict[str, Any]:
    """Return the column values of a check result; the context is mandatory."""
    context = result.context
    if context is None:
        raise ValueError("check result must have a context to be stored")
    return {
        "check_id": result.check_id,
        "dataset_id": context.dataset_id,
        "scope_id": context.scope_id,
        "column_name": context.column,
        "kind": result.kind.value,
        "status": result.status.value,
        "reason_code": result.reason_code,
        "message": result.message,
        "observation": observation_to_json(result.observation),
        "constraint_spec": constraint_to_json(result.constraint),
        "details": details_to_json(result.details),
    }


def check_result_from_row(row: Mapping[str, Any]) -> CheckResult:
    """Rebuild a check result from the column values of check_result_to_row."""
    kind = CheckKind(row["kind"])
    return CheckResult(
        check_id=row["check_id"],
        kind=kind,
        status=CheckStatus(row["status"]),
        observation=observation_from_json(kind, row["observation"]),
        constraint=constraint_from_json(kind, row["constraint_spec"]),
        reason_code=row["reason_code"],
        message=row["message"],
        context=QualityContext(row["dataset_id"], row["scope_id"], row["column_name"]),
        details=details_from_json(row["details"]),
    )
