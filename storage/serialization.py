"""JSON form of quality contracts stored in PostgreSQL JSONB columns.

Pure functions without database access. The encoded values contain only JSON
types; timestamps are ISO 8601 strings and durations are seconds.
"""

from collections.abc import Mapping
from datetime import timedelta
import json
from typing import Any

from pydantic import Field, TypeAdapter

from contracts.base import Contract
from contracts.quality import (
    CheckDetail, CheckKind, CheckResult, CheckStatus, Constraint,
    DecisionReason, FreshnessConstraint, FreshnessObservation,
    NullRateConstraint, NullRateObservation, Observation, QualityContext,
    RowCountConstraint, RowCountObservation, SchemaConstraint, SchemaField,
    SchemaObservation,
)


_FIELDS = TypeAdapter(tuple[SchemaField, ...] | None)
_DETAILS = TypeAdapter(tuple[CheckDetail, ...])
_REASONS = TypeAdapter(tuple[DecisionReason, ...])
_OBSERVATIONS = {
    CheckKind.FRESHNESS: FreshnessObservation,
    CheckKind.NULL_RATE: NullRateObservation,
    CheckKind.ROW_COUNT: RowCountObservation,
    CheckKind.SCHEMA: SchemaObservation,
}
_CONSTRAINTS = {
    CheckKind.NULL_RATE: NullRateConstraint,
    CheckKind.ROW_COUNT: RowCountConstraint,
    CheckKind.SCHEMA: SchemaConstraint,
}


class _StoredFreshnessConstraint(Contract):
    """Preserve the existing JSONB duration format, independently of model JSON."""

    max_age_seconds: float = Field(ge=0, allow_inf_nan=False)


def fields_to_json(fields: tuple[SchemaField, ...] | None) -> list[dict[str, str]] | None:
    """Encode schema fields, preserving None for an unavailable schema."""
    if fields is None:
        return None
    return [item.model_dump(mode="json") for item in fields]


def fields_from_json(data: list[dict[str, str]] | None) -> tuple[SchemaField, ...] | None:
    """Decode schema fields encoded by fields_to_json."""
    return _FIELDS.validate_json(json.dumps(data), strict=True)


def observation_to_json(observation: Observation) -> dict[str, Any]:
    """Encode any quality observation as a JSON object."""
    if isinstance(observation, tuple(_OBSERVATIONS.values())):
        return observation.model_dump(mode="json")
    raise ValueError("unsupported observation type")


def constraint_to_json(constraint: Constraint) -> dict[str, Any]:
    """Encode any quality constraint as a JSON object."""
    if isinstance(constraint, FreshnessConstraint):
        return {"max_age_seconds": constraint.max_age.total_seconds()}
    if isinstance(constraint, tuple(_CONSTRAINTS.values())):
        return constraint.model_dump(mode="json")
    raise ValueError("unsupported constraint type")


def observation_from_json(kind: CheckKind, data: Mapping[str, Any]) -> Observation:
    """Decode the observation of a check of the given kind."""
    return _OBSERVATIONS[kind].model_validate_json(json.dumps(dict(data)))


def constraint_from_json(kind: CheckKind, data: Mapping[str, Any]) -> Constraint:
    """Decode the constraint of a check of the given kind."""
    if kind is CheckKind.FRESHNESS:
        stored = _StoredFreshnessConstraint.model_validate_json(json.dumps(dict(data)))
        return FreshnessConstraint(max_age=timedelta(seconds=stored.max_age_seconds))
    return _CONSTRAINTS[kind].model_validate_json(json.dumps(dict(data)))


def details_to_json(details: tuple[CheckDetail, ...]) -> list[dict[str, str | None]]:
    """Encode structured check diagnostics."""
    return [item.model_dump(mode="json") for item in details]


def details_from_json(data: list[Mapping[str, Any]]) -> tuple[CheckDetail, ...]:
    """Decode structured check diagnostics."""
    return _DETAILS.validate_json(json.dumps(data), strict=True)


def reasons_to_json(reasons: tuple[DecisionReason, ...]) -> list[dict[str, str | None]]:
    """Encode the explanation of a quality decision."""
    return [item.model_dump(mode="json") for item in reasons]


def reasons_from_json(data: list[Mapping[str, Any]]) -> tuple[DecisionReason, ...]:
    """Decode the explanation of a quality decision."""
    return _REASONS.validate_json(json.dumps(data), strict=True)


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
        context=QualityContext(dataset_id=row["dataset_id"], scope_id=row["scope_id"], column=row["column_name"]),
        details=details_from_json(row["details"]),
    )
