"""Source-independent data quality contracts.

These models validate inputs; they do not evaluate checks or apply policies.
None denotes an unavailable observation, never a successful measurement.
"""

from datetime import timedelta
from enum import Enum
from typing import Self

from pydantic import AwareDatetime, Field, field_validator, model_validator

from contracts.base import Contract, Count, NonBlankText, Rate


class CheckKind(str, Enum):
    """Supported deterministic checks and their stable identifiers."""

    FRESHNESS = "freshness"
    NULL_RATE = "null_rate"
    ROW_COUNT = "row_count"
    SCHEMA = "schema"


class CheckStatus(str, Enum):
    """Compliance outcome, including observations that cannot be evaluated."""

    PASS = "PASS"
    FAIL = "FAIL"
    UNKNOWN = "UNKNOWN"


class QualityStatus(str, Enum):
    """Policy decision for the caller to act upon."""

    PASS = "PASS"
    WARN = "WARN"
    BLOCK = "BLOCK"


class QualityContext(Contract):
    """Opaque identifiers supplied by the caller for one evaluation scope.

    scope_id identifies a batch, partition, or stream window. Its meaning and
    completeness are owned by the caller, not by the quality library.
    """

    dataset_id: NonBlankText
    scope_id: NonBlankText
    column: NonBlankText | None = None


class FreshnessObservation(Contract):
    """Last update time and an explicit evaluation time supplied by the caller."""

    last_updated_at: AwareDatetime | None
    evaluated_at: AwareDatetime

    @model_validator(mode="after")
    def chronological(self) -> Self:
        if self.last_updated_at is not None and self.last_updated_at > self.evaluated_at:
            raise ValueError("last_updated_at must not be after evaluated_at")
        return self


class FreshnessConstraint(Contract):
    """Maximum age, inclusive; no implicit wall clock or schedule."""

    max_age: timedelta = Field(ge=timedelta(0))


class NullRateObservation(Contract):
    """A precomputed fraction; None also represents an undefined empty sample."""

    null_rate: Rate | None


class NullRateConstraint(Contract):
    """Maximum null fraction, inclusive."""

    max_null_rate: Rate


class RowCountObservation(Contract):
    """Measured row count; None denotes an unavailable measurement."""

    row_count: Count | None


class RowCountConstraint(Contract):
    """Inclusive bounds; equal bounds express an exact count."""

    min_count: Count | None = None
    max_count: Count | None = None

    @model_validator(mode="after")
    def consistent_bounds(self) -> Self:
        if self.min_count is None and self.max_count is None:
            raise ValueError("at least one row count bound is required")
        if self.min_count is not None and self.max_count is not None and self.min_count > self.max_count:
            raise ValueError("min_count must not exceed max_count")
        return self


class SchemaField(Contract):
    """Flat field; adapters agree on canonical type names before calling checks."""

    name: NonBlankText
    data_type: NonBlankText


class SchemaObservation(Contract):
    """None means unavailable; an empty tuple is an observed empty schema."""

    fields: tuple[SchemaField, ...] | None

    @field_validator("fields")
    @classmethod
    def unique_fields(cls, value: tuple[SchemaField, ...] | None) -> tuple[SchemaField, ...] | None:
        if value is not None and len({field.name for field in value}) != len(value):
            raise ValueError("schema field names must be unique")
        return value


class SchemaConstraint(Contract):
    """Names/types match exactly, order is irrelevant; all listed fields required."""

    fields: tuple[SchemaField, ...]
    allow_extra_fields: bool = False

    @field_validator("fields")
    @classmethod
    def unique_fields(cls, value: tuple[SchemaField, ...] | None) -> tuple[SchemaField, ...] | None:
        if value is not None and len({field.name for field in value}) != len(value):
            raise ValueError("schema field names must be unique")
        return value


Observation = FreshnessObservation | NullRateObservation | RowCountObservation | SchemaObservation


Constraint = FreshnessConstraint | NullRateConstraint | RowCountConstraint | SchemaConstraint


_CHECK_TYPES = {
    CheckKind.FRESHNESS: (FreshnessObservation, FreshnessConstraint),
    CheckKind.NULL_RATE: (NullRateObservation, NullRateConstraint),
    CheckKind.ROW_COUNT: (RowCountObservation, RowCountConstraint),
    CheckKind.SCHEMA: (SchemaObservation, SchemaConstraint),
}


class CheckDetail(Contract):
    """Structured diagnostic, e.g. field='id', code='type_mismatch'."""

    code: NonBlankText
    field: NonBlankText | None = None
    expected: NonBlankText | None = None
    actual: NonBlankText | None = None


class CheckResult(Contract):
    """Evidence from one rule; status describes compliance, not an action.

    reason_code is a stable machine-readable identifier; message is explanatory.
    The check implementation owns the consistency of status with the evidence.
    """

    check_id: NonBlankText
    kind: CheckKind
    status: CheckStatus
    observation: Observation
    constraint: Constraint
    reason_code: NonBlankText
    message: NonBlankText
    context: QualityContext | None = None
    details: tuple[CheckDetail, ...] = ()

    @model_validator(mode="after")
    def matching_evidence(self) -> Self:
        observation_type, constraint_type = _CHECK_TYPES[self.kind]
        if not isinstance(self.observation, observation_type):
            raise ValueError("observation does not match check kind")
        if not isinstance(self.constraint, constraint_type):
            raise ValueError("constraint does not match check kind")
        return self


class PolicyRule(Contract):
    """Reaction for a check_id: failures block, unavailable observations warn.

    Overrides may choose WARN or BLOCK, but cannot turn a problem into PASS.
    """

    check_id: NonBlankText
    on_fail: QualityStatus = QualityStatus.BLOCK
    on_unknown: QualityStatus = QualityStatus.WARN

    @field_validator("on_fail", "on_unknown")
    @classmethod
    def problem_reaction(cls, value: QualityStatus) -> QualityStatus:
        if value is QualityStatus.PASS:
            raise ValueError("policy reactions must be QualityStatus.WARN or BLOCK")
        return value


class DecisionReason(Contract):
    """Policy explanation; check_id=None allows reasons such as no_checks."""

    code: NonBlankText
    message: NonBlankText
    check_id: NonBlankText | None = None


class QualityDecision(Contract):
    """Policy output for one caller-selected scope; never executes an action."""

    status: QualityStatus
    reasons: tuple[DecisionReason, ...] = Field(min_length=1)
