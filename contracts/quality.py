"""Source-independent data quality contracts.

These models validate inputs; they do not evaluate checks or apply policies.
None denotes an unavailable observation, never a successful measurement.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from math import isfinite


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


def _text(value: str, name: str) -> None:
    """Raise ValueError unless value is a non-blank string."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _rate(value: float, name: str) -> None:
    """Raise ValueError unless value is a finite fraction, excluding bool."""
    if type(value) not in (int, float) or not isfinite(value) or not 0 <= value <= 1:
        raise ValueError(f"{name} must be a finite number in [0, 1]")


def _count(value: int, name: str) -> None:
    """Raise ValueError unless value is a non-negative integer, excluding bool."""
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")


def _timestamp(value: datetime, name: str) -> None:
    """Raise ValueError unless value is a timezone-aware datetime."""
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError(f"{name} must be a timezone-aware datetime")


@dataclass(frozen=True)
class QualityContext:
    """Opaque identifiers supplied by the caller for one evaluation scope.

    scope_id identifies a batch, partition, or stream window. Its meaning and
    completeness are owned by the caller, not by the quality library.
    """

    dataset_id: str
    scope_id: str
    column: str | None = None

    def __post_init__(self) -> None:
        """Validate scope identifiers and the optional column name."""
        _text(self.dataset_id, "dataset_id")
        _text(self.scope_id, "scope_id")
        if self.column is not None:
            _text(self.column, "column")


@dataclass(frozen=True)
class FreshnessObservation:
    """Last update time and an explicit evaluation time supplied by the caller."""

    last_updated_at: datetime | None
    evaluated_at: datetime

    def __post_init__(self) -> None:
        """Require aware timestamps and reject updates after evaluation time."""
        _timestamp(self.evaluated_at, "evaluated_at")
        if self.last_updated_at is not None:
            _timestamp(self.last_updated_at, "last_updated_at")
            if self.last_updated_at > self.evaluated_at:
                raise ValueError("last_updated_at must not be after evaluated_at")


@dataclass(frozen=True)
class FreshnessConstraint:
    """Maximum age, inclusive; no implicit wall clock or schedule."""

    max_age: timedelta

    def __post_init__(self) -> None:
        """Require a non-negative duration as the maximum permitted age."""
        if not isinstance(self.max_age, timedelta) or self.max_age < timedelta(0):
            raise ValueError("max_age must be a non-negative timedelta")


@dataclass(frozen=True)
class NullRateObservation:
    """A precomputed fraction; None also represents an undefined empty sample."""

    null_rate: float | None

    def __post_init__(self) -> None:
        """Validate the observed fraction when it is available."""
        if self.null_rate is not None:
            _rate(self.null_rate, "null_rate")


@dataclass(frozen=True)
class NullRateConstraint:
    """Maximum null fraction, inclusive."""

    max_null_rate: float

    def __post_init__(self) -> None:
        """Require a finite maximum null fraction in the inclusive range [0, 1]."""
        _rate(self.max_null_rate, "max_null_rate")


@dataclass(frozen=True)
class RowCountObservation:
    """Measured row count; None denotes an unavailable measurement."""

    row_count: int | None

    def __post_init__(self) -> None:
        """Validate the observed count while preserving missing observations."""
        if self.row_count is not None:
            _count(self.row_count, "row_count")


@dataclass(frozen=True)
class RowCountConstraint:
    """Inclusive bounds; equal bounds express an exact count."""

    min_count: int | None = None
    max_count: int | None = None

    def __post_init__(self) -> None:
        """Require at least one non-negative integer bound and consistent ordering."""
        if self.min_count is None and self.max_count is None:
            raise ValueError("at least one row count bound is required")
        for name in ("min_count", "max_count"):
            value = getattr(self, name)
            if value is not None:
                _count(value, name)
        if (self.min_count is not None and self.max_count is not None
                and self.min_count > self.max_count):
            raise ValueError("min_count must not exceed max_count")


@dataclass(frozen=True)
class SchemaField:
    """Flat field; adapters agree on canonical type names before calling checks."""

    name: str
    data_type: str

    def __post_init__(self) -> None:
        """Require non-blank field and canonical type names."""
        _text(self.name, "name")
        _text(self.data_type, "data_type")


def _fields(value: tuple[SchemaField, ...]) -> None:
    """Raise ValueError unless fields form a tuple with unique field names."""
    if not isinstance(value, tuple) or any(not isinstance(f, SchemaField) for f in value):
        raise ValueError("schema fields must be a tuple of SchemaField objects")
    if len({f.name for f in value}) != len(value):
        raise ValueError("schema field names must be unique")


@dataclass(frozen=True)
class SchemaObservation:
    """None means unavailable; an empty tuple is an observed empty schema."""

    fields: tuple[SchemaField, ...] | None

    def __post_init__(self) -> None:
        """Validate the schema fields when a schema observation is available."""
        if self.fields is not None:
            _fields(self.fields)


@dataclass(frozen=True)
class SchemaConstraint:
    """Names/types match exactly, order is irrelevant; all listed fields required."""

    fields: tuple[SchemaField, ...]
    allow_extra_fields: bool = False

    def __post_init__(self) -> None:
        """Validate expected fields and the explicit extra-field allowance."""
        _fields(self.fields)
        if type(self.allow_extra_fields) is not bool:
            raise ValueError("allow_extra_fields must be a bool")


Observation = FreshnessObservation | NullRateObservation | RowCountObservation | SchemaObservation
Constraint = FreshnessConstraint | NullRateConstraint | RowCountConstraint | SchemaConstraint

_CHECK_TYPES = {
    CheckKind.FRESHNESS: (FreshnessObservation, FreshnessConstraint),
    CheckKind.NULL_RATE: (NullRateObservation, NullRateConstraint),
    CheckKind.ROW_COUNT: (RowCountObservation, RowCountConstraint),
    CheckKind.SCHEMA: (SchemaObservation, SchemaConstraint),
}


@dataclass(frozen=True)
class CheckDetail:
    """Structured diagnostic, e.g. field='id', code='type_mismatch'."""

    code: str
    field: str | None = None
    expected: str | None = None
    actual: str | None = None

    def __post_init__(self) -> None:
        """Require a diagnostic code and non-blank optional detail values."""
        _text(self.code, "code")
        for name in ("field", "expected", "actual"):
            value = getattr(self, name)
            if value is not None:
                _text(value, name)


@dataclass(frozen=True)
class CheckResult:
    """Evidence from one rule; status describes compliance, not an action.

    reason_code is a stable machine-readable identifier; message is explanatory.
    The check implementation owns the consistency of status with the evidence.
    """

    check_id: str
    kind: CheckKind
    status: CheckStatus
    observation: Observation
    constraint: Constraint
    reason_code: str
    message: str
    context: QualityContext | None = None
    details: tuple[CheckDetail, ...] = ()

    def __post_init__(self) -> None:
        """Validate result metadata and contract types without evaluating evidence."""
        for name in ("check_id", "reason_code", "message"):
            _text(getattr(self, name), name)
        if not isinstance(self.kind, CheckKind) or not isinstance(self.status, CheckStatus):
            raise ValueError("kind and status must be check enums")
        observation_type, constraint_type = _CHECK_TYPES[self.kind]
        if not isinstance(self.observation, observation_type):
            raise ValueError("observation does not match check kind")
        if not isinstance(self.constraint, constraint_type):
            raise ValueError("constraint does not match check kind")
        if self.context is not None and not isinstance(self.context, QualityContext):
            raise ValueError("context must be a QualityContext")
        if not isinstance(self.details, tuple) or any(
            not isinstance(d, CheckDetail) for d in self.details
        ):
            raise ValueError("details must be a tuple of CheckDetail objects")


@dataclass(frozen=True)
class PolicyRule:
    """Reaction for a check_id: failures block, unavailable observations warn.

    Overrides may choose WARN or BLOCK, but cannot turn a problem into PASS.
    """

    check_id: str
    on_fail: QualityStatus = QualityStatus.BLOCK
    on_unknown: QualityStatus = QualityStatus.WARN

    def __post_init__(self) -> None:
        """Require a rule identifier and WARN or BLOCK reactions to problems."""
        _text(self.check_id, "check_id")
        for value in (self.on_fail, self.on_unknown):
            if not isinstance(value, QualityStatus) or value is QualityStatus.PASS:
                raise ValueError("policy reactions must be QualityStatus.WARN or BLOCK")


@dataclass(frozen=True)
class DecisionReason:
    """Policy explanation; check_id=None allows reasons such as no_checks."""

    code: str
    message: str
    check_id: str | None = None

    def __post_init__(self) -> None:
        """Validate the explanation and its optional reference to a check."""
        _text(self.code, "code")
        _text(self.message, "message")
        if self.check_id is not None:
            _text(self.check_id, "check_id")


@dataclass(frozen=True)
class QualityDecision:
    """Policy output for one caller-selected scope; never executes an action."""

    status: QualityStatus
    reasons: tuple[DecisionReason, ...]

    def __post_init__(self) -> None:
        """Require a policy status and at least one typed explanation."""
        if not isinstance(self.status, QualityStatus):
            raise ValueError("status must be a QualityStatus")
        if not isinstance(self.reasons, tuple) or not self.reasons or any(
            not isinstance(r, DecisionReason) for r in self.reasons
        ):
            raise ValueError("reasons must be a non-empty tuple of DecisionReason objects")
