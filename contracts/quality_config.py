"""Typed quality configuration shared by plans and configuration loading."""

import hashlib
import json
from typing import Literal
from pydantic import Field, model_validator
from contracts.base import Contract, NonBlankText
from contracts.quality import CheckKind, QualityStatus, SchemaField


class RetrySettings(Contract):
    max_attempts: int = Field(default=3, ge=1, le=10)
    delays_seconds: tuple[float, ...] = (5.0, 15.0)

    @model_validator(mode="after")
    def valid_delays(self):
        if len(self.delays_seconds) != self.max_attempts - 1:
            raise ValueError("one delay is required for each retry")
        if any(not 0 <= delay <= 60 for delay in self.delays_seconds):
            raise ValueError("retry delays must be between 0 and 60 seconds")
        return self


class Reactions(Contract):
    on_fail: QualityStatus = QualityStatus.BLOCK
    on_unknown: QualityStatus = QualityStatus.WARN

    @model_validator(mode="after")
    def no_pass(self):
        if QualityStatus.PASS in (self.on_fail, self.on_unknown):
            raise ValueError("problems cannot map to PASS")
        return self


class FreshnessParams(Contract):
    max_age_seconds: int = Field(default=3600, ge=0)


class NullRateParams(Contract):
    max_null_rate: float = Field(default=0.05, ge=0, le=1, allow_inf_nan=False)


class RowCountParams(Contract):
    mode: Literal["historical", "fixed"] = "historical"
    history_window: int = Field(default=10, ge=1)
    min_history: int = Field(default=3, ge=1)
    min_ratio: float = Field(default=0.5, ge=0, allow_inf_nan=False)
    max_ratio: float = Field(default=2.0, ge=0, allow_inf_nan=False)
    min_count: int | None = Field(default=None, ge=0)
    max_count: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def bounds(self):
        if self.min_history > self.history_window or self.min_ratio > self.max_ratio:
            raise ValueError("invalid historical bounds")
        if self.mode == "fixed":
            if self.min_count is None and self.max_count is None:
                raise ValueError("fixed row_count requires a bound")
            if (
                self.min_count is not None
                and self.max_count is not None
                and self.min_count > self.max_count
            ):
                raise ValueError("min_count exceeds max_count")
        elif self.min_count is not None or self.max_count is not None:
            raise ValueError("historical row_count cannot have fixed bounds")
        return self


class SchemaParams(Contract):
    fields: tuple[SchemaField, ...] | None = None
    allow_extra_fields: bool = False

    @model_validator(mode="after")
    def unique_fields(self):
        if self.fields is not None and len({f.name for f in self.fields}) != len(
            self.fields
        ):
            raise ValueError("duplicate schema field")
        return self


PARAMETERS = {
    CheckKind.FRESHNESS: FreshnessParams,
    CheckKind.NULL_RATE: NullRateParams,
    CheckKind.ROW_COUNT: RowCountParams,
    CheckKind.SCHEMA: SchemaParams,
}


class CheckSpec(Reactions):
    key: NonBlankText
    kind: CheckKind
    column: NonBlankText | None = None
    required: bool = True
    params: dict = Field(default_factory=dict)
    on_insufficient_history: QualityStatus = QualityStatus.WARN
    on_missing_schema: QualityStatus = QualityStatus.WARN
    implementation_version: Literal["1"] = "1"

    @model_validator(mode="after")
    def parameters(self):
        PARAMETERS[self.kind].model_validate_json(json.dumps(self.params))
        if (self.kind is CheckKind.NULL_RATE) != (self.column is not None):
            raise ValueError("only null_rate requires a column")
        if QualityStatus.PASS in (self.on_insufficient_history, self.on_missing_schema):
            raise ValueError("skips cannot map to PASS")
        return self


class CheckSuite(Contract):
    dataset_id: NonBlankText
    version: int = Field(default=1, ge=1)
    retry: RetrySettings = Field(default_factory=RetrySettings)
    checks: tuple[CheckSpec, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_keys(self):
        if len({c.key for c in self.checks}) != len(self.checks):
            raise ValueError("duplicate check key")
        return self

    @property
    def fingerprint(self) -> str:
        """Hash stable JSON: JSONB can reorder object keys, including params."""
        canonical = json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
