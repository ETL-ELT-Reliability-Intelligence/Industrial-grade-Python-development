"""Immutable, rule-independent feature snapshots and their calculation attempts."""

import hashlib
import json
from typing import Literal

from pydantic import AwareDatetime, Field, model_validator

from contracts.base import Contract, Count, NonBlankText


class FeatureColumn(Contract):
    name: NonBlankText
    kind: Literal["generic", "numeric", "categorical"] = "generic"
    categories: tuple[str, ...] = ()

    @model_validator(mode="after")
    def vocabulary(self):
        if len(set(self.categories)) != len(self.categories):
            raise ValueError("duplicate categories")
        if self.categories and self.kind != "categorical":
            raise ValueError("only categorical columns have a vocabulary")
        return self


class FeatureDefinition(Contract):
    columns: tuple[FeatureColumn, ...] = ()
    quantiles: tuple[float, ...] = (0.25, 0.5, 0.75)
    quantile_method: Literal["linear"] = "linear"
    ddof: Literal[0] = 0
    null_policy: Literal["missing_or_null"] = "missing_or_null"
    numeric_policy: Literal["finite_decimal_no_bool"] = "finite_decimal_no_bool"
    distinct_policy: Literal["typed_canonical_json_non_null"] = (
        "typed_canonical_json_non_null"
    )
    frequency_denominator: Literal["non_null"] = "non_null"

    @model_validator(mode="after")
    def unique_features(self):
        if len({c.name for c in self.columns}) != len(self.columns):
            raise ValueError("duplicate columns")
        if any(not 0 <= q <= 1 for q in self.quantiles):
            raise ValueError("quantiles must be finite and between zero and one")
        if tuple(sorted(set(self.quantiles))) != self.quantiles:
            raise ValueError("quantiles must be unique and sorted")
        if tuple(sorted(c.name for c in self.columns)) != tuple(
            c.name for c in self.columns
        ):
            raise ValueError("columns must be sorted by name")
        return self

    @property
    def fingerprint(self) -> str:
        payload = json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        )
        return hashlib.sha256(payload.encode()).hexdigest()


class ColumnFeatures(Contract):
    name: NonBlankText
    missing_column: bool
    null_count: Count
    non_null_count: Count
    distinct_count: Count
    null_rate: float | None = Field(default=None, ge=0, le=1, allow_inf_nan=False)
    distinct_ratio: float | None = Field(default=None, ge=0, le=1, allow_inf_nan=False)
    valid_count: Count | None = None
    parse_error_count: Count | None = None
    minimum: float | None = Field(default=None, allow_inf_nan=False)
    maximum: float | None = Field(default=None, allow_inf_nan=False)
    mean: float | None = Field(default=None, allow_inf_nan=False)
    stddev: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    quantiles: tuple[float, ...] | None = None
    category_counts: tuple[int, ...] | None = None
    category_frequencies: tuple[float, ...] | None = None
    other_count: Count | None = None
    other_frequency: float | None = Field(default=None, ge=0, le=1, allow_inf_nan=False)


class FeatureProfile(Contract):
    profile_id: NonBlankText
    dataset_id: NonBlankText
    batch_id: NonBlankText
    raw_sha256: NonBlankText
    source_id: NonBlankText
    received_at: AwareDatetime
    schema_version: int = Field(ge=1)
    granularity: Literal["batch", "hour", "month"]
    period_start: AwareDatetime | None
    period_end: AwareDatetime | None
    complete: bool
    raw_uri: NonBlankText
    definition: FeatureDefinition
    definition_hash: NonBlankText
    implementation_version: NonBlankText
    row_count: Count
    columns: tuple[ColumnFeatures, ...]

    @model_validator(mode="after")
    def definition_matches(self):
        if self.definition_hash != self.definition.fingerprint:
            raise ValueError("feature definition hash mismatch")
        if tuple(c.name for c in self.columns) != tuple(
            c.name for c in self.definition.columns
        ):
            raise ValueError("feature columns do not match definition")
        return self


class ProfilingAttempt(Contract):
    attempt_id: NonBlankText
    evaluation_id: NonBlankText
    definition_hash: NonBlankText
    implementation_version: NonBlankText
    started_at: AwareDatetime
    status: Literal["RUNNING", "COMPLETED", "ERROR", "CONFLICT"] = "RUNNING"
    finished_at: AwareDatetime | None = None
    profile_id: str | None = None
    error: str | None = None
