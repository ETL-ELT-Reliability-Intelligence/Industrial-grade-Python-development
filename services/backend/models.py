"""HTTP read models; shared contracts retain their upstream meaning."""

from typing import Generic, TypeVar

from pydantic import AwareDatetime, BaseModel

from contracts.quality import CheckResult, QualityDecision

T = TypeVar("T")


class Page(BaseModel, Generic[T]):
    items: list[T]
    limit: int
    offset: int
    has_more: bool


class Dataset(BaseModel):
    dataset_id: str
    created_at: AwareDatetime


class QualityScope(BaseModel):
    scope_id: str
    last_saved_at: AwareDatetime


class StoredCheck(BaseModel):
    result: CheckResult
    recorded_at: AwareDatetime


class ScopeQuality(BaseModel):
    dataset_id: str
    scope_id: str
    checks: list[StoredCheck]
    decision: QualityDecision | None
    decided_at: AwareDatetime | None
