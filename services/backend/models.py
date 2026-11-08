"""HTTP read models; shared contracts retain their upstream meaning."""

from typing import Generic, TypeVar

from pydantic import AwareDatetime, BaseModel

from contracts.evaluation import Evaluation, ExecutionStatus
from contracts.quality import CheckResult, QualityDecision, QualityStatus

T = TypeVar("T")


class Page(BaseModel, Generic[T]):
    items: list[T]
    limit: int
    offset: int
    has_more: bool


class Dataset(BaseModel):
    dataset_id: str
    created_at: AwareDatetime


class Pipeline(BaseModel):
    pipeline_id: str


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


class EvaluationRecord(BaseModel):
    dataset_id: str
    scope_id: str
    created_at: AwareDatetime
    updated_at: AwareDatetime


class EvaluationSummary(EvaluationRecord):
    evaluation_id: str
    state: ExecutionStatus
    evaluated_at: AwareDatetime
    config_hash: str
    decision_status: QualityStatus | None


class EvaluationDetail(EvaluationRecord):
    evaluation: Evaluation
