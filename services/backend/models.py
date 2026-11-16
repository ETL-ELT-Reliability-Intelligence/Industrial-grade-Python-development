"""HTTP read models; shared contracts retain their upstream meaning."""

from typing import Generic, TypeVar, Literal

from pydantic import AwareDatetime, BaseModel

from contracts.evaluation import Evaluation, ExecutionStatus
from contracts.quality import CheckResult, QualityDecision, QualityStatus
from contracts.state import Anomaly

T = TypeVar("T")


class Page(BaseModel, Generic[T]):
    """Offset-based response envelope with a lookahead-derived has_more flag."""

    items: list[T]
    limit: int
    offset: int
    has_more: bool


class Dataset(BaseModel):
    """Catalog entry with its storage registration time."""

    dataset_id: str
    created_at: AwareDatetime


class Pipeline(BaseModel):
    pipeline_id: str


class QualityScope(BaseModel):
    """Scope identity with the latest time its quality evidence was saved."""

    scope_id: str
    last_saved_at: AwareDatetime


class StoredCheck(BaseModel):
    """Shared check result paired with its persistence timestamp."""

    result: CheckResult
    recorded_at: AwareDatetime


class ScopeQuality(BaseModel):
    """Stored scope evidence; an absent decision does not imply PASS."""

    dataset_id: str
    scope_id: str
    checks: list[StoredCheck]
    decision: QualityDecision | None
    decided_at: AwareDatetime | None


class EvaluationRecord(BaseModel):
    """Dataset and scope ownership with evaluation persistence timestamps."""

    dataset_id: str
    scope_id: str
    created_at: AwareDatetime
    updated_at: AwareDatetime


class EvaluationSummary(EvaluationRecord):
    """Compact evaluation state without execution payloads."""

    evaluation_id: str
    state: ExecutionStatus
    evaluated_at: AwareDatetime
    config_hash: str
    decision_status: QualityStatus | None


class EvaluationDetail(EvaluationRecord):
    """Full persisted evaluation and its storage metadata."""

    evaluation: Evaluation


class DetectorResult(BaseModel):
    """Saved evidence; SKIPPED never implies a normal measurement."""

    status: Literal["PASS", "WARN", "SKIPPED", "ERROR"]
    detector: str
    reason: str | None = None
    actual: float | None = None
    expected: float | None = None
    lower: float | None = None
    upper: float | None = None
    model_version: str | None = None
    checks: list[dict] = []
    anomalies: list[Anomaly] = []


class DetectionEvidence(BaseModel):
    dataset_id: str
    batch_id: str
    evaluation_id: str
    rule: DetectorResult
    ml: DetectorResult
    training: dict | None = None
