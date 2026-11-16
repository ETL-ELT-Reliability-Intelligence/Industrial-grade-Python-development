"""Persisted evaluation snapshots; check compliance and execution are independent."""

from enum import Enum

from pydantic import AwareDatetime, Field, model_validator

from contracts.base import Contract, NonBlankText
from contracts.events import DatasetIngestedEvent
from contracts.quality import CheckResult, Constraint, Observation, QualityDecision
from contracts.quality_config import CheckSpec, CheckSuite


class ExecutionStatus(str, Enum):
    """Execution progress, independent of the quality compliance outcome."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    SKIPPED = "SKIPPED"
    ERROR = "ERROR"


class Attempt(Contract):
    """One numbered execution attempt, with optional completion and error details."""

    number: int = Field(ge=1)
    started_at: AwareDatetime
    finished_at: AwareDatetime | None = None
    code: str | None = None
    message: str | None = None


class CheckExecution(Contract):
    """Frozen check inputs and retry history; only completed checks carry results."""

    check_id: NonBlankText
    spec: CheckSpec
    state: ExecutionStatus = ExecutionStatus.PENDING
    observation: Observation | None = None
    constraint: Constraint | None = None
    result: CheckResult | None = None
    code: str | None = None
    message: str | None = None
    attempts: tuple[Attempt, ...] = ()
    baseline: dict = Field(default_factory=dict)

    @model_validator(mode="after")
    def result_state(self):
        if (self.state is ExecutionStatus.COMPLETED) != (self.result is not None):
            raise ValueError("only completed checks have a result")
        if self.result is not None and self.result.check_id != self.check_id:
            raise ValueError("result belongs to another check")
        return self


class Evaluation(Contract):
    """Persisted plan, execution state and delivery history for one batch evaluation."""

    evaluation_id: NonBlankText
    event: DatasetIngestedEvent
    suite: CheckSuite
    config_hash: NonBlankText
    evaluated_at: AwareDatetime
    state: ExecutionStatus = ExecutionStatus.PENDING
    checks: tuple[CheckExecution, ...]
    preparation_attempts: tuple[Attempt, ...] = ()
    delivery_attempts: tuple[Attempt, ...] = ()
    prepared: bool = False
    raw_sha256: str | None = None
    decision: QualityDecision | None = None
    published: bool = False

    @model_validator(mode="after")
    def identity(self):
        if (
            self.event.dataset_id != self.suite.dataset_id
            or self.config_hash != self.suite.fingerprint
        ):
            raise ValueError("evaluation configuration identity mismatch")
        expected = {f"{self.suite.dataset_id}.{c.key}": c for c in self.suite.checks}
        actual = {c.check_id: c.spec for c in self.checks}
        if len(actual) != len(self.checks) or actual != expected:
            raise ValueError("execution records do not match the plan")
        if (self.state is ExecutionStatus.COMPLETED) != (self.decision is not None):
            raise ValueError("only final evaluations have a decision")
        return self
