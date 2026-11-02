"""Valid evaluation snapshots for API, PostgreSQL and browser scenarios."""

from datetime import timedelta

from contracts.evaluation import Attempt, CheckExecution, Evaluation, ExecutionStatus
from contracts.events import DatasetIngestedEvent
from contracts.quality import CheckKind, DecisionReason, QualityDecision, QualityStatus
from contracts.quality_config import CheckSpec, CheckSuite
from services.backend.models import EvaluationDetail
from tests.application.support import NOW, evidence


def evaluation_record(
    evaluation_id="evaluation-1",
    dataset_id="orders",
    scope_id="window-1",
    *,
    completed=False,
):
    spec = CheckSpec(
        key="volume",
        kind=CheckKind.ROW_COUNT,
        params={"mode": "fixed", "min_count": 100},
    )
    suite = CheckSuite(dataset_id=dataset_id, checks=(spec,))
    check_id = f"{dataset_id}.volume"
    result = evidence(dataset_id, scope_id)[0].model_copy(update={"check_id": check_id})
    execution = CheckExecution(
        check_id=check_id,
        spec=spec,
        state=ExecutionStatus.COMPLETED if completed else ExecutionStatus.PENDING,
        observation=result.observation,
        constraint=result.constraint,
        result=result if completed else None,
        attempts=(
            Attempt(number=1, started_at=NOW, finished_at=NOW + timedelta(seconds=1)),
        )
        if completed
        else (),
        baseline={"batch_ids": ["historical-batch"], "median": 120},
    )
    evaluation = Evaluation(
        evaluation_id=evaluation_id,
        event=DatasetIngestedEvent(
            dataset_id=dataset_id,
            batch_id=scope_id,
            records=0,
            received_at=NOW,
            schema_version=1,
        ),
        suite=suite,
        config_hash=suite.fingerprint,
        evaluated_at=NOW,
        state=ExecutionStatus.COMPLETED if completed else ExecutionStatus.PENDING,
        prepared=True,
        checks=(execution,),
        decision=QualityDecision(
            status=QualityStatus.BLOCK,
            reasons=(
                DecisionReason(
                    code="row_count_below_minimum",
                    message="Too few rows",
                    check_id=check_id,
                ),
            ),
        )
        if completed
        else None,
    )
    return EvaluationDetail(
        dataset_id=dataset_id,
        scope_id=scope_id,
        created_at=NOW,
        updated_at=NOW,
        evaluation=evaluation,
    )
