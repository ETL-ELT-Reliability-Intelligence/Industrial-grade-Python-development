"""Соответствие реакции _check_reaction и итогового статуса finalize."""

import pytest

from contracts.evaluation import CheckExecution, ExecutionStatus
from contracts.quality import (
    CheckKind,
    CheckResult,
    CheckStatus,
    QualityContext,
    QualityStatus,
    RowCountConstraint,
    RowCountObservation,
)
from contracts.quality_config import CheckSpec, CheckSuite
from data_quality.execution import finalize
from services.stream_processor.handlers import StreamEventHandler

SCOPE_ID = "b1"
CHECK_KEY = "rows"
CHECK_ID = f"orders.{CHECK_KEY}"


def make_spec(**overrides) -> CheckSpec:
    """Проверка row_count с фиксированной границей и заданными реакциями."""
    return CheckSpec(
        key=CHECK_KEY,
        kind=CheckKind.ROW_COUNT,
        params={"mode": "fixed", "min_count": 1},
        **overrides,
    )


def make_execution(spec, *, status=None, state=None, code=None) -> CheckExecution:
    """Исполнение с результатом (по status) либо без него (по state и code)."""
    if status is None:
        return CheckExecution(
            check_id=CHECK_ID,
            spec=spec,
            state=state,
            code=code,
            message=code.replace("_", " ") if code else None,
        )
    observation = RowCountObservation(row_count=5)
    constraint = RowCountConstraint(min_count=1)
    result = CheckResult(
        check_id=CHECK_ID,
        kind=CheckKind.ROW_COUNT,
        status=status,
        observation=observation,
        constraint=constraint,
        reason_code=status.value.lower(),
        message=f"check finished with {status.value}",
        context=QualityContext(dataset_id="orders", scope_id=SCOPE_ID),
    )
    return CheckExecution(
        check_id=CHECK_ID,
        spec=spec,
        state=ExecutionStatus.COMPLETED,
        observation=observation,
        constraint=constraint,
        result=result,
    )


@pytest.mark.parametrize(
    "spec_overrides, status, state, code, expected",
    [
        # Результат есть: PASS игнорирует on_fail, FAIL и UNKNOWN дают свои реакции.
        ({}, CheckStatus.PASS, ExecutionStatus.COMPLETED, None, QualityStatus.PASS),
        (
            {"on_fail": QualityStatus.BLOCK},
            CheckStatus.FAIL,
            ExecutionStatus.COMPLETED,
            None,
            QualityStatus.BLOCK,
        ),
        (
            {"on_fail": QualityStatus.WARN},
            CheckStatus.FAIL,
            ExecutionStatus.COMPLETED,
            None,
            QualityStatus.WARN,
        ),
        (
            {"on_unknown": QualityStatus.BLOCK},
            CheckStatus.UNKNOWN,
            ExecutionStatus.COMPLETED,
            None,
            QualityStatus.BLOCK,
        ),
        (
            {"on_unknown": QualityStatus.WARN},
            CheckStatus.UNKNOWN,
            ExecutionStatus.COMPLETED,
            None,
            QualityStatus.WARN,
        ),
        # Пропуск из-за нехватки истории управляется on_insufficient_history.
        (
            {"on_insufficient_history": QualityStatus.WARN},
            None,
            ExecutionStatus.SKIPPED,
            "insufficient_history",
            QualityStatus.WARN,
        ),
        (
            {"on_insufficient_history": QualityStatus.BLOCK},
            None,
            ExecutionStatus.SKIPPED,
            "insufficient_history",
            QualityStatus.BLOCK,
        ),
        # Нехватка выборки и неполное окно используют ту же реакцию.
        (
            {"on_insufficient_history": QualityStatus.WARN},
            None,
            ExecutionStatus.SKIPPED,
            "insufficient_samples",
            QualityStatus.WARN,
        ),
        (
            {"on_insufficient_history": QualityStatus.BLOCK},
            None,
            ExecutionStatus.SKIPPED,
            "insufficient_samples",
            QualityStatus.BLOCK,
        ),
        (
            {"on_insufficient_history": QualityStatus.WARN},
            None,
            ExecutionStatus.SKIPPED,
            "incomplete_window",
            QualityStatus.WARN,
        ),
        (
            {"on_insufficient_history": QualityStatus.BLOCK},
            None,
            ExecutionStatus.SKIPPED,
            "incomplete_window",
            QualityStatus.BLOCK,
        ),
        # Пропуск необязательной проверки без отдельной реакции даёт WARN.
        (
            {"required": False},
            None,
            ExecutionStatus.SKIPPED,
            None,
            QualityStatus.WARN,
        ),
        # Отсутствие схемы управляется on_missing_schema.
        (
            {"on_missing_schema": QualityStatus.WARN},
            None,
            ExecutionStatus.SKIPPED,
            "missing_schema",
            QualityStatus.WARN,
        ),
        (
            {"on_missing_schema": QualityStatus.BLOCK},
            None,
            ExecutionStatus.SKIPPED,
            "missing_schema",
            QualityStatus.BLOCK,
        ),
        # Ошибка подготовки не имеет своей реакции: зависят только required.
        (
            {"required": True},
            None,
            ExecutionStatus.ERROR,
            "preparation_error",
            QualityStatus.BLOCK,
        ),
        (
            {"required": False},
            None,
            ExecutionStatus.ERROR,
            "preparation_error",
            QualityStatus.WARN,
        ),
    ],
    ids=[
        "pass",
        "fail-blocks",
        "fail-warns",
        "unknown-blocks",
        "unknown-warns",
        "skipped-history-warns",
        "skipped-history-blocks",
        "skipped-samples-warns",
        "skipped-samples-blocks",
        "skipped-window-warns",
        "skipped-window-blocks",
        "skipped-optional-warns",
        "skipped-schema-warns",
        "skipped-schema-blocks",
        "error-required-blocks",
        "error-optional-warns",
    ],
)
def test_check_reaction_matches_finalize(
    spec_overrides, status, state, code, expected
):
    spec = make_spec(**spec_overrides)
    execution = make_execution(spec, status=status, state=state, code=code)

    assert StreamEventHandler._check_reaction(None, execution) is expected

    suite = CheckSuite(dataset_id="orders", checks=(spec,))
    assert finalize(suite, SCOPE_ID, (execution,)).status is expected
