"""Registry, evidence preparation and completeness-aware pure aggregation."""

import json
from functools import partial

from contracts.evaluation import CheckExecution, ExecutionStatus
from contracts.quality import (
    CheckDetail,
    CheckKind,
    DecisionReason,
    PolicyRule,
    QualityContext,
    QualityDecision,
    QualityStatus,
    SchemaConstraint,
    SchemaObservation,
)
from data_quality.checks import (
    check_null_rate,
    check_row_count,
    check_schema,
)
from contracts.quality_config import PARAMETERS
from data_quality.policies import evaluate_quality


from data_quality.checks.scalar import check_scalar
from data_quality.preparation import prepare

REGISTRY = {
    CheckKind.ROW_COUNT: check_row_count,
    CheckKind.NULL_RATE: check_null_rate,
    CheckKind.SCHEMA: check_schema,
    CheckKind.CARDINALITY: partial(check_scalar, kind=CheckKind.CARDINALITY),
    CheckKind.DISTRIBUTION: partial(check_scalar, kind=CheckKind.DISTRIBUTION),
    CheckKind.VALUE_RULE: partial(check_scalar, kind=CheckKind.VALUE_RULE),
}


def prepare_check(check, stats, event, evaluated_at, schema, history, profile):
    """Freeze observations, bounds and baseline provenance for one check.

    Return a copied execution marked SKIPPED when preparation supplies a skip
    reason, otherwise PENDING. The input execution is not mutated.
    """
    params = PARAMETERS[check.spec.kind].model_validate_json(
        json.dumps(check.spec.params)
    )
    if check.spec.kind is CheckKind.SCHEMA:
        fields = params.fields if params.fields is not None else schema
        observation = SchemaObservation(fields=stats.schema)
        constraint = (
            SchemaConstraint(
                fields=fields, allow_extra_fields=params.allow_extra_fields
            )
            if fields is not None
            else None
        )
        code = "missing_schema" if fields is None else None
        baseline = {}
    else:
        observation, constraint, code, baseline = prepare(
            check.spec, params, profile, history
        )
    return check.model_copy(
        update={
            "observation": observation,
            "constraint": constraint,
            "baseline": baseline,
            "state": ExecutionStatus.SKIPPED if code else ExecutionStatus.PENDING,
            "code": code,
            "message": code.replace("_", " ") if code else None,
        }
    )


def execute_check(check, event):
    """Evaluate frozen evidence and attach its scope and baseline provenance."""
    result = REGISTRY[check.spec.kind](
        check.observation,
        check.constraint,
        check_id=check.check_id,
        context=QualityContext(
            dataset_id=event.dataset_id,
            scope_id=event.batch_id,
            column=check.spec.column,
        ),
    )
    if check.baseline:
        result = result.model_copy(
            update={
                "details": result.details
                + (
                    CheckDetail(
                        code="historical_baseline",
                        expected=json.dumps(
                            check.baseline, ensure_ascii=False, sort_keys=True
                        ),
                    ),
                )
            }
        )
    return result


def finalize(
    suite, scope_id: str, executions: tuple[CheckExecution, ...]
) -> QualityDecision:
    """Aggregate completed, skipped and failed executions into a quality decision.

    Missing required checks block; missing optional checks warn. Recognized
    skips use the configured reactions. No checks or actions are executed.

    Raises:
        ValueError: Evidence is foreign, duplicated, inconsistent with the plan,
            or still pending.
    """
    expected = {f"{suite.dataset_id}.{c.key}": c for c in suite.checks}
    seen = {}
    for item in executions:
        if item.check_id not in expected or item.check_id in seen:
            raise ValueError("unexpected or duplicate execution")
        if item.spec != expected[item.check_id]:
            raise ValueError("execution configuration differs from plan")
        if item.result is not None:
            result = item.result
            context = result.context
            if context is None or (
                context.dataset_id,
                context.scope_id,
                context.column,
            ) != (suite.dataset_id, scope_id, item.spec.column):
                raise ValueError("result belongs to another scope or column")
            if (
                result.kind != item.spec.kind
                or result.observation != item.observation
                or result.constraint != item.constraint
            ):
                raise ValueError("result evidence differs from plan")
        if item.state in (ExecutionStatus.PENDING, ExecutionStatus.RUNNING):
            raise ValueError("evaluation is still pending")
        seen[item.check_id] = item
    results = [item.result for item in executions if item.result is not None]
    decisions = []
    if results:
        decisions.append(
            evaluate_quality(
                results,
                [
                    PolicyRule(check_id=key, on_fail=c.on_fail, on_unknown=c.on_unknown)
                    for key, c in expected.items()
                ],
            )
        )
    for key, spec in expected.items():
        item = seen.get(key)
        if item is not None and item.result is not None:
            continue
        code = (
            item.code
            if item
            else "missing_required_check"
            if spec.required
            else "missing_optional_check"
        )
        reaction = QualityStatus.BLOCK if spec.required else QualityStatus.WARN
        if item is not None and item.state is ExecutionStatus.SKIPPED:
            if code in {
                "insufficient_history",
                "insufficient_samples",
                "incomplete_window",
            }:
                reaction = spec.on_insufficient_history
            elif code == "missing_schema":
                reaction = spec.on_missing_schema
        decisions.append(
            QualityDecision(
                status=reaction,
                reasons=(
                    DecisionReason(
                        code=code or "execution_error",
                        check_id=key,
                        message=item.message
                        if item and item.message
                        else "Expected check did not produce a result.",
                    ),
                ),
            )
        )
    priority = {QualityStatus.PASS: 0, QualityStatus.WARN: 1, QualityStatus.BLOCK: 2}
    return QualityDecision(
        status=max((d.status for d in decisions), key=priority.get),
        reasons=tuple(r for d in decisions for r in d.reasons),
    )
