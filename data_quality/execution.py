"""Registry, evidence preparation and completeness-aware pure aggregation."""

from datetime import timedelta
import json
from statistics import median

from contracts.evaluation import CheckExecution, ExecutionStatus
from contracts.quality import (
    CheckKind,
    DecisionReason,
    FreshnessConstraint,
    FreshnessObservation,
    NullRateConstraint,
    NullRateObservation,
    PolicyRule,
    QualityContext,
    QualityDecision,
    QualityStatus,
    RowCountConstraint,
    RowCountObservation,
    SchemaConstraint,
    SchemaObservation,
)
from data_quality.checks import (
    check_freshness,
    check_null_rate,
    check_row_count,
    check_schema,
)
from contracts.quality_config import PARAMETERS
from data_quality.policies import evaluate_quality


def _freshness(params, stats, event, evaluated_at, schema, history):
    return (
        FreshnessObservation(
            last_updated_at=event.received_at, evaluated_at=evaluated_at
        ),
        FreshnessConstraint(max_age=timedelta(seconds=params.max_age_seconds)),
        None,
    )


def _null_rate(params, stats, event, evaluated_at, schema, history):
    return None, NullRateConstraint(max_null_rate=params.max_null_rate), None


def _rows(params, stats, event, evaluated_at, schema, history):
    observation = RowCountObservation(row_count=stats.row_count)
    if params.mode == "fixed":
        return (
            observation,
            RowCountConstraint(min_count=params.min_count, max_count=params.max_count),
            None,
        )
    values = [
        int(p.value)
        for p in history
        if p.value is not None and p.scope_id != event.batch_id
    ][: params.history_window]
    if len(values) < params.min_history:
        return observation, None, "insufficient_history"
    baseline = median(values)
    return (
        observation,
        RowCountConstraint(
            min_count=int(baseline * params.min_ratio),
            max_count=int(baseline * params.max_ratio),
        ),
        None,
    )


def _schema(params, stats, event, evaluated_at, schema, history):
    fields = params.fields if params.fields is not None else schema
    if fields is None:
        return SchemaObservation(fields=stats.schema), None, "missing_schema"
    return (
        SchemaObservation(fields=stats.schema),
        SchemaConstraint(fields=fields, allow_extra_fields=params.allow_extra_fields),
        None,
    )


# Each entry owns execution and preparation. No dispatch branches in the runner.
REGISTRY = {
    CheckKind.FRESHNESS: (check_freshness, _freshness),
    CheckKind.NULL_RATE: (check_null_rate, _null_rate),
    CheckKind.ROW_COUNT: (check_row_count, _rows),
    CheckKind.SCHEMA: (check_schema, _schema),
}


def prepare_check(check, stats, event, evaluated_at, schema, history):
    params = PARAMETERS[check.spec.kind].model_validate_json(
        json.dumps(check.spec.params)
    )
    _, prepare = REGISTRY[check.spec.kind]
    observation, constraint, code = prepare(
        params, stats, event, evaluated_at, schema, history
    )
    if check.spec.kind is CheckKind.NULL_RATE:
        observation = NullRateObservation(
            null_rate=stats.null_rates.get(check.spec.column)
        )
    return check.model_copy(
        update={
            "observation": observation,
            "constraint": constraint,
            "state": ExecutionStatus.SKIPPED if code else ExecutionStatus.PENDING,
            "code": code,
            "message": code.replace("_", " ") if code else None,
        }
    )


def execute_check(check, event):
    function, _ = REGISTRY[check.spec.kind]
    return function(
        check.observation,
        check.constraint,
        check_id=check.check_id,
        context=QualityContext(
            dataset_id=event.dataset_id,
            scope_id=event.batch_id,
            column=check.spec.column,
        ),
    )


def finalize(
    suite, scope_id: str, executions: tuple[CheckExecution, ...]
) -> QualityDecision:
    """Reject foreign/duplicate evidence; missing mandatory checks always block."""
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
            if code == "insufficient_history":
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
