"""Stateless aggregation of check results into a quality decision."""

from collections.abc import Iterable

from contracts.quality import (
    CheckResult, CheckStatus, DecisionReason, PolicyRule, QualityDecision, QualityStatus,
)


def evaluate_quality(
    results: Iterable[CheckResult],
    rules: Iterable[PolicyRule] = (),
) -> QualityDecision:
    """Apply reactions and aggregate with BLOCK > WARN > PASS precedence.

    The caller supplies results for one evaluation scope and owns completeness.
    Unconfigured checks use PolicyRule defaults; unused rules are permitted.
    Duplicate check IDs in either input raise ValueError. An empty result set
    yields WARN. Reasons preserve input order and explain each applied reaction.
    This function never compares metrics again or executes the returned decision.
    """
    configured = {}
    for rule in rules:
        if not isinstance(rule, PolicyRule):
            raise ValueError("rules must contain PolicyRule objects")
        if rule.check_id in configured:
            raise ValueError(f"duplicate policy rule: {rule.check_id}")
        configured[rule.check_id] = rule

    priority = {QualityStatus.PASS: 0, QualityStatus.WARN: 1, QualityStatus.BLOCK: 2}
    decision = QualityStatus.PASS
    reasons = []
    seen = set()
    for result in results:
        if not isinstance(result, CheckResult):
            raise ValueError("results must contain CheckResult objects")
        if result.check_id in seen:
            raise ValueError(f"duplicate check result: {result.check_id}")
        seen.add(result.check_id)
        rule = configured.get(result.check_id)
        if rule is None:
            rule = PolicyRule(check_id=result.check_id)
        if result.status is CheckStatus.PASS:
            reaction = QualityStatus.PASS
        elif result.status is CheckStatus.FAIL:
            reaction = rule.on_fail
        else:
            reaction = rule.on_unknown
        if priority[reaction] > priority[decision]:
            decision = reaction
        reasons.append(DecisionReason(
            code=result.reason_code,
            message=f"{result.status.value} -> {reaction.value}: {result.message}",
            check_id=result.check_id,
        ))

    if not reasons:
        return QualityDecision(
            status=QualityStatus.WARN,
            reasons=(DecisionReason(code="no_checks", message="No check results were supplied."),),
        )
    return QualityDecision(status=decision, reasons=tuple(reasons))
