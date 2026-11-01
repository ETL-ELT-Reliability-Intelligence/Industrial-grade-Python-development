"""Checks of derived scores against persisted, inclusive bounds."""

from contracts.quality import CheckResult, CheckStatus


def check_scalar(observation, constraint, *, kind, check_id, context=None):
    value = observation.value
    if value is None:
        status, code = CheckStatus.UNKNOWN, "missing_observation"
    elif constraint.minimum is not None and value < constraint.minimum:
        status, code = CheckStatus.FAIL, "below_minimum"
    elif constraint.maximum is not None and value > constraint.maximum:
        status, code = CheckStatus.FAIL, "above_maximum"
    else:
        status, code = CheckStatus.PASS, "requirement_met"
    return CheckResult(
        check_id=check_id,
        kind=kind,
        status=status,
        observation=observation,
        constraint=constraint,
        context=context,
        reason_code=code,
        message=f"Observed {value}; expected [{constraint.minimum}, {constraint.maximum}].",
    )
