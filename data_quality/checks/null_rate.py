"""Null-rate evaluation on an already computed fraction."""

from contracts.quality import (
    CheckKind, CheckResult, CheckStatus, NullRateConstraint,
    NullRateObservation, QualityContext,
)


def check_null_rate(
    observation: NullRateObservation,
    constraint: NullRateConstraint,
    *,
    check_id: str,
    context: QualityContext | None = None,
) -> CheckResult:
    """Compare the observed null fraction with an inclusive maximum.

    An unavailable or undefined fraction yields UNKNOWN; zero is a measurement.
    """
    rate = observation.null_rate
    if rate is None:
        status, code, message = (
            CheckStatus.UNKNOWN, "missing_observation", "Null rate is unavailable.",
        )
    elif rate <= constraint.max_null_rate:
        status, code = CheckStatus.PASS, "requirement_met"
        message = f"Null rate {rate} is within the maximum {constraint.max_null_rate}."
    else:
        status, code = CheckStatus.FAIL, "threshold_exceeded"
        message = f"Null rate {rate} exceeds the maximum {constraint.max_null_rate}."
    return CheckResult(
        check_id=check_id, kind=CheckKind.NULL_RATE, status=status,
        observation=observation, constraint=constraint, reason_code=code,
        message=message, context=context,
    )
