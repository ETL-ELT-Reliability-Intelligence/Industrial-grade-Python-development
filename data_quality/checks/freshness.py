"""Freshness evaluation using only caller-supplied timestamps."""

from datetime import timezone

from contracts.quality import (
    CheckKind, CheckResult, CheckStatus, FreshnessConstraint,
    FreshnessObservation, QualityContext,
)


def check_freshness(
    observation: FreshnessObservation,
    constraint: FreshnessConstraint,
    *,
    check_id: str,
    context: QualityContext | None = None,
) -> CheckResult:
    """Compare elapsed age with an inclusive maximum without reading a clock.

    Missing update time yields UNKNOWN. UTC arithmetic measures elapsed time
    even when the timestamps share a timezone with daylight-saving transitions.
    """
    if observation.last_updated_at is None:
        status, code, message = (
            CheckStatus.UNKNOWN, "missing_observation", "Last update time is unavailable.",
        )
    else:
        age = (observation.evaluated_at.astimezone(timezone.utc)
               - observation.last_updated_at.astimezone(timezone.utc))
        if age <= constraint.max_age:
            status, code = CheckStatus.PASS, "requirement_met"
            message = f"Data age {age} is within the maximum {constraint.max_age}."
        else:
            status, code = CheckStatus.FAIL, "threshold_exceeded"
            message = f"Data age {age} exceeds the maximum {constraint.max_age}."
    return CheckResult(
        check_id=check_id, kind=CheckKind.FRESHNESS, status=status,
        observation=observation, constraint=constraint, reason_code=code,
        message=message, context=context,
    )
