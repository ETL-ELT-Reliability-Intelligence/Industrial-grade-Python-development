"""Row-count evaluation for a caller-selected batch, partition, or window."""

from contracts.quality import (
    CheckKind, CheckResult, CheckStatus, QualityContext,
    RowCountConstraint, RowCountObservation,
)


def check_row_count(
    observation: RowCountObservation,
    constraint: RowCountConstraint,
    *,
    check_id: str,
    context: QualityContext | None = None,
) -> CheckResult:
    """Compare a measured count with inclusive, optionally one-sided bounds.

    Missing counts yield UNKNOWN. A measured zero is evaluated normally.
    """
    count = observation.row_count
    if count is None:
        status, code, message = (
            CheckStatus.UNKNOWN, "missing_observation", "Row count is unavailable.",
        )
    elif constraint.min_count is not None and count < constraint.min_count:
        status, code = CheckStatus.FAIL, "below_minimum"
        message = f"Row count {count} is below the minimum {constraint.min_count}."
    elif constraint.max_count is not None and count > constraint.max_count:
        status, code = CheckStatus.FAIL, "above_maximum"
        message = f"Row count {count} exceeds the maximum {constraint.max_count}."
    else:
        status, code = CheckStatus.PASS, "requirement_met"
        message = f"Row count {count} is within the configured bounds."
    return CheckResult(
        check_id=check_id, kind=CheckKind.ROW_COUNT, status=status,
        observation=observation, constraint=constraint, reason_code=code,
        message=message, context=context,
    )
