"""Select distinct, earlier, compatible periods before taking a history window."""


def select_history(current, candidates):
    """Return distinct compatible earlier periods, newest first.

    Exclude observations received after the current batch and periods crossing
    its cutoff. The latest delivery of a period supersedes older deliveries
    even when ineligible. Match source, granularity, schema, statistics and
    configuration versions. The caller applies the history window limit.
    """
    event = current.event
    cutoff = event.period_start or event.received_at
    # Latest delivery of a period supersedes earlier deliveries, even if rejected.
    candidates = sorted(
        candidates, key=lambda p: (p.event.received_at, p.event.batch_id), reverse=True
    )
    seen = set()
    selected = []
    for profile in candidates:
        previous = profile.event
        start = previous.period_start or previous.received_at
        if (
            previous.batch_id == event.batch_id
            or start >= cutoff
            or previous.received_at > event.received_at
        ):
            continue
        if (previous.source_id, previous.granularity) != (
            event.source_id,
            event.granularity,
        ):
            continue
        if previous.period_end is not None and previous.period_end > cutoff:
            continue
        period = (
            (previous.period_start, previous.period_end)
            if previous.period_start
            else previous.batch_id
        )
        if period in seen:
            continue
        seen.add(period)
        if not profile.eligible or not previous.complete:
            continue
        if (previous.schema_version, profile.stats_version, profile.config_hash) != (
            event.schema_version,
            current.stats_version,
            current.config_hash,
        ):
            continue
        selected.append(profile)
    return sorted(
        selected,
        key=lambda p: p.event.period_start or p.event.received_at,
        reverse=True,
    )


def eligible(evaluation):
    """Bootstrap WARN is allowed; failed, unknown and errored checks are not."""
    return bool(evaluation.event.complete and evaluation.checks) and all(
        (check.result is not None and check.result.status.value == "PASS")
        or (
            check.state.value == "SKIPPED"
            and check.code in {"insufficient_history", "missing_schema"}
        )
        for check in evaluation.checks
    )
