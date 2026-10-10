"""Exact comparison of flat schemas normalized by the caller."""

from contracts.quality import (
    CheckDetail, CheckKind, CheckResult, CheckStatus, QualityContext,
    SchemaConstraint, SchemaObservation,
)


def check_schema(
    observation: SchemaObservation,
    constraint: SchemaConstraint,
    *,
    check_id: str,
    context: QualityContext | None = None,
) -> CheckResult:
    """Compare field names and types, ignoring order and reporting all mismatches.

    Details are sorted by field name. Extra fields fail unless explicitly allowed;
    missing fields and type mismatches fail regardless of that setting.
    """
    details = []
    if observation.fields is None:
        status, code, message = (
            CheckStatus.UNKNOWN, "missing_observation", "Schema is unavailable.",
        )
    else:
        actual = {field.name: field.data_type for field in observation.fields}
        expected = {field.name: field.data_type for field in constraint.fields}
        for name in sorted(actual.keys() | expected.keys()):
            if name not in actual:
                details.append(CheckDetail(code="missing_field", field=name, expected=expected[name]))
            elif name not in expected:
                if not constraint.allow_extra_fields:
                    details.append(CheckDetail(code="unexpected_field", field=name, actual=actual[name]))
            elif actual[name] != expected[name]:
                details.append(CheckDetail(code="type_mismatch", field=name, expected=expected[name], actual=actual[name]))
        if details:
            status, code = CheckStatus.FAIL, "schema_mismatch"
            message = f"Schema has {len(details)} field mismatch(es)."
        else:
            status, code = CheckStatus.PASS, "requirement_met"
            message = "Schema satisfies the expected fields and types."
    return CheckResult(
        check_id=check_id, kind=CheckKind.SCHEMA, status=status,
        observation=observation, constraint=constraint, reason_code=code,
        message=message, context=context, details=tuple(details),
    )
