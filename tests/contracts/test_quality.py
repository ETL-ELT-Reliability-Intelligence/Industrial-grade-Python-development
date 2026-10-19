"""Validation at the boundary between metric producers and quality checks."""

from pydantic import ValidationError
from datetime import datetime, timedelta, timezone
import unittest

import pytest

from contracts.base import Contract
from contracts.quality import (
    CheckDetail, CheckKind, CheckResult, CheckStatus, DecisionReason,
    FreshnessConstraint, FreshnessObservation, NullRateConstraint,
    NullRateObservation, PolicyRule, QualityContext, QualityDecision,
    QualityStatus, RowCountConstraint, RowCountObservation, SchemaConstraint,
    SchemaField, SchemaObservation,
)


def validated_replace(model, **changes):
    """Revalidate changes; model_copy(update=...) intentionally skips validation."""
    return type(model).model_validate({**dict(model), **changes})


@pytest.mark.parametrize('model,values', [
    (QualityContext, dict(dataset_id='orders', scope_id='batch')),
    (FreshnessObservation, dict(last_updated_at=None, evaluated_at=datetime.now(timezone.utc))),
    (FreshnessConstraint, dict(max_age=timedelta(0))),
    (NullRateObservation, dict(null_rate=None)),
    (NullRateConstraint, dict(max_null_rate=0)),
    (RowCountObservation, dict(row_count=0)),
    (RowCountConstraint, dict(min_count=0)),
    (SchemaField, dict(name='id', data_type='integer')),
    (SchemaObservation, dict(fields=())),
    (SchemaConstraint, dict(fields=())),
    (CheckDetail, dict(code='missing_field')),
    (PolicyRule, dict(check_id='rows')),
    (DecisionReason, dict(code='no_checks', message='No checks.')),
    (QualityDecision, dict(status=QualityStatus.WARN,
                          reasons=(DecisionReason(code='no_checks', message='No checks.'),))),
])
def test_quality_model_configuration_and_roundtrip(model, values):
    assert issubclass(model, Contract)
    item = model(**values)
    assert model.model_validate_json(item.model_dump_json()) == item
    with pytest.raises(ValidationError):
        model(**values, unexpected=True)
    field = next(iter(values))
    with pytest.raises(ValidationError):
        setattr(item, field, values[field])


def test_defaults_are_validated():
    class InvalidDefault(NullRateConstraint):
        max_null_rate: float = '0.5'

    with pytest.raises(ValidationError):
        InvalidDefault()


class ObservationTests(unittest.TestCase):
    """Verify observation and constraint validation at the input boundary."""

    def test_missing_observations_are_distinct_from_zero_or_empty(self):
        """Keep unavailable measurements distinct from valid zero or empty values."""
        for missing, measured in (
            (NullRateObservation(null_rate=None), NullRateObservation(null_rate=0)),
            (RowCountObservation(row_count=None), RowCountObservation(row_count=0)),
            (SchemaObservation(fields=None), SchemaObservation(fields=())),
        ):
            with self.subTest(missing=missing):
                self.assertNotEqual(missing, measured)

    def test_rate_boundaries_and_invalid_values(self):
        """Accept inclusive fraction boundaries and reject invalid numeric inputs."""
        for model in (NullRateObservation, NullRateConstraint):
            for value in (0, 0.5, 1):
                model(**{next(iter(model.model_fields)): value})
            for value in (-0.1, 1.1, float('nan'), float('inf'), True, '0.5'):
                with self.subTest(model=model, value=value):
                    with self.assertRaises(ValueError):
                        model(**{next(iter(model.model_fields)): value})
        with self.assertRaises(ValueError):
            NullRateConstraint(max_null_rate=None)

    def test_counts_and_bounds(self):
        """Validate integer counts and consistent, optionally one-sided bounds."""
        for value in (-1, 1.5, True, '10'):
            for model in (RowCountObservation, RowCountConstraint):
                with self.subTest(model=model, value=value):
                    with self.assertRaises(ValueError):
                        model(**{next(iter(model.model_fields)): value})
        for kwargs in ({}, {'min_count': 2, 'max_count': 1}, {'max_count': -1}):
            with self.assertRaises(ValueError):
                RowCountConstraint(**kwargs)
        RowCountConstraint(min_count=0)
        RowCountConstraint(max_count=0)
        RowCountConstraint(min_count=10, max_count=10)

    def test_freshness_requires_explicit_aware_time(self):
        """Validate timestamp awareness, chronological order, and maximum age."""
        now = datetime(2026, 10, 4, tzinfo=timezone.utc)
        FreshnessObservation(last_updated_at=None, evaluated_at=now)
        FreshnessObservation(last_updated_at=now, evaluated_at=now)
        # Different offsets representing the same instant are valid.
        FreshnessObservation(last_updated_at=now.astimezone(timezone(timedelta(hours=3))), evaluated_at=now)
        for updated, evaluated in (
            (now.replace(tzinfo=None), now),
            (None, now.replace(tzinfo=None)),
            (now + timedelta(seconds=1), now),
            (None, None),
        ):
            with self.subTest(updated=updated, evaluated=evaluated):
                with self.assertRaises(ValueError):
                    FreshnessObservation(last_updated_at=updated, evaluated_at=evaluated)
        FreshnessConstraint(max_age=timedelta(0))
        for value in (timedelta(seconds=-1), 60, None):
            with self.assertRaises(ValueError):
                FreshnessConstraint(max_age=value)

    def test_schema_rejects_duplicate_or_untyped_fields(self):
        """Require typed, uniquely named fields and a boolean extra-field flag."""
        field = SchemaField(name='id', data_type='integer')
        for model in (SchemaObservation, SchemaConstraint):
            model(fields=())
            model(fields=(field,))
            for fields in ((field, field), [field], ('id',)):
                with self.subTest(model=model, fields=fields):
                    with self.assertRaises(ValueError):
                        model(fields=fields)
        for name, data_type in (('', 'integer'), ('id', ' ')):
            with self.assertRaises(ValueError):
                SchemaField(name=name, data_type=data_type)
        with self.assertRaises(ValueError):
            SchemaConstraint(fields=(), allow_extra_fields='false')


class ResultTests(unittest.TestCase):
    """Verify result evidence, metadata, and contract compatibility."""

    def setUp(self):
        """Create a valid null-rate result for each test to inspect or vary."""
        self.result = CheckResult(
            check_id='orders.id.null_rate',
            kind=CheckKind.NULL_RATE,
            status=CheckStatus.FAIL,
            observation=NullRateObservation(null_rate=0.2),
            constraint=NullRateConstraint(max_null_rate=0.1),
            reason_code='threshold_exceeded',
            message='Null rate exceeds the configured maximum.',
            context=QualityContext(dataset_id='orders', scope_id='batch-42', column='id'),
        )

    def test_result_preserves_evidence_and_is_immutable(self):
        """Preserve supplied evidence and prevent result reassignment."""
        self.assertEqual(self.result.observation.null_rate, 0.2)
        self.assertEqual(self.result.constraint.max_null_rate, 0.1)
        self.assertEqual(self.result.context.scope_id, 'batch-42')
        self.assertEqual(CheckResult.model_validate_json(self.result.model_dump_json()), self.result)
        with self.assertRaises(ValidationError):
            validated_replace(self.result, unexpected=True)
        with self.assertRaises(ValidationError):
            self.result.status = CheckStatus.PASS

    def test_result_rejects_mixed_contracts(self):
        """Reject mismatched contract types and malformed result metadata."""
        for changes in (
            {'observation': RowCountObservation(row_count=10)},
            {'constraint': RowCountConstraint(min_count=1)},
            {'kind': 'null_rate'},
            {'status': QualityStatus.BLOCK},
            {'status': 'FAIL'},
            {'check_id': ' '},
            {'reason_code': ''},
            {'context': {}},
            {'details': [{}]},
        ):
            with self.subTest(changes=changes):
                with self.assertRaises(ValueError):
                    validated_replace(self.result, **changes)

    def test_all_check_kinds_have_matching_contracts(self):
        """Accept the matching observation and constraint for every check kind."""
        now = datetime(2026, 10, 4, tzinfo=timezone.utc)
        for kind, observation, constraint in (
            (CheckKind.FRESHNESS, FreshnessObservation(last_updated_at=None, evaluated_at=now),
             FreshnessConstraint(max_age=timedelta(hours=1))),
            (CheckKind.NULL_RATE, NullRateObservation(null_rate=None), NullRateConstraint(max_null_rate=0)),
            (CheckKind.ROW_COUNT, RowCountObservation(row_count=None), RowCountConstraint(min_count=1)),
            (CheckKind.SCHEMA, SchemaObservation(fields=None), SchemaConstraint(fields=())),
        ):
            with self.subTest(kind=kind):
                validated_replace(self.result, kind=kind, observation=observation,
                        constraint=constraint, status=CheckStatus.UNKNOWN,
                        reason_code='missing_observation', message='Observation unavailable.')

    def test_structured_schema_details(self):
        """Preserve field-level schema diagnostics in structured result details."""
        detail = CheckDetail(code='type_mismatch', field='id', expected='integer', actual='string')
        result = validated_replace(
            self.result, kind=CheckKind.SCHEMA,
            observation=SchemaObservation(fields=(SchemaField(name='id', data_type='string'),)),
            constraint=SchemaConstraint(fields=(SchemaField(name='id', data_type='integer'),)),
            reason_code='schema_mismatch', details=(detail,),
        )
        self.assertEqual(result.details[0].expected, 'integer')

    def test_context_identifiers_are_required_when_context_is_provided(self):
        """Reject blank dataset, scope, or supplied column identifiers."""
        for args in (('', 'batch'), ('orders', ''), ('orders', 'batch', ' ')):
            with self.assertRaises(ValueError):
                QualityContext(**dict(zip(('dataset_id', 'scope_id', 'column'), args)))


class PolicyContractTests(unittest.TestCase):
    """Verify reaction settings and policy decision contracts."""

    def test_default_and_overridden_reactions(self):
        """Accept WARN/BLOCK overrides while rejecting PASS and non-policy enums."""
        rule = PolicyRule(check_id='orders.id.null_rate')
        self.assertIs(rule.on_fail, QualityStatus.BLOCK)
        self.assertIs(rule.on_unknown, QualityStatus.WARN)
        PolicyRule(check_id=rule.check_id, on_fail=QualityStatus.WARN, on_unknown=QualityStatus.BLOCK)
        for field in ('on_fail', 'on_unknown'):
            for value in (QualityStatus.PASS, 'WARN', CheckStatus.FAIL):
                with self.assertRaises(ValueError):
                    validated_replace(rule, **{field: value})

    def test_decision_requires_policy_status_and_explanation(self):
        """Require a policy enum and a non-empty tuple of structured reasons."""
        reason = DecisionReason(code='no_checks', message='No checks were supplied.')
        decision = QualityDecision(status=QualityStatus.WARN, reasons=(reason,))
        self.assertIsNone(decision.reasons[0].check_id)
        for status, reasons in (
            (CheckStatus.PASS, (reason,)), (QualityStatus.PASS, ()),
            (QualityStatus.WARN, [reason]), (QualityStatus.WARN, ('no_checks',)),
        ):
            with self.assertRaises(ValueError):
                QualityDecision(status=status, reasons=reasons)


if __name__ == '__main__':
    unittest.main()
