"""Validation at the boundary between metric producers and quality checks."""

from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
import unittest

from contracts.quality import (
    CheckDetail, CheckKind, CheckResult, CheckStatus, DecisionReason,
    FreshnessConstraint, FreshnessObservation, NullRateConstraint,
    NullRateObservation, PolicyRule, QualityContext, QualityDecision,
    QualityStatus, RowCountConstraint, RowCountObservation, SchemaConstraint,
    SchemaField, SchemaObservation,
)


class ObservationTests(unittest.TestCase):
    """Verify observation and constraint validation at the input boundary."""

    def test_missing_observations_are_distinct_from_zero_or_empty(self):
        """Keep unavailable measurements distinct from valid zero or empty values."""
        for missing, measured in (
            (NullRateObservation(None), NullRateObservation(0)),
            (RowCountObservation(None), RowCountObservation(0)),
            (SchemaObservation(None), SchemaObservation(())),
        ):
            with self.subTest(missing=missing):
                self.assertNotEqual(missing, measured)

    def test_rate_boundaries_and_invalid_values(self):
        """Accept inclusive fraction boundaries and reject invalid numeric inputs."""
        for model in (NullRateObservation, NullRateConstraint):
            for value in (0, 0.5, 1):
                model(value)
            for value in (-0.1, 1.1, float('nan'), float('inf'), True, '0.5'):
                with self.subTest(model=model, value=value):
                    with self.assertRaises(ValueError):
                        model(value)
        with self.assertRaises(ValueError):
            NullRateConstraint(None)

    def test_counts_and_bounds(self):
        """Validate integer counts and consistent, optionally one-sided bounds."""
        for value in (-1, 1.5, True, '10'):
            for model in (RowCountObservation, RowCountConstraint):
                with self.subTest(model=model, value=value):
                    with self.assertRaises(ValueError):
                        model(value)
        for kwargs in ({}, {'min_count': 2, 'max_count': 1}, {'max_count': -1}):
            with self.assertRaises(ValueError):
                RowCountConstraint(**kwargs)
        RowCountConstraint(min_count=0)
        RowCountConstraint(max_count=0)
        RowCountConstraint(min_count=10, max_count=10)

    def test_freshness_requires_explicit_aware_time(self):
        """Validate timestamp awareness, chronological order, and maximum age."""
        now = datetime(2026, 10, 4, tzinfo=timezone.utc)
        FreshnessObservation(None, now)
        FreshnessObservation(now, now)
        # Different offsets representing the same instant are valid.
        FreshnessObservation(now.astimezone(timezone(timedelta(hours=3))), now)
        for updated, evaluated in (
            (now.replace(tzinfo=None), now),
            (None, now.replace(tzinfo=None)),
            (now + timedelta(seconds=1), now),
            (None, None),
        ):
            with self.subTest(updated=updated, evaluated=evaluated):
                with self.assertRaises(ValueError):
                    FreshnessObservation(updated, evaluated)
        FreshnessConstraint(timedelta(0))
        for value in (timedelta(seconds=-1), 60, None):
            with self.assertRaises(ValueError):
                FreshnessConstraint(value)

    def test_schema_rejects_duplicate_or_untyped_fields(self):
        """Require typed, uniquely named fields and a boolean extra-field flag."""
        field = SchemaField('id', 'integer')
        for model in (SchemaObservation, SchemaConstraint):
            model(())
            model((field,))
            for fields in ((field, field), [field], ('id',)):
                with self.subTest(model=model, fields=fields):
                    with self.assertRaises(ValueError):
                        model(fields)
        for name, data_type in (('', 'integer'), ('id', ' ')):
            with self.assertRaises(ValueError):
                SchemaField(name, data_type)
        with self.assertRaises(ValueError):
            SchemaConstraint((), allow_extra_fields='false')


class ResultTests(unittest.TestCase):
    """Verify result evidence, metadata, and contract compatibility."""

    def setUp(self):
        """Create a valid null-rate result for each test to inspect or vary."""
        self.result = CheckResult(
            check_id='orders.id.null_rate',
            kind=CheckKind.NULL_RATE,
            status=CheckStatus.FAIL,
            observation=NullRateObservation(0.2),
            constraint=NullRateConstraint(0.1),
            reason_code='threshold_exceeded',
            message='Null rate exceeds the configured maximum.',
            context=QualityContext('orders', 'batch-42', 'id'),
        )

    def test_result_preserves_evidence_and_is_immutable(self):
        """Preserve supplied evidence and prevent result reassignment."""
        self.assertEqual(self.result.observation.null_rate, 0.2)
        self.assertEqual(self.result.constraint.max_null_rate, 0.1)
        self.assertEqual(self.result.context.scope_id, 'batch-42')
        with self.assertRaises(FrozenInstanceError):
            self.result.status = CheckStatus.PASS

    def test_result_rejects_mixed_contracts(self):
        """Reject mismatched contract types and malformed result metadata."""
        for changes in (
            {'observation': RowCountObservation(10)},
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
                    replace(self.result, **changes)

    def test_all_check_kinds_have_matching_contracts(self):
        """Accept the matching observation and constraint for every check kind."""
        now = datetime(2026, 10, 4, tzinfo=timezone.utc)
        for kind, observation, constraint in (
            (CheckKind.FRESHNESS, FreshnessObservation(None, now),
             FreshnessConstraint(timedelta(hours=1))),
            (CheckKind.NULL_RATE, NullRateObservation(None), NullRateConstraint(0)),
            (CheckKind.ROW_COUNT, RowCountObservation(None), RowCountConstraint(min_count=1)),
            (CheckKind.SCHEMA, SchemaObservation(None), SchemaConstraint(())),
        ):
            with self.subTest(kind=kind):
                replace(self.result, kind=kind, observation=observation,
                        constraint=constraint, status=CheckStatus.UNKNOWN,
                        reason_code='missing_observation', message='Observation unavailable.')

    def test_structured_schema_details(self):
        """Preserve field-level schema diagnostics in structured result details."""
        detail = CheckDetail('type_mismatch', 'id', 'integer', 'string')
        result = replace(
            self.result, kind=CheckKind.SCHEMA,
            observation=SchemaObservation((SchemaField('id', 'string'),)),
            constraint=SchemaConstraint((SchemaField('id', 'integer'),)),
            reason_code='schema_mismatch', details=(detail,),
        )
        self.assertEqual(result.details[0].expected, 'integer')

    def test_context_identifiers_are_required_when_context_is_provided(self):
        """Reject blank dataset, scope, or supplied column identifiers."""
        for args in (('', 'batch'), ('orders', ''), ('orders', 'batch', ' ')):
            with self.assertRaises(ValueError):
                QualityContext(*args)


class PolicyContractTests(unittest.TestCase):
    """Verify reaction settings and policy decision contracts."""

    def test_default_and_overridden_reactions(self):
        """Accept WARN/BLOCK overrides while rejecting PASS and non-policy enums."""
        rule = PolicyRule('orders.id.null_rate')
        self.assertIs(rule.on_fail, QualityStatus.BLOCK)
        self.assertIs(rule.on_unknown, QualityStatus.WARN)
        PolicyRule(rule.check_id, QualityStatus.WARN, QualityStatus.BLOCK)
        for field in ('on_fail', 'on_unknown'):
            for value in (QualityStatus.PASS, 'WARN', CheckStatus.FAIL):
                with self.assertRaises(ValueError):
                    replace(rule, **{field: value})

    def test_decision_requires_policy_status_and_explanation(self):
        """Require a policy enum and a non-empty tuple of structured reasons."""
        reason = DecisionReason('no_checks', 'No checks were supplied.')
        decision = QualityDecision(QualityStatus.WARN, (reason,))
        self.assertIsNone(decision.reasons[0].check_id)
        for status, reasons in (
            (CheckStatus.PASS, (reason,)), (QualityStatus.PASS, ()),
            (QualityStatus.WARN, [reason]), (QualityStatus.WARN, ('no_checks',)),
        ):
            with self.assertRaises(ValueError):
                QualityDecision(status, reasons)


if __name__ == '__main__':
    unittest.main()
