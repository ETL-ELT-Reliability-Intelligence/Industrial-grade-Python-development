"""Verify behavior shared by all deterministic checks."""

from datetime import datetime, timedelta, timezone
import unittest
from contracts.quality import CheckKind, CheckStatus, FreshnessConstraint, FreshnessObservation, NullRateConstraint, NullRateObservation, QualityContext, RowCountConstraint, RowCountObservation, SchemaConstraint, SchemaObservation
from data_quality.checks import check_freshness, check_null_rate, check_row_count, check_schema


class CommonCheckTests(unittest.TestCase):
    """Verify behavior shared by all deterministic checks."""

    def test_missing_observations_and_preserved_evidence(self):
        """Return UNKNOWN consistently and preserve all caller-supplied evidence."""
        now = datetime(2026, 10, 4, tzinfo=timezone.utc)
        context = QualityContext("orders", "window-42")
        for check, kind, observation, constraint in (
            (check_freshness, CheckKind.FRESHNESS, FreshnessObservation(None, now),
             FreshnessConstraint(timedelta(hours=1))),
            (check_null_rate, CheckKind.NULL_RATE, NullRateObservation(None),
             NullRateConstraint(0.1)),
            (check_row_count, CheckKind.ROW_COUNT, RowCountObservation(None),
             RowCountConstraint(min_count=1)),
            (check_schema, CheckKind.SCHEMA, SchemaObservation(None), SchemaConstraint(())),
        ):
            with self.subTest(kind=kind):
                result = check(observation, constraint, check_id="rule", context=context)
                self.assertIs(result.status, CheckStatus.UNKNOWN)
                self.assertEqual(result.reason_code, "missing_observation")
                self.assertIs(result.kind, kind)
                self.assertIs(result.observation, observation)
                self.assertIs(result.constraint, constraint)
                self.assertIs(result.context, context)
                self.assertEqual(result.check_id, "rule")
                self.assertEqual(result, check(observation, constraint,
                                              check_id="rule", context=context))
