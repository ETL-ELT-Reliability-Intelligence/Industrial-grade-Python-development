"""Verify freshness boundaries and timezone handling."""

from datetime import datetime, timedelta, timezone
import unittest
from contracts.quality import CheckStatus, FreshnessConstraint, FreshnessObservation
from data_quality.checks import check_freshness


class FreshnessCheckTests(unittest.TestCase):
    """Verify freshness boundaries and timezone handling."""

    def test_freshness_inclusive_boundary(self):
        """Evaluate elapsed ages below, at, and above the permitted maximum."""
        now = datetime(2026, 10, 4, tzinfo=timezone.utc)
        for age, expected in ((59, CheckStatus.PASS), (60, CheckStatus.PASS),
                              (61, CheckStatus.FAIL)):
            with self.subTest(age=age):
                result = check_freshness(
                    FreshnessObservation(last_updated_at=now - timedelta(seconds=age), evaluated_at=now),
                    FreshnessConstraint(max_age=timedelta(seconds=60)), check_id="freshness",
                )
                self.assertIs(result.status, expected)
                self.assertEqual(result.reason_code, "threshold_exceeded" if age > 60
                                 else "requirement_met")
        self.assertIs(check_freshness(
            FreshnessObservation(last_updated_at=now, evaluated_at=now), FreshnessConstraint(max_age=timedelta(0)),
            check_id="freshness",
        ).status, CheckStatus.PASS)

    def test_freshness_compares_instants_across_offsets(self):
        """Treat different timezone representations of an instant as zero age."""
        now = datetime(2026, 10, 4, tzinfo=timezone.utc)
        updated = now.astimezone(timezone(timedelta(hours=3)))
        result = check_freshness(
            FreshnessObservation(last_updated_at=updated, evaluated_at=now), FreshnessConstraint(max_age=timedelta(0)),
            check_id="freshness",
        )
        self.assertIs(result.status, CheckStatus.PASS)
