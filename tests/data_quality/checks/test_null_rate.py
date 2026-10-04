"""Verify inclusive null-rate thresholds."""

import unittest
from contracts.quality import CheckStatus, NullRateConstraint, NullRateObservation
from data_quality.checks import check_null_rate


class NullRateCheckTests(unittest.TestCase):
    """Verify inclusive null-rate thresholds."""

    def test_null_rate_inclusive_boundary(self):
        """Accept zero and the exact maximum, but fail values above it."""
        for value, maximum, expected in (
            (0, 0, CheckStatus.PASS), (0.1, 0.2, CheckStatus.PASS),
            (0.2, 0.2, CheckStatus.PASS), (0.3, 0.2, CheckStatus.FAIL),
            (1, 1, CheckStatus.PASS),
        ):
            with self.subTest(value=value, maximum=maximum):
                result = check_null_rate(NullRateObservation(value),
                                         NullRateConstraint(maximum), check_id="nulls")
                self.assertIs(result.status, expected)
                self.assertEqual(result.reason_code, "threshold_exceeded"
                                 if expected is CheckStatus.FAIL else "requirement_met")
