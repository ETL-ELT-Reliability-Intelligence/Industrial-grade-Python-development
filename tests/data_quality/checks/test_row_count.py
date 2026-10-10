"""Verify inclusive row-count bounds."""

import unittest
from contracts.quality import CheckStatus, RowCountConstraint, RowCountObservation
from data_quality.checks import check_row_count


class RowCountCheckTests(unittest.TestCase):
    """Verify inclusive row-count bounds."""

    def test_row_count_bounds(self):
        """Check two-sided, one-sided, exact-count, and zero-count requirements."""
        for count, minimum, maximum, code in (
            (9, 10, 20, "below_minimum"), (10, 10, 20, "requirement_met"),
            (20, 10, 20, "requirement_met"), (21, 10, 20, "above_maximum"),
            (100, 1, None, "requirement_met"), (0, None, 10, "requirement_met"),
            (11, None, 10, "above_maximum"), (0, 1, None, "below_minimum"),
            (0, 0, 0, "requirement_met"), (5, 5, 5, "requirement_met"),
        ):
            with self.subTest(count=count, minimum=minimum, maximum=maximum):
                result = check_row_count(RowCountObservation(row_count=count),
                                         RowCountConstraint(min_count=minimum, max_count=maximum), check_id="rows")
                self.assertEqual(result.reason_code, code)
                self.assertIs(result.status, CheckStatus.PASS if code == "requirement_met"
                              else CheckStatus.FAIL)
