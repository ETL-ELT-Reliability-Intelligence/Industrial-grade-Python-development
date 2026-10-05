"""Validation of dataset statistic contracts."""

from datetime import datetime, timezone
import unittest

from contracts.metrics import MetricKind, MetricPoint

NOW = datetime(2026, 10, 5, tzinfo=timezone.utc)


class MetricPointTests(unittest.TestCase):
    """Verify column usage and value ranges of metric points."""

    def test_valid_points(self):
        """Accept every metric kind with a suitable value, including None."""
        MetricPoint("orders", "b-1", MetricKind.ROW_COUNT, NOW, "v1", 0)
        MetricPoint("orders", "b-1", MetricKind.ROW_COUNT, NOW, "v1")
        MetricPoint("orders", "b-1", MetricKind.NULL_RATE, NOW, "v1", 0.5, "id")
        MetricPoint("orders", "b-1", MetricKind.DISTINCT_COUNT, NOW, "v1", 10, "id")
        MetricPoint("orders", "b-1", MetricKind.FRESHNESS_SECONDS, NOW, "v1", 12.5)

    def test_invalid_points(self):
        """Reject wrong columns, out-of-range values and malformed fields."""
        cases = (
            dict(metric=MetricKind.NULL_RATE, value=0.5),
            dict(metric=MetricKind.NULL_RATE, value=0.5, column=" "),
            dict(metric=MetricKind.ROW_COUNT, value=1, column="id"),
            dict(metric=MetricKind.ROW_COUNT, value=1.5),
            dict(metric=MetricKind.ROW_COUNT, value=-1),
            dict(metric=MetricKind.ROW_COUNT, value=True),
            dict(metric=MetricKind.NULL_RATE, value=1.1, column="id"),
            dict(metric=MetricKind.FRESHNESS_SECONDS, value=-1),
            dict(metric=MetricKind.FRESHNESS_SECONDS, value=float("inf")),
            dict(metric="row_count", value=1),
            dict(metric=MetricKind.ROW_COUNT, value=1, observed_at=NOW.replace(tzinfo=None)),
            dict(metric=MetricKind.ROW_COUNT, value=1, stats_version=""),
        )
        for case in cases:
            with self.subTest(case=case):
                arguments = dict(dataset_id="orders", scope_id="b-1", observed_at=NOW,
                                 stats_version="v1")
                arguments.update(case)
                with self.assertRaises(ValueError):
                    MetricPoint(**arguments)


if __name__ == "__main__":
    unittest.main()
