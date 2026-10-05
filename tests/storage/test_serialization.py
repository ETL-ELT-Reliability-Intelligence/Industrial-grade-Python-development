"""JSON round trips of quality results, without a database."""

from datetime import datetime, timedelta, timezone
import json
import unittest

from contracts.quality import (
    FreshnessConstraint, FreshnessObservation, NullRateConstraint, NullRateObservation,
    QualityContext, RowCountConstraint, RowCountObservation, SchemaConstraint, SchemaField,
    SchemaObservation,
)
from data_quality.checks import check_freshness, check_null_rate, check_row_count, check_schema
from storage.serialization import (
    check_result_from_row, check_result_to_row, reasons_from_json, reasons_to_json,
)
from data_quality.policies import evaluate_quality

NOW = datetime(2026, 10, 5, 10, tzinfo=timezone.utc)


def sample_results():
    """Return results of every check kind, including FAIL and UNKNOWN."""
    context = QualityContext("orders", "b-1")
    column = QualityContext("orders", "b-1", "comment")
    fields = (SchemaField("id", "integer"), SchemaField("name", "string"))
    return (
        check_freshness(FreshnessObservation(NOW - timedelta(minutes=95), NOW),
                        FreshnessConstraint(timedelta(minutes=30)),
                        check_id="orders.freshness", context=context),
        check_freshness(FreshnessObservation(None, NOW),
                        FreshnessConstraint(timedelta(minutes=30)),
                        check_id="orders.freshness_unknown", context=context),
        check_null_rate(NullRateObservation(0.15), NullRateConstraint(0.1),
                        check_id="orders.comment.null_rate", context=column),
        check_row_count(RowCountObservation(1800000), RowCountConstraint(3150000, None),
                        check_id="orders.row_count", context=context),
        check_schema(SchemaObservation((SchemaField("id", "string"), SchemaField("z", "string"))),
                     SchemaConstraint(fields, allow_extra_fields=True),
                     check_id="orders.schema", context=context),
        check_schema(SchemaObservation(None), SchemaConstraint(fields),
                     check_id="orders.schema_unknown", context=context),
    )


class SerializationTests(unittest.TestCase):
    """Verify that stored JSON restores the exact contract objects."""

    def test_check_results_round_trip(self):
        """Restore every kind of result through JSON text."""
        for result in sample_results():
            with self.subTest(check_id=result.check_id):
                row = check_result_to_row(result)
                row = json.loads(json.dumps(row))
                self.assertEqual(check_result_from_row(row), result)

    def test_result_without_context_cannot_be_stored(self):
        """Require the scope that identifies the stored result."""
        result = check_row_count(RowCountObservation(1), RowCountConstraint(1), check_id="rows")
        with self.assertRaises(ValueError):
            check_result_to_row(result)

    def test_decision_reasons_round_trip(self):
        """Restore decision reasons, including a reason without check_id."""
        for results in (sample_results(), ()):
            reasons = evaluate_quality(results).reasons
            self.assertEqual(reasons_from_json(json.loads(json.dumps(reasons_to_json(reasons)))),
                             reasons)


if __name__ == "__main__":
    unittest.main()
