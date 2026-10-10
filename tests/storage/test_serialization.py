"""JSON round trips of quality results, without a database."""

from datetime import datetime, timedelta, timezone
import json
import unittest

import pytest

from contracts.quality import (
    CheckKind,
    FreshnessConstraint, FreshnessObservation, NullRateConstraint, NullRateObservation,
    QualityContext, RowCountConstraint, RowCountObservation, SchemaConstraint, SchemaField,
    SchemaObservation,
)
from data_quality.checks import check_freshness, check_null_rate, check_row_count, check_schema
from storage.serialization import (
    check_result_from_row, check_result_to_row, reasons_from_json, reasons_to_json,
    constraint_from_json, constraint_to_json, observation_from_json,
)
from data_quality.policies import evaluate_quality

NOW = datetime(2026, 10, 5, 10, tzinfo=timezone.utc)


def test_legacy_freshness_jsonb_shape():
    constraint = constraint_from_json(CheckKind.FRESHNESS, {"max_age_seconds": 1800.25})
    assert constraint.max_age == timedelta(seconds=1800.25)
    assert constraint_to_json(constraint) == {"max_age_seconds": 1800.25}
    observation = observation_from_json(CheckKind.FRESHNESS, {
        "last_updated_at": None, "evaluated_at": "2026-10-05T10:00:00+00:00",
    })
    assert observation.last_updated_at is None
    assert observation.evaluated_at == NOW


@pytest.mark.parametrize("seconds", [True, "1800", -1, float("nan"), float("inf")])
def test_legacy_duration_is_strictly_validated(seconds):
    with pytest.raises(ValueError):
        constraint_from_json(CheckKind.FRESHNESS, {"max_age_seconds": seconds})


@pytest.mark.parametrize("kind,payload", [
    (CheckKind.NULL_RATE, {"null_rate": "0.1"}),
    (CheckKind.ROW_COUNT, {"row_count": True}),
    (CheckKind.ROW_COUNT, {"row_count": 1, "extra": 2}),
    (CheckKind.FRESHNESS, {"last_updated_at": None, "evaluated_at": "2026-10-05T10:00:00"}),
    (CheckKind.SCHEMA, {"fields": [{"name": "id", "data_type": "int"}] * 2}),
    (CheckKind.SCHEMA, {"fields": [{"name": "id", "data_type": "int", "extra": 1}]}),
])
def test_invalid_stored_observations_are_not_coerced(kind, payload):
    with pytest.raises(ValueError):
        observation_from_json(kind, payload)


def sample_results():
    """Return results of every check kind, including FAIL and UNKNOWN."""
    context = QualityContext(dataset_id="orders", scope_id="b-1")
    column = QualityContext(dataset_id="orders", scope_id="b-1", column="comment")
    fields = (SchemaField(name="id", data_type="integer"), SchemaField(name="name", data_type="string"))
    return (
        check_freshness(FreshnessObservation(last_updated_at=NOW - timedelta(minutes=95), evaluated_at=NOW),
                        FreshnessConstraint(max_age=timedelta(minutes=30)),
                        check_id="orders.freshness", context=context),
        check_freshness(FreshnessObservation(last_updated_at=None, evaluated_at=NOW),
                        FreshnessConstraint(max_age=timedelta(minutes=30)),
                        check_id="orders.freshness_unknown", context=context),
        check_null_rate(NullRateObservation(null_rate=0.15), NullRateConstraint(max_null_rate=0.1),
                        check_id="orders.comment.null_rate", context=column),
        check_row_count(RowCountObservation(row_count=1800000), RowCountConstraint(min_count=3150000, max_count=None),
                        check_id="orders.row_count", context=context),
        check_schema(SchemaObservation(fields=(SchemaField(name="id", data_type="string"), SchemaField(name="z", data_type="string"))),
                     SchemaConstraint(fields=fields, allow_extra_fields=True),
                     check_id="orders.schema", context=context),
        check_schema(SchemaObservation(fields=None), SchemaConstraint(fields=fields),
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
        result = check_row_count(RowCountObservation(row_count=1), RowCountConstraint(min_count=1), check_id="rows")
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
