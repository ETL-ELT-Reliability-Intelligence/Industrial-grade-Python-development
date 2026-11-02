"""Verify behavior shared by all deterministic checks."""

import unittest
from contracts.quality import (
    CheckKind,
    CheckStatus,
    NullRateConstraint,
    NullRateObservation,
    QualityContext,
    RowCountConstraint,
    RowCountObservation,
    SchemaConstraint,
    SchemaObservation,
)
from data_quality.checks import check_null_rate, check_row_count, check_schema
from data_quality.execution import REGISTRY
from contracts.quality import ScalarObservation, ScalarConstraint


class CommonCheckTests(unittest.TestCase):
    """Verify behavior shared by all deterministic checks."""

    def test_missing_observations_and_preserved_evidence(self):
        """Return UNKNOWN consistently and preserve all caller-supplied evidence."""
        context = QualityContext(dataset_id="orders", scope_id="window-42")
        covered = set()
        for check, kind, observation, constraint in (
            *(
                (
                    REGISTRY[kind],
                    kind,
                    ScalarObservation(value=None),
                    ScalarConstraint(maximum=1.0),
                )
                for kind in (
                    CheckKind.CARDINALITY,
                    CheckKind.DISTRIBUTION,
                    CheckKind.VALUE_RULE,
                )
            ),
            (
                check_null_rate,
                CheckKind.NULL_RATE,
                NullRateObservation(null_rate=None),
                NullRateConstraint(max_null_rate=0.1),
            ),
            (
                check_row_count,
                CheckKind.ROW_COUNT,
                RowCountObservation(row_count=None),
                RowCountConstraint(min_count=1),
            ),
            (
                check_schema,
                CheckKind.SCHEMA,
                SchemaObservation(fields=None),
                SchemaConstraint(fields=()),
            ),
        ):
            covered.add(kind)
            with self.subTest(kind=kind):
                result = check(
                    observation, constraint, check_id="rule", context=context
                )
                self.assertIs(result.status, CheckStatus.UNKNOWN)
                self.assertEqual(result.reason_code, "missing_observation")
                self.assertIs(result.kind, kind)
                self.assertIs(result.observation, observation)
                self.assertIs(result.constraint, constraint)
                self.assertIs(result.context, context)
                self.assertEqual(result.check_id, "rule")
                self.assertEqual(
                    type(result).model_validate_json(result.model_dump_json()), result
                )
                self.assertEqual(
                    result,
                    check(observation, constraint, check_id="rule", context=context),
                )
        self.assertEqual(
            covered, set(REGISTRY), "Add UNKNOWN cases for every registered check"
        )
