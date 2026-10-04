"""Policy tests using actual deterministic check results."""

from itertools import permutations
import unittest

from contracts.quality import (
    PolicyRule, QualityStatus, RowCountConstraint, RowCountObservation,
)
from data_quality.checks import check_row_count
from data_quality.policies import evaluate_quality


class QualityPolicyTests(unittest.TestCase):
    """Verify default reactions, overrides, precedence, and input ambiguity."""

    def setUp(self):
        """Create passing, failing, and unavailable results with distinct IDs."""
        self.passed = check_row_count(RowCountObservation(1), RowCountConstraint(min_count=1),
                                     check_id="passed")
        self.failed = check_row_count(RowCountObservation(0), RowCountConstraint(min_count=1),
                                     check_id="failed")
        self.unknown = check_row_count(RowCountObservation(None), RowCountConstraint(min_count=1),
                                      check_id="unknown")

    def test_defaults(self):
        """Pass compliant checks, block failures, and warn on missing observations."""
        for result, expected in ((self.passed, QualityStatus.PASS),
                                 (self.failed, QualityStatus.BLOCK),
                                 (self.unknown, QualityStatus.WARN)):
            with self.subTest(status=result.status):
                decision = evaluate_quality([result])
                self.assertIs(decision.status, expected)
                self.assertEqual(decision.reasons[0].check_id, result.check_id)
                self.assertEqual(decision.reasons[0].code, result.reason_code)
                self.assertIn(expected.value, decision.reasons[0].message)

    def test_reactions_can_be_overridden(self):
        """Apply overrides to the configured check ID and leave successes passing."""
        self.assertIs(evaluate_quality([self.failed], [
            PolicyRule("failed", on_fail=QualityStatus.WARN),
        ]).status, QualityStatus.WARN)
        self.assertIs(evaluate_quality([self.unknown], [
            PolicyRule("unknown", on_unknown=QualityStatus.BLOCK),
        ]).status, QualityStatus.BLOCK)
        self.assertIs(evaluate_quality([self.passed], [
            PolicyRule("passed", on_unknown=QualityStatus.BLOCK),
        ]).status, QualityStatus.PASS)
        self.assertIs(evaluate_quality([self.failed], [
            PolicyRule("another_check", on_fail=QualityStatus.WARN),
        ]).status, QualityStatus.BLOCK)

    def test_precedence_is_independent_of_result_order(self):
        """Keep the strongest reaction while retaining reasons for every result."""
        for results in permutations((self.passed, self.failed, self.unknown)):
            decision = evaluate_quality(iter(results))
            self.assertIs(decision.status, QualityStatus.BLOCK)
            self.assertEqual([r.check_id for r in decision.reasons],
                             [r.check_id for r in results])
        for results in permutations((self.passed, self.unknown)):
            self.assertIs(evaluate_quality(results).status, QualityStatus.WARN)

    def test_empty_results_warn(self):
        """Do not present an absent set of checks as evidence of good quality."""
        decision = evaluate_quality(iter(()))
        self.assertIs(decision.status, QualityStatus.WARN)
        self.assertEqual(decision.reasons[0].code, "no_checks")
        self.assertIsNone(decision.reasons[0].check_id)

    def test_duplicate_ids_are_rejected(self):
        """Reject repeated results and rules instead of silently overwriting them."""
        with self.assertRaisesRegex(ValueError, "duplicate check result"):
            evaluate_quality([self.passed, self.passed])
        with self.assertRaisesRegex(ValueError, "duplicate policy rule"):
            evaluate_quality([self.passed], [PolicyRule("passed"), PolicyRule("passed")])

    def test_policy_rejects_untyped_inputs(self):
        """Require contract objects rather than transport dictionaries."""
        with self.assertRaises(ValueError):
            evaluate_quality([{}])
        with self.assertRaises(ValueError):
            evaluate_quality([self.passed], [{}])

    def test_policy_accepts_rule_generators_and_is_repeatable(self):
        """Consume iterable rules without retaining mutable policy state."""
        rules = (PolicyRule("failed", on_fail=QualityStatus.WARN),)
        expected = evaluate_quality([self.failed], rules)
        self.assertEqual(evaluate_quality([self.failed], iter(rules)), expected)
        self.assertIs(evaluate_quality([self.failed]).status, QualityStatus.BLOCK)
