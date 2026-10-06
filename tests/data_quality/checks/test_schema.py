"""Verify exact schema requirements and complete, ordered diagnostics."""

import unittest
from contracts.quality import CheckDetail, CheckStatus, SchemaConstraint, SchemaField, SchemaObservation
from data_quality.checks import check_schema


class SchemaCheckTests(unittest.TestCase):
    """Verify exact schema requirements and complete, ordered diagnostics."""

    def test_field_order_does_not_affect_compliance(self):
        """Accept reordered fields without changing names or types."""
        fields = (SchemaField("id", "integer"), SchemaField("name", "string"))
        result = check_schema(SchemaObservation(tuple(reversed(fields))),
                              SchemaConstraint(fields), check_id="schema")
        self.assertIs(result.status, CheckStatus.PASS)
        self.assertEqual(result.reason_code, "requirement_met")
        self.assertEqual(result.details, ())

    def test_all_mismatches_are_reported_in_name_order(self):
        """Report missing, unexpected, and mistyped fields without type coercion."""
        observation = SchemaObservation((SchemaField("z", "string"), SchemaField("id", "int")))
        constraint = SchemaConstraint((SchemaField("id", "integer"), SchemaField("a", "string")))
        result = check_schema(observation, constraint, check_id="schema")
        self.assertIs(result.status, CheckStatus.FAIL)
        self.assertEqual(result.reason_code, "schema_mismatch")
        self.assertEqual(result.details, (
            CheckDetail("missing_field", "a", "string"),
            CheckDetail("type_mismatch", "id", "integer", "int"),
            CheckDetail("unexpected_field", "z", actual="string"),
        ))

    def test_allowing_extra_fields_does_not_relax_required_fields(self):
        """Ignore extras only, preserving missing-field and type requirements."""
        expected = (SchemaField("id", "integer"),)
        extra = SchemaField("extra", "string")
        for fields, status in (
            ((*expected, extra), CheckStatus.PASS),
            ((extra,), CheckStatus.FAIL),
            ((SchemaField("id", "string"), extra), CheckStatus.FAIL),
            ((SchemaField("ID", "integer"),), CheckStatus.FAIL),
        ):
            with self.subTest(fields=fields):
                result = check_schema(SchemaObservation(fields),
                                      SchemaConstraint(expected, True), check_id="schema")
                self.assertIs(result.status, status)
                self.assertFalse(any(d.code == "unexpected_field" for d in result.details))

    def test_empty_schema_is_an_observation(self):
        """Evaluate empty schemas rather than interpreting them as unavailable."""
        for actual, expected, status in (
            ((), (), CheckStatus.PASS),
            ((), (SchemaField("id", "integer"),), CheckStatus.FAIL),
            ((SchemaField("id", "integer"),), (), CheckStatus.FAIL),
        ):
            with self.subTest(actual=actual, expected=expected):
                result = check_schema(SchemaObservation(actual), SchemaConstraint(expected),
                                      check_id="schema")
                self.assertIs(result.status, status)
