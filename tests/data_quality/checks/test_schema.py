"""Verify exact schema requirements and complete, ordered diagnostics."""

import unittest
from contracts.quality import CheckDetail, CheckStatus, SchemaConstraint, SchemaField, SchemaObservation
from data_quality.checks import check_schema


class SchemaCheckTests(unittest.TestCase):
    """Verify exact schema requirements and complete, ordered diagnostics."""

    def test_field_order_does_not_affect_compliance(self):
        """Accept reordered fields without changing names or types."""
        fields = (SchemaField(name="id", data_type="integer"), SchemaField(name="name", data_type="string"))
        result = check_schema(SchemaObservation(fields=tuple(reversed(fields))),
                              SchemaConstraint(fields=fields), check_id="schema")
        self.assertIs(result.status, CheckStatus.PASS)
        self.assertEqual(result.reason_code, "requirement_met")
        self.assertEqual(result.details, ())

    def test_all_mismatches_are_reported_in_name_order(self):
        """Report missing, unexpected, and mistyped fields without type coercion."""
        observation = SchemaObservation(fields=(SchemaField(name="z", data_type="string"), SchemaField(name="id", data_type="int")))
        constraint = SchemaConstraint(fields=(SchemaField(name="id", data_type="integer"), SchemaField(name="a", data_type="string")))
        result = check_schema(observation, constraint, check_id="schema")
        self.assertIs(result.status, CheckStatus.FAIL)
        self.assertEqual(result.reason_code, "schema_mismatch")
        self.assertEqual(result.details, (
            CheckDetail(code="missing_field", field="a", expected="string"),
            CheckDetail(code="type_mismatch", field="id", expected="integer", actual="int"),
            CheckDetail(code="unexpected_field", field="z", actual="string"),
        ))

    def test_allowing_extra_fields_does_not_relax_required_fields(self):
        """Ignore extras only, preserving missing-field and type requirements."""
        expected = (SchemaField(name="id", data_type="integer"),)
        extra = SchemaField(name="extra", data_type="string")
        for fields, status in (
            ((*expected, extra), CheckStatus.PASS),
            ((extra,), CheckStatus.FAIL),
            ((SchemaField(name="id", data_type="string"), extra), CheckStatus.FAIL),
            ((SchemaField(name="ID", data_type="integer"),), CheckStatus.FAIL),
        ):
            with self.subTest(fields=fields):
                result = check_schema(SchemaObservation(fields=fields),
                                      SchemaConstraint(fields=expected, allow_extra_fields=True), check_id="schema")
                self.assertIs(result.status, status)
                self.assertFalse(any(d.code == "unexpected_field" for d in result.details))

    def test_empty_schema_is_an_observation(self):
        """Evaluate empty schemas rather than interpreting them as unavailable."""
        for actual, expected, status in (
            ((), (), CheckStatus.PASS),
            ((), (SchemaField(name="id", data_type="integer"),), CheckStatus.FAIL),
            ((SchemaField(name="id", data_type="integer"),), (), CheckStatus.FAIL),
        ):
            with self.subTest(actual=actual, expected=expected):
                result = check_schema(SchemaObservation(fields=actual), SchemaConstraint(fields=expected),
                                      check_id="schema")
                self.assertIs(result.status, status)
