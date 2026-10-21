from services.stream_processor.statistics import compute_statistics

RECORDS = [
    {"id": 1, "amount": 10.5, "name": "a", "active": True, "comment": None},
    {"id": 2, "amount": 20.0, "name": "b", "active": False, "comment": "ok"},
    {"id": 3, "amount": 30.25, "name": "a", "active": True},
]


def test_row_count_is_the_number_of_records():
    assert compute_statistics(RECORDS).row_count == 3


def test_schema_types_are_inferred_per_column():
    schema = {field.name: field.data_type for field in compute_statistics(RECORDS).schema}

    assert schema == {
        "id": "integer", "amount": "number", "name": "string",
        "active": "boolean", "comment": "string",
    }


def test_missing_keys_count_as_nulls():
    rates = compute_statistics(RECORDS).null_rates

    assert rates["comment"] == 2 / 3
    assert rates["id"] == 0


def test_distinct_counts_ignore_nulls():
    counts = compute_statistics(RECORDS).distinct_counts

    assert counts == {"id": 3, "amount": 3, "name": 2, "active": 2, "comment": 1}


def test_empty_batch_has_no_columns_and_undefined_rates():
    statistics = compute_statistics([])

    assert statistics.row_count == 0
    assert statistics.schema == ()
    assert statistics.null_rates == {}
    assert statistics.distinct_counts == {}


def test_mixed_types_are_unknown():
    statistics = compute_statistics([{"value": 1}, {"value": "a"}])

    assert statistics.schema[0].data_type == "unknown"


def test_all_null_column_is_unknown_and_fully_null():
    statistics = compute_statistics([{"value": None}, {"value": None}])

    assert statistics.schema[0].data_type == "unknown"
    assert statistics.null_rates["value"] == 1.0
    assert statistics.distinct_counts["value"] == 0


def test_unhashable_values_are_counted_by_their_json_form():
    statistics = compute_statistics([{"tags": ["a"]}, {"tags": ["a"]}, {"tags": ["b"]}])

    assert statistics.distinct_counts["tags"] == 2
