"""Statistical decisions, provenance, edge cases and real Olist-shaped batches."""

from datetime import timedelta
import json
from pathlib import Path

import pytest

from contracts.events import DatasetIngestedEvent
from contracts.quality import CheckKind
from contracts.quality_config import CheckSuite, PARAMETERS
from data_quality.config import load_suites
from data_quality.history import select_history
from data_quality.preparation import prepare, js_distance
from services.stream_processor.profiling import build_profile
from tests.stream_processor.test_handlers import NOW, make_handler, write_batch


def suite(kind, column=None, **params):
    spec = {"key": "test", "kind": kind, "params": params}
    if column:
        spec["column"] = column
    return CheckSuite.model_validate_json(
        json.dumps({"dataset_id": "test", "checks": [spec]})
    )


def profile(config, rows, index=4, **metadata):
    event = DatasetIngestedEvent(
        dataset_id="test",
        batch_id=f"b{index}",
        records=len(rows),
        received_at=NOW + timedelta(hours=index),
        schema_version=1,
        **metadata,
    )
    return build_profile(event, rows, config).model_copy(update={"eligible": True})


def evaluate(config, current, history):
    spec = config.checks[0]
    params = PARAMETERS[spec.kind].model_validate_json(json.dumps(spec.params))
    return prepare(spec, params, current, select_history(current, history))


def test_volume_uses_spread_and_constant_history_has_a_tolerance():
    config = suite("row_count")
    current = profile(config, [{}] * 40)
    flat = [profile(config, [{}] * 100, i) for i in range(3)]
    obs, bounds, code, baseline = evaluate(config, current, flat)
    assert obs.row_count == 40 and bounds.min_count == 80 and bounds.max_count == 120
    assert code is None and len(baseline["batch_ids"]) == 3
    variable = [profile(config, [{}] * n, i) for i, n in enumerate((70, 100, 130))]
    _, bounds, _, _ = evaluate(config, current, variable)
    assert bounds.min_count < 40  # Actual historical spread changes the decision.


def test_null_growth_from_zero_and_conditional_delivery_population():
    config = suite(
        "null_rate", "delivery", where_column="status", where_value="delivered"
    )
    normal = [{"status": "delivered", "delivery": "2018-01-02"}] * 100
    history = [profile(config, normal, i) for i in range(3)]
    current = profile(
        config,
        normal[:80]
        + [{"status": "delivered", "delivery": None}] * 20
        + [{"status": "canceled", "delivery": None}] * 200,
    )
    obs, bounds, code, _ = evaluate(config, current, history)
    assert code is None and obs.null_rate == 0.2 and bounds.max_null_rate < 0.2
    unchanged = profile(
        config, normal + [{"status": "canceled", "delivery": None}] * 500
    )
    assert evaluate(config, unchanged, history)[0].null_rate == 0


def test_cardinality_compares_both_count_and_ratio_at_similar_volume():
    config = suite("cardinality", "city")
    normal = [{"city": str(i % 50)} for i in range(100)]
    history = [profile(config, normal, i) for i in range(3)]
    observation, _, code, baseline = evaluate(
        config, profile(config, [{"city": "one"}] * 100), history
    )
    assert code is None and observation.value > 1
    assert observation.measurements["distinct_ratio"] == 0.01
    assert "distinct_count" in baseline and "distinct_ratio" in baseline
    assert (
        evaluate(config, profile(config, normal * 4), history)[2]
        == "insufficient_history"
    )


@pytest.mark.parametrize(
    "kind,params,normal,changed",
    [
        ("numeric", {"bins": [0, 10, 100]}, "5.0", "500"),
        ("categorical", {}, "card", "new_method"),
    ],
)
def test_distribution_detects_new_categories_and_numeric_overflow(
    kind, params, normal, changed
):
    config = suite("distribution", "x", data_type=kind, **params)
    history = [profile(config, [{"x": normal}] * 100, i) for i in range(3)]
    obs, _, code, baseline = evaluate(
        config, profile(config, [{"x": changed}] * 100), history
    )
    assert code is None and obs.value == 1
    assert baseline["data_type"] == kind
    assert (
        evaluate(config, profile(config, [{"x": normal}] * 200), history)[0].value == 0
    )


def test_distribution_distance_is_symmetric_and_zero_on_identical_shares():
    p, q = {"a": 0.5, "b": 0.5}, {"a": 0.2, "c": 0.8}
    assert js_distance(p, q) == pytest.approx(js_distance(q, p))
    assert js_distance(p, p) == 0


@pytest.mark.parametrize(
    "rows,reason",
    [
        ([], "missing_column"),
        ([{"x": None}] * 100, "insufficient_samples"),
        ([{"x": "bad"}] * 100, "invalid_numeric_values"),
        ([{"x": "NaN"}] * 100, "invalid_numeric_values"),
        ([{"x": True}] * 100, "invalid_numeric_values"),
    ],
)
def test_distribution_never_passes_empty_or_invalid_input(rows, reason):
    config = suite("distribution", "x", data_type="numeric", bins=[0, 10])
    assert evaluate(config, profile(config, rows), [])[2] == reason


def test_value_rules_count_errors_and_preserve_raw_values():
    config = suite("value_rule", "price", rule="non_negative")
    rows = [{"price": x} for x in ("0", "1.25", "-0.01", "bad", "Infinity", None)]
    obs, bound, code, _ = evaluate(config, profile(config, rows), [])
    assert code is None and bound.maximum == 0
    assert obs.measurements["invalid_count"] == 4
    assert obs.value == pytest.approx(4 / 6) and rows[1]["price"] == "1.25"


def test_date_order_checks_present_pairs_and_does_not_accept_invalid_dates():
    config = suite(
        "value_rule",
        "delivery",
        rule="date_order",
        other_column="purchase",
        allow_null=True,
    )
    rows = [
        {"purchase": "2018-01-02", "delivery": d}
        for d in ("2018-01-03", "2018-01-01", "bad", None)
    ]
    obs, _, _, _ = evaluate(config, profile(config, rows), [])
    assert (
        obs.measurements["invalid_count"] == 2
        and obs.measurements["evaluated_count"] == 3
    )


def test_history_excludes_duplicate_periods_future_sources_and_rejected_replacements():
    config = suite("row_count")

    def monthly(index, month, **kw):
        return profile(
            config,
            [{}],
            index,
            source_id="olist",
            granularity="month",
            period_start=NOW.replace(month=month, day=1),
            period_end=NOW.replace(month=month + 1, day=1),
            **kw,
        )

    first = monthly(0, 1)
    replacement = monthly(1, 1).model_copy(update={"eligible": False})
    good = monthly(2, 2)
    future = monthly(3, 5)
    current = monthly(4, 4)
    other_source = profile(config, [{}], 0, source_id="mock")
    assert select_history(
        current, [first, replacement, good, future, other_source]
    ) == [good]
    incompatible = good.model_copy(update={"stats_version": "other"})
    assert select_history(current, [incompatible]) == []


def test_olist_pipeline_profiles_bootstrap_then_detect_partial_month(tmp_path):
    from services.ingestion.olist_sample import write_sample
    from services.ingestion.sources import OlistSource
    from services.ingestion.service import IngestionService
    from services.ingestion.publisher import InMemoryPublisher
    from services.ingestion.storage import LocalRawStore

    root = tmp_path / "olist"
    write_sample(root, months=4, orders_per_month=300)
    handler, _, _, metrics, _, _ = make_handler(
        suites=load_suites(Path("config/quality"))
    )
    service = IngestionService(
        LocalRawStore(tmp_path / "raw"), InMemoryPublisher(), clock=lambda: NOW
    )
    for month in range(4):
        event = service.ingest(
            OlistSource("order_payments", root, month, 0.4 if month == 3 else 1.0)
        )
        result = handler.handle_dataset_ingested(event)
        assert result.decision.status.value == "WARN"
        assert metrics.profiles[(event.dataset_id, event.batch_id)].eligible == (
            month < 3
        )
    rows = next(c for c in result.checks if c.spec.kind is CheckKind.ROW_COUNT)
    assert rows.result.status.value == "FAIL"
    assert len(rows.baseline["batch_ids"]) == 3
    assert result == handler.handle_dataset_ingested(event)


def test_negative_amount_blocks_without_history_and_no_id_rules_are_shipped(tmp_path):
    catalog = load_suites(Path("config/quality"))
    for dataset in catalog:
        for check in catalog[dataset].checks:
            assert check.column is None or not check.column.endswith("_id")
            assert check.kind.value != "freshness"
    handler, *_ = make_handler(suites=catalog)
    event = DatasetIngestedEvent(
        dataset_id="mock_orders",
        batch_id="bad",
        records=1,
        received_at=NOW,
        schema_version=1,
        source_id="mock",
        raw_uri=write_batch(
            tmp_path, "bad", [{"amount": -1, "payment_method": "card"}]
        ),
    )
    result = handler.handle_dataset_ingested(event)
    assert result.decision.status.value == "BLOCK"
    assert (
        next(
            c for c in result.checks if c.spec.kind is CheckKind.VALUE_RULE
        ).result.status.value
        == "FAIL"
    )


@pytest.mark.parametrize(
    "kind,params",
    [
        ("distribution", {"data_type": "numeric", "bins": [10, 0]}),
        ("distribution", {"data_type": "numeric", "bins": []}),
        ("value_rule", {"rule": "unique"}),
        ("null_rate", {"where_column": "status"}),
    ],
)
def test_invalid_configuration_is_rejected(kind, params):
    with pytest.raises(ValueError):
        suite(kind, "x", **params)


def test_window_metadata_is_validated_and_partial_windows_do_not_compare():
    with pytest.raises(ValueError):
        DatasetIngestedEvent(
            dataset_id="test",
            batch_id="bad",
            records=0,
            received_at=NOW,
            schema_version=1,
            granularity="month",
        )
    config = suite("row_count")
    history = [profile(config, [{}] * 100, i) for i in range(3)]
    current = profile(config, [{}] * 10, complete=False)
    assert evaluate(config, current, history)[2] == "incomplete_window"


def test_later_arrival_of_an_older_period_does_not_leak_into_old_evaluation():
    config = suite("row_count")
    old_period = dict(
        source_id="olist",
        granularity="month",
        period_start=NOW.replace(month=1, day=1),
        period_end=NOW.replace(month=2, day=1),
    )
    future_arrival = profile(config, [{}], 9, **old_period)
    current = profile(
        config,
        [{}],
        4,
        source_id="olist",
        granularity="month",
        period_start=NOW.replace(month=2, day=1),
        period_end=NOW.replace(month=3, day=1),
    )
    assert select_history(current, [future_arrival]) == []
