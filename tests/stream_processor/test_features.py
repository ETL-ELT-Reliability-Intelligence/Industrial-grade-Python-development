import hashlib
import json
import math

import pytest

from contracts.features import FeatureDefinition
from services.stream_processor.feature_accumulation import load_feature_definitions
from services.stream_processor.feature_calculation import build_feature_profile
from services.stream_processor.raw import read_raw
from tests.stream_processor.test_evaluation import event_for, suite
from tests.stream_processor.test_handlers import make_handler


def definition(columns=None, quantiles=None):
    value = {"columns": columns or [{"name": "amount", "kind": "numeric"}]}
    if quantiles is not None:
        value["quantiles"] = quantiles
    return FeatureDefinition.model_validate_json(json.dumps(value))


def profile(tmp_path, rows, spec=None):
    event = event_for(tmp_path, rows)
    records = read_raw(event.raw_uri)
    return build_feature_profile(
        event, records, spec or definition(), records.sha256, "test-v1"
    )


def test_numeric_semantics_include_denominators_and_linear_quantiles(tmp_path):
    values = [0, "10", 20, None, "bad", True, "NaN", "Infinity", "1e9999"]
    result = profile(tmp_path, [{"amount": v} for v in values]).columns[0]
    assert (result.valid_count, result.null_count, result.parse_error_count) == (
        3,
        1,
        5,
    )
    assert result.mean == 10
    assert result.minimum == 0
    assert result.maximum == 20
    assert result.stddev == pytest.approx(math.sqrt(200 / 3), rel=1e-12, abs=1e-12)
    assert result.quantiles == (5, 10, 15)


@pytest.mark.parametrize(
    "rows,missing,nulls,errors",
    [
        ([], True, 0, 0),
        ([{}], True, 1, 0),
        ([{"amount": None}], False, 1, 0),
        ([{"amount": "bad"}], False, 0, 1),
    ],
)
def test_empty_missing_and_invalid_are_not_zero_measurements(
    tmp_path, rows, missing, nulls, errors
):
    result = profile(tmp_path, rows).columns[0]
    assert result.missing_column is missing
    assert result.null_count == nulls
    assert result.parse_error_count == errors
    assert result.valid_count == 0
    assert result.mean is result.stddev is result.quantiles is None
    if not rows:
        assert result.null_rate is None


def test_single_value_population_stddev_is_zero(tmp_path):
    result = profile(tmp_path, [{"amount": 4}]).columns[0]
    assert result.stddev == 0
    assert result.quantiles == (4, 4, 4)


def test_numeric_results_do_not_depend_on_external_decimal_context(tmp_path):
    from decimal import localcontext, ROUND_DOWN

    rows = [{"amount": n} for n in ["0.111111111111111111", "0.333333333333333333", 4]]
    expected = profile(tmp_path, rows).columns
    with localcontext() as context:
        context.prec = 2
        context.rounding = ROUND_DOWN
        assert profile(tmp_path, rows).columns == expected


def test_fixed_vocabulary_and_other_bucket_do_not_collide(tmp_path):
    spec = definition(
        [{"name": "category", "kind": "categorical", "categories": ["other", "a", "b"]}]
    )
    result = profile(
        tmp_path, [{"category": c} for c in ["other", "a", "new", None]], spec
    ).columns[0]
    assert result.category_counts == (1, 1, 0)
    assert result.other_count == 1
    assert result.category_frequencies == pytest.approx((1 / 3, 1 / 3, 0))
    assert result.other_frequency == pytest.approx(1 / 3)


def test_row_permutation_preserves_counters_and_numeric_precision(tmp_path):
    rows = [{"amount": n} for n in [1e20, 0.1, -1e20, 5, None, "bad"]]
    first = profile(tmp_path, rows)
    second = profile(tmp_path, list(reversed(rows)))
    a, b = first.columns[0], second.columns[0]
    assert (a.null_count, a.distinct_count, a.valid_count, a.parse_error_count) == (
        b.null_count,
        b.distinct_count,
        b.valid_count,
        b.parse_error_count,
    )
    for field in ("mean", "stddev", "minimum", "maximum", "quantiles"):
        assert getattr(a, field) == pytest.approx(
            getattr(b, field), rel=1e-12, abs=1e-12
        )
    assert first.raw_sha256 != second.raw_sha256


def test_raw_checksum_is_of_exact_bytes_used_for_parsing(tmp_path):
    path = tmp_path / "raw.jsonl"
    content = b'{"amount": 1}\r\n\r\n'
    path.write_bytes(content)
    records = read_raw(path.as_uri())
    assert records == [{"amount": 1}]
    assert records.sha256 == hashlib.sha256(content).hexdigest()


def test_definition_is_independent_of_quality_thresholds_and_configured_for_datasets():
    definitions = load_feature_definitions()
    assert definitions["order_items"].columns[0].kind == "numeric"
    assert "checks" not in definitions["orders"].model_dump()
    assert definition().fingerprint != definition(quantiles=[0.5]).fingerprint
    for invalid in ([float("nan")], [-0.1], [0.5, 0.5]):
        with pytest.raises(ValueError):
            definition(quantiles=invalid)


def test_quality_reevaluation_reuses_profile_and_preserves_separate_eligibility(
    tmp_path,
):
    handler, _, _, metrics, quality, _ = make_handler(suites={"orders": suite()})
    event = event_for(tmp_path)
    first = handler.handle_dataset_ingested(event)
    original = next(iter(metrics.feature_profiles.values()))
    handler._suites = {"orders": suite(100)}
    second = handler.handle_dataset_ingested(event, request_id="stricter")
    assert first.decision.status.value == "PASS"
    assert second.decision.status.value == "BLOCK"
    assert len(metrics.feature_profiles) == 1
    assert len(quality.evaluations) == 2
    assert metrics.feature_links[(first.evaluation_id, original.profile_id)] is True
    assert metrics.feature_links[(second.evaluation_id, original.profile_id)] is False
    assert next(iter(metrics.feature_profiles.values())) == original
    assert "eligible" not in original.model_dump()


def test_calculation_failure_and_retry_do_not_change_quality(tmp_path, monkeypatch):
    import services.stream_processor.feature_accumulation as module

    handler, _, _, metrics, quality, _ = make_handler(suites={"orders": suite()})
    event = event_for(tmp_path)
    real = module.build_feature_profile

    def fail(*args):
        raise ArithmeticError("calculation failed")

    monkeypatch.setattr(module, "build_feature_profile", fail)
    first = handler.handle_dataset_ingested(event)
    assert first.decision.status.value == "PASS"
    assert next(iter(metrics.profiling_attempts.values())).status == "ERROR"
    assert not metrics.feature_profiles
    monkeypatch.setattr(module, "build_feature_profile", real)
    second = handler.handle_dataset_ingested(event)
    assert first == second
    assert len(quality.evaluations) == len(metrics.feature_profiles) == 1
    assert [a.status for a in metrics.profiling_attempts.values()] == [
        "ERROR",
        "COMPLETED",
    ]


def test_replay_reuses_success_without_recalculation(tmp_path, monkeypatch):
    handler, _, _, metrics, _, _ = make_handler(suites={"orders": suite()})
    event = event_for(tmp_path)
    handler.handle_dataset_ingested(event)
    monkeypatch.setattr(
        "services.stream_processor.feature_accumulation.build_feature_profile",
        lambda *args: pytest.fail("must reuse successful profile"),
    )
    handler.handle_dataset_ingested(event)
    assert len(metrics.feature_profiles) == 1
    assert len(metrics.feature_links) == 1


def test_raw_mutation_is_a_conflict_even_on_completed_evaluation(tmp_path):
    handler, _, _, metrics, _, _ = make_handler(suites={"orders": suite()})
    event = event_for(tmp_path)
    first = handler.handle_dataset_ingested(event)
    original = next(iter(metrics.feature_profiles.values()))
    event_for(tmp_path, [{"id": 999}])
    repeated = handler.handle_dataset_ingested(event)
    assert repeated == first
    assert [a.status for a in metrics.profiling_attempts.values()] == [
        "COMPLETED",
        "CONFLICT",
    ]
    assert list(metrics.feature_profiles.values()) == [original]


def test_evaluation_checksum_protects_retry_before_first_profile(tmp_path):
    handler, _, _, metrics, _, _ = make_handler(suites={"orders": suite()})
    event = event_for(tmp_path)
    # Simulate termination after quality delivery, before any profiling attempt.
    collector = handler._features.collect
    handler._features.collect = lambda *args: None
    evaluation = handler.handle_dataset_ingested(event)
    handler._features.collect = collector
    assert evaluation.raw_sha256
    assert not metrics.feature_inputs
    event_for(tmp_path, [{"id": 999}])
    handler.handle_dataset_ingested(event)
    assert not metrics.feature_profiles
    assert next(iter(metrics.profiling_attempts.values())).status == "CONFLICT"


def test_invalid_mutated_raw_is_also_a_conflict(tmp_path):
    from services.stream_processor.raw import local_path

    handler, _, _, metrics, _, _ = make_handler(suites={"orders": suite()})
    event = event_for(tmp_path)
    handler.handle_dataset_ingested(event)
    local_path(event.raw_uri).write_bytes(b"not-json")
    handler.handle_dataset_ingested(event)
    assert list(metrics.profiling_attempts.values())[-1].status == "CONFLICT"


def test_definition_and_implementation_changes_preserve_old_snapshot(tmp_path):
    handler, _, _, metrics, quality, _ = make_handler(suites={"orders": suite()})
    event = event_for(tmp_path)
    first = handler.handle_dataset_ingested(event)
    original = next(iter(metrics.feature_profiles.values()))
    handler._features.definitions = {"orders": definition()}
    handler.handle_dataset_ingested(event)
    handler._features.implementation = "fixed-calculator"
    handler.handle_dataset_ingested(event)
    assert len(metrics.feature_profiles) == 3
    assert len(quality.evaluations) == 1
    assert original in metrics.feature_profiles.values()
    assert all(key[0] == first.evaluation_id for key in metrics.feature_links)


def test_storage_failure_leaves_quality_final_and_replay_repairs_profile(
    tmp_path, monkeypatch
):
    handler, _, _, metrics, quality, _ = make_handler(suites={"orders": suite()})
    event = event_for(tmp_path)
    original_save = metrics.save_feature_profile

    def fail(profile):
        raise ConnectionError("database unavailable")

    monkeypatch.setattr(metrics, "save_feature_profile", fail)
    with pytest.raises(ConnectionError):
        handler.handle_dataset_ingested(event)
    evaluation = next(iter(quality.evaluations.values()))
    assert evaluation.published
    assert evaluation.decision.status.value == "PASS"
    monkeypatch.setattr(metrics, "save_feature_profile", original_save)
    assert handler.handle_dataset_ingested(event) == evaluation
    assert len(metrics.feature_profiles) == 1
