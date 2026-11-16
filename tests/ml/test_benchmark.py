from services.ml.benchmark import generated_history, summarize, START, CUTOFF, TEST_START
from services.ml.volume import prepare_training


def test_benchmark_is_seeded_and_training_is_chronological():
    profiles, labels = generated_history(42)
    same, same_labels = generated_history(42)
    # Event UUIDs are transport identities, not model inputs.
    assert [(p.event.batch_id, p.row_count) for p in profiles] == [(p.event.batch_id, p.row_count) for p in same]
    assert labels == same_labels
    rows, diagnostics = prepare_training(profiles, CUTOFF)
    assert diagnostics["usable_targets"] == 504
    assert rows[335][0] < rows[336][0] < CUTOFF < TEST_START
    assert all(p.event.received_at > p.event.period_end for p in profiles)
    assert any(item["actual"] is None for item in labels.values())
    _, missing = prepare_training(profiles[1:], CUTOFF)
    assert missing["usable_targets"] < 504
    assert (START.isoformat() in missing["missing_lag_targets"][0]["missing_periods"])


def test_recall_counts_skipped_anomalies_and_delay_is_not_zero_for_missed_episode():
    records = [
        {"index": 0, "kind": "drop", "actual": 1, "status": "SKIPPED"},
        {"index": 1, "kind": "drop", "actual": 1, "status": "WARN", "expected": 10, "lower": 8, "upper": 12},
        {"index": 2, "kind": "missed", "actual": 2, "status": "PASS", "expected": 10, "lower": 0, "upper": 20},
        {"index": 3, "kind": "normal", "actual": 10, "status": "WARN", "expected": 1, "lower": 0, "upper": 2},
    ]
    result = summarize(records)
    assert result["precision"] == 0.5
    assert result["recall"] == 1 / 3
    assert result["false_alarms"] == 1
    assert result["detection_delays"] == [{"episode": "drop", "delay_hours": 1}, {"episode": "missed", "delay_hours": None}]
