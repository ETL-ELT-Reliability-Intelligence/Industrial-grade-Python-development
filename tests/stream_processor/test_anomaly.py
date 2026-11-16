from datetime import datetime, timedelta, timezone

from contracts.events import DatasetIngestedEvent
from contracts.profiles import BatchProfile
from services.stream_processor.anomaly import (
    DEFAULT_ANOMALY_CONFIG_PATH,
    detect_anomalies,
    load_anomaly_config,
)

NOW = datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc)
MINUTE = timedelta(minutes=1)


def make_config():
    return load_anomaly_config(DEFAULT_ANOMALY_CONFIG_PATH)


def make_event(batch_id: str, received_at: datetime, records: int = 10):
    return DatasetIngestedEvent(
        dataset_id="orders",
        batch_id=batch_id,
        records=records,
        received_at=received_at,
        schema_version=1,
    )


def make_profile(batch_id: str, received_at: datetime, row_count: int, columns=None):
    return BatchProfile(
        event=make_event(batch_id, received_at, row_count),
        config_hash="hash",
        row_count=row_count,
        columns=columns or {},
    )


def stable_history(metric: str, value: float, count: int = 3):
    return [
        make_profile(
            f"b{index}",
            NOW - (count - index + 1) * MINUTE,
            10,
            columns={"amount": {metric: value}},
        )
        for index in range(1, count + 1)
    ]


def test_load_anomaly_config_from_directory(tmp_path):
    (tmp_path / "defaults.yaml").write_text(
        "detector: robust-z\n"
        "version: v2\n"
        "min_observations: 4\n"
        "metrics:\n"
        "  row_count:\n"
        "    threshold_z: 5.0\n"
        "    absolute_tolerance: 2.0\n"
        "    relative_tolerance: 0.1\n",
        encoding="utf-8",
    )

    config = load_anomaly_config(tmp_path)

    assert config.version == "v2"
    assert config.min_observations == 4
    assert config.metrics["row_count"].threshold_z == 5.0


def test_short_history_produces_no_anomalies():
    config = make_config()
    current = make_profile(
        "b4", NOW, 1, columns={"amount": {"null_rate": 1.0}}
    )

    assert detect_anomalies(current, [], config) == []
    assert (
        detect_anomalies(
            current,
            [make_profile("b1", NOW - MINUTE, 10, {"amount": {"null_rate": 0.0}})],
            config,
        )
        == []
    )


def test_null_rate_growth_is_anomaly_with_bounded_score():
    config = make_config()
    current = make_profile(
        "b4", NOW, 10, columns={"amount": {"null_rate": 1.0}}
    )

    anomalies = detect_anomalies(current, stable_history("null_rate", 0.0), config)

    assert [a.metric for a in anomalies] == ["null_rate.amount"]
    assert anomalies[0].expected == 0.0
    assert anomalies[0].actual == 1.0
    assert 0 < anomalies[0].score <= 1.0


def test_row_count_drop_is_anomaly():
    config = make_config()
    history = [
        make_profile(f"b{index}", NOW - (4 - index) * MINUTE, 10)
        for index in range(1, 4)
    ]
    current = make_profile("b4", NOW, 0)

    anomalies = detect_anomalies(current, history, config)

    assert [a.metric for a in anomalies] == ["row_count"]
    assert anomalies[0].expected == 10.0
    assert anomalies[0].actual == 0.0
    assert anomalies[0].score <= 1.0


def test_distinct_ratio_drop_is_anomaly():
    config = make_config()
    current = make_profile(
        "b4", NOW, 10, columns={"amount": {"distinct_ratio": 0.1}}
    )

    anomalies = detect_anomalies(current, stable_history("distinct_ratio", 0.9), config)

    assert [a.metric for a in anomalies] == ["distinct_ratio.amount"]


def test_anomaly_id_is_deterministic():
    config = make_config()
    history = stable_history("null_rate", 0.0)
    current = make_profile("b4", NOW, 10, columns={"amount": {"null_rate": 1.0}})

    first = detect_anomalies(current, history, config)
    second = detect_anomalies(current, history, config)

    assert [a.anomaly_id for a in first] == [a.anomaly_id for a in second]


def test_score_never_exceeds_one():
    config = make_config()
    current = make_profile("b4", NOW, 10_000)

    anomalies = detect_anomalies(
        current,
        [
            make_profile(f"b{index}", NOW - (4 - index) * MINUTE, 10)
            for index in range(1, 4)
        ],
        config,
    )

    assert anomalies[0].score == 1.0
