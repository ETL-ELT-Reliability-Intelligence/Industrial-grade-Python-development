"""Detect batch-profile anomalies with robust z-scores and configured tolerances."""

from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import yaml
from pydantic import BaseModel, ConfigDict

from contracts.profiles import BatchProfile
from contracts.state import Anomaly

DEFAULT_ANOMALY_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config/anomaly"


class MetricRule(BaseModel):
    """Metric thresholds with absolute and relative tolerance floors for dispersion."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    threshold_z: float
    absolute_tolerance: float
    relative_tolerance: float


class AnomalyConfig(BaseModel):
    """Detector identity and per-metric anomaly rules."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    detector: str
    version: str
    min_observations: int
    metrics: dict[str, MetricRule]


def load_anomaly_config(path: Path) -> AnomalyConfig:
    """Validate anomaly rules from a YAML file or a directory containing defaults.yaml."""
    source = path / "defaults.yaml" if path.is_dir() else path
    data = yaml.safe_load(source.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"expected a mapping in {source}")
    return AnomalyConfig.model_validate(data)


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def _mad(values: list[float], median: float) -> float:
    return _median([abs(value - median) for value in values])


def _evaluate(
    actual: float, values: list[float], rule: MetricRule, min_observations: int
) -> tuple[float, float] | None:
    """Return expected value and score for an anomaly, otherwise None."""
    if len(values) < min_observations:
        return None
    expected = _median(values)
    spread = max(
        1.4826 * _mad(values, expected),
        rule.absolute_tolerance,
        rule.relative_tolerance * abs(expected),
        1e-9,
    )
    z = abs(actual - expected) / spread
    if z < rule.threshold_z:
        return None
    return expected, min(z / (2 * rule.threshold_z), 1.0)


def _build(
    current: BatchProfile, metric: str, expected: float, actual: float, score: float,
    config: AnomalyConfig,
) -> Anomaly:
    event = current.event
    identity = f"anomaly:{event.dataset_id}:{event.batch_id}:{metric}:{config.version}"
    return Anomaly(
        anomaly_id=str(uuid5(NAMESPACE_URL, identity)),
        dataset_id=event.dataset_id,
        batch_id=event.batch_id,
        metric=metric,
        expected=float(expected),
        actual=float(actual),
        score=score,
        detector=config.detector,
        model_version=config.version,
        detected_at=event.received_at,
    )


def _column_values(history: list[BatchProfile], column: str, metric: str) -> list[float]:
    """Collect available column measurements, preserving measured zeros."""
    values = []
    for profile in history:
        summary = profile.columns.get(column)
        value = summary.get(metric) if summary else None
        if value is not None:
            values.append(float(value))
    return values


def detect_anomalies(
    current: BatchProfile, history: list[BatchProfile], config: AnomalyConfig
) -> list[Anomaly]:
    """Compare a profile against caller-selected compatible history without I/O.

    Scores increase with deviation and remain in [0, 1]; they are not
    probabilities. Metrics with insufficient history produce no anomalies.
    """
    anomalies: list[Anomaly] = []

    rule = config.metrics.get("row_count")
    if rule is not None:
        found = _evaluate(
            float(current.row_count),
            [float(p.row_count) for p in history],
            rule,
            config.min_observations,
        )
        if found is not None:
            anomalies.append(
                _build(
                    current,
                    "row_count",
                    found[0],
                    float(current.row_count),
                    found[1],
                    config,
                )
            )

    for metric in ("null_rate", "distinct_ratio"):
        rule = config.metrics.get(metric)
        if rule is None:
            continue
        for column in sorted(current.columns):
            actual = current.columns[column].get(metric)
            if actual is None:
                continue
            found = _evaluate(
                float(actual),
                _column_values(history, column, metric),
                rule,
                config.min_observations,
            )
            if found is not None:
                anomalies.append(
                    _build(
                        current,
                        f"{metric}.{column}",
                        found[0],
                        float(actual),
                        found[1],
                        config,
                    )
                )
    return anomalies
