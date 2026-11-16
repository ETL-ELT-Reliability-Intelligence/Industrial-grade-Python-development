"""Point-in-time lag features and quantile boosting for hourly order volume."""

from datetime import timedelta, timezone
import hashlib
import io
import json
import math
from statistics import mean, median, pstdev

from data_quality.dates import aware_datetime

HOUR = timedelta(hours=1)
OBSERVATION_VERSION = "order-volume-v1"
VERSION = "order-volume-lag-boost-v2"
LAGS = (1, 2, 3, 24, 48, 168)
WINDOWS = (6, 24, 168)
FEATURES = (["hour_sin", "hour_cos", "weekday_sin", "weekday_cos", "weekend"]
            + [f"lag_{lag}" for lag in LAGS]
            + [f"rolling_{window}_{metric}" for window in WINDOWS
               for metric in ("mean", "median", "std", "available")])


def observe_hour(event, records):
    """Measure orders in a closed, complete UTC hour or return a reason code.

    The interval includes its start and excludes its end. Zero is valid only
    for an explicitly complete hour; invalid timestamps reject the observation.
    """
    start, end = event.period_start, event.period_end
    if event.granularity != "hour" or not event.complete or start is None or end is None:
        return {"reason": "incomplete_hour", "version": OBSERVATION_VERSION}
    start = start.astimezone(timezone.utc)
    if start.minute or start.second or start.microsecond or end - start != HOUR:
        return {"reason": "invalid_hour", "version": OBSERVATION_VERSION}
    if end > event.received_at:
        return {"reason": "open_hour", "version": OBSERVATION_VERSION}
    count = 0
    for record in records:
        try:
            created = aware_datetime(record.get("created_at"))
        except (ValueError, TypeError):
            return {"reason": "invalid_created_at", "version": OBSERVATION_VERSION}
        if not start <= created < end or created > event.received_at:
            return {"reason": "orders_outside_hour", "version": OBSERVATION_VERSION}
        count += 1
    return {"count": count, "version": OBSERVATION_VERSION}


def calendar_features(starts):
    """Encode UTC hour, weekday and weekend indicators in feature order."""
    result = []
    for start in starts:
        start = start.astimezone(timezone.utc)
        result.append([
            math.sin(2 * math.pi * start.hour / 24), math.cos(2 * math.pi * start.hour / 24),
            math.sin(2 * math.pi * start.weekday() / 7), math.cos(2 * math.pi * start.weekday() / 7),
            int(start.weekday() >= 5),
        ])
    return result


def latest_profiles(profiles, as_of):
    """Select the latest mock-source revision per hour received no later than as_of."""
    latest = {}
    for profile in sorted(profiles, key=lambda p: (p.event.received_at, p.event.batch_id)):
        event = profile.event
        if (event.dataset_id == "mock_orders" and event.source_id == "mock"
                and event.period_start is not None and event.received_at <= as_of):
            latest[event.period_start] = profile
    return latest


def observed_count(profile):
    """Return a compatible complete hourly count, or None when unavailable."""
    if profile is None:
        return None
    event = profile.event
    observation = profile.order_volume or {}
    if (observation.get("version") != OBSERVATION_VERSION or not event.complete
            or event.granularity != "hour" or event.period_start is None
            or event.period_end != event.period_start + HOUR):
        return None
    return observation.get("count")


def lag_features(start, profiles):
    """Build lag features using prior hours received by the forecast origin.

    Return None unless at least 80% of the preceding week, including t-24 and
    t-168, is available. Missing lags remain None; partial rolling windows
    include coverage counts. Return feature values and source batch IDs.
    """
    latest = latest_profiles(profiles, start)
    history = {lag: observed_count(latest.get(start - lag * HOUR)) for lag in range(1, 169)}
    available = sum(value is not None for value in history.values())
    if available < math.ceil(0.8 * 168) or history[24] is None or history[168] is None:
        return None
    vector = calendar_features([start])[0] + [history[lag] for lag in LAGS]
    for window in WINDOWS:
        values = [history[lag] for lag in range(1, window + 1) if history[lag] is not None]
        vector.extend([mean(values), median(values), pstdev(values), len(values)]
                      if values else [None, None, None, 0])
    batch_ids = [latest[start - lag * HOUR].event.batch_id
                 for lag in range(1, 169) if history[lag] is not None]
    return {"values": vector, "batch_ids": batch_ids, "available_hours": available}


def training_rows(profiles, cutoff):
    """Build 21 consecutive target days with point-in-time lag features.

    Return an empty list if any target or required history is unavailable.
    The latest revision wins even when invalid; missing hours never become zero.
    """
    end = cutoff.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0) - HOUR
    start = end - timedelta(days=21)
    profiles = list(profiles)
    latest = latest_profiles(profiles, cutoff)
    rows = []
    for index in range(21 * 24):
        hour = start + index * HOUR
        profile = latest.get(hour)
        count = observed_count(profile)
        inputs = lag_features(hour, profiles)
        if count is None or inputs is None:
            return []
        rows.append((hour, count, profile.event.batch_id, inputs))
    return rows


def deviation(actual, expected):
    """Scale absolute error by the expected count, with a denominator floor of one."""
    return abs(actual - expected) / max(expected, 1.0)


def train(rows, cutoff):
    """Fit mean and quantile models and calibrate their prediction interval.

    Require 21 hourly target days: 14 for training and seven for calibration.
    Return serialized models, provenance metadata and a metadata-derived version.
    Raise ValueError when the target row count differs from 21 days.
    """
    import joblib
    import sklearn
    from sklearn.ensemble import HistGradientBoostingRegressor

    if len(rows) != 21 * 24:
        raise ValueError("21 target days with preceding weekly history are required")
    training, calibration = rows[:14 * 24], rows[14 * 24:]
    models = {}
    x_train, x_calibration = [r[3]["values"] for r in training], [r[3]["values"] for r in calibration]
    for name, quantile in (("expected", None), ("lower", 0.05), ("upper", 0.95)):
        model = HistGradientBoostingRegressor(
            loss="squared_error" if quantile is None else "quantile", quantile=quantile,
            max_iter=100, max_leaf_nodes=15, min_samples_leaf=10,
            early_stopping=False, random_state=42,
        )
        model.fit(x_train, [r[1] for r in training])
        models[name] = model
    predictions = {name: model.predict(x_calibration) for name, model in models.items()}
    errors = []
    for index, row in enumerate(calibration):
        expected = max(0.0, float(predictions["expected"][index]))
        lower, upper = bounds(expected, predictions["lower"][index], predictions["upper"][index], 0.0)
        errors.append(max(lower - row[1], row[1] - upper, 0) / max(expected, 1.0))
    padding = float(sorted(errors)[math.ceil(0.99 * len(errors)) - 1])
    metadata = {
        "implementation": VERSION, "dataset_id": "mock_orders", "source_id": "mock",
        "cutoff": cutoff.isoformat(),
        "features": FEATURES, "sklearn_version": sklearn.__version__,
        "interval_padding": padding, "quantiles": [0.05, 0.95], "calibration_quantile": 0.99,
        "training_hours": len(training), "calibration_hours": len(calibration),
        "first_hour": rows[0][0].isoformat(), "last_hour": rows[-1][0].isoformat(),
        "batch_ids": [r[2] for r in rows],
        "history_batch_ids": sorted({batch for r in rows for batch in r[3]["batch_ids"]}),
        "parameters": {name: model.get_params() for name, model in models.items()},
        "data_sha256": hashlib.sha256(json.dumps(
            [(r[0].isoformat(), r[1], r[2], r[3]) for r in rows], separators=(",", ":"), allow_nan=False
        ).encode()).hexdigest(),
    }
    version = hashlib.sha256(json.dumps(metadata, sort_keys=True).encode()).hexdigest()
    buffer = io.BytesIO()
    joblib.dump(models, buffer)
    return {"model_version": version, "metadata": metadata, "artifact": buffer.getvalue()}


def bounds(expected, lower, upper, padding):
    """Expand quantile bounds by minimum relative tolerance and calibrated padding."""
    scale = max(expected, 1.0)
    return (max(0.0, min(float(lower), expected - 0.2 * scale) - padding * scale),
            max(float(upper), expected + 0.2 * scale) + padding * scale)


class Predictor:
    """Cache a deserialized model version for repeated hourly predictions."""

    def __init__(self):
        self.version = None
        self.model = None

    def predict(self, stored, start, actual, inputs):
        """Predict an interval and report WARN when the actual count falls outside.

        Raise ValueError when implementation, feature order or scikit-learn version
        differs from stored metadata. The result does not change quality policy.
        """
        import joblib
        import sklearn

        metadata = stored["metadata"]
        if (metadata["implementation"] != VERSION or metadata["features"] != FEATURES
                or metadata["sklearn_version"] != sklearn.__version__):
            raise ValueError("model runtime version mismatch; retrain with the processor environment")
        if self.version != stored["model_version"]:
            self.model = joblib.load(io.BytesIO(bytes(stored["artifact"])))
            self.version = stored["model_version"]
        predictions = {name: float(model.predict([inputs["values"]])[0]) for name, model in self.model.items()}
        expected = max(0.0, predictions["expected"])
        lower, upper = bounds(expected, predictions["lower"], predictions["upper"], metadata["interval_padding"])
        score = deviation(actual, expected)
        threshold = ((upper - expected) if actual > expected else (expected - lower)) / max(expected, 1.0)
        return {
            "status": "WARN" if actual < lower or actual > upper else "PASS",
            "model_version": self.version, "actual": actual, "expected": expected,
            "deviation": score, "threshold": threshold,
            "lower": lower, "upper": upper, "features": inputs,
            "direction": "increase" if actual > expected else "decrease" if actual < expected else "unchanged",
        }
