from types import SimpleNamespace

import pytest

from data_quality.config import load_suites
from pathlib import Path
from services.stream_processor.order_volume import OrderVolumeMonitor
from tests.ml.test_volume import START, HOUR, OBSERVATION_VERSION, event, profiles, CUTOFF
from tests.stream_processor.test_handlers import make_handler, write_batch


class MemoryVolume:
    def __init__(self):
        self.predictions = {}
        self.model = None

    def get_prediction(self, identity):
        return self.predictions.get(identity)

    def save_prediction(self, identity, result):
        self.predictions.setdefault(identity, result)

    def find_model(self, event):
        return self.model

    def load_history(self, start):
        return profiles()


def monitor_fixture():
    repository = MemoryVolume()
    profile = SimpleNamespace(order_volume={"version": OBSERVATION_VERSION, "count": 1000})
    anomalies, events = {}, []
    monitor = OrderVolumeMonitor(
        repository, SimpleNamespace(get_profile=lambda *_: profile),
        SimpleNamespace(save_anomaly=lambda a: anomalies.setdefault(a.anomaly_id, a)),
        SimpleNamespace(publish=events.append),
    )
    evaluation = SimpleNamespace(event=event(CUTOFF), evaluation_id="evaluation-1", evaluated_at=CUTOFF + 2 * HOUR)
    return monitor, repository, evaluation, anomalies, events


def test_no_model_and_invalid_hour_have_explicit_status():
    monitor, repository, evaluation, _, _ = monitor_fixture()
    assert monitor.process(evaluation)["reason"] == "insufficient_training_history"
    monitor.profiles = SimpleNamespace(get_profile=lambda *_: None)
    evaluation.evaluation_id = "evaluation-2"
    assert monitor.process(evaluation)["reason"] == "missing_hourly_observation"


def test_saved_prediction_survives_retry_and_model_change():
    monitor, repository, evaluation, anomalies, events = monitor_fixture()
    repository.model = {"model_version": "v1"}
    monitor.predictor = SimpleNamespace(predict=lambda *_: {
        "status": "WARN", "model_version": "v1", "actual": 1000, "expected": 100.0,
        "deviation": 9.0, "threshold": 0.2, "direction": "increase",
    })
    result = monitor.process(evaluation)
    monitor.predictor = None
    repository.model = {"model_version": "v2"}
    assert monitor.process(evaluation) == result
    assert len(anomalies) == 1
    assert len(events) == 2 and events[0].event_id == events[1].event_id


def test_model_error_is_recorded_without_raising():
    monitor, repository, evaluation, anomalies, _ = monitor_fixture()
    repository.model = {"model_version": "broken"}
    def fail(*_):
        raise ValueError("broken model")
    monitor.predictor = SimpleNamespace(predict=fail)
    assert monitor.process(evaluation)["status"] == "ERROR"
    assert not anomalies


def test_insufficient_lags_skip_instead_of_predicting_zero():
    monitor, repository, evaluation, anomalies, _ = monitor_fixture()
    repository.model = {"model_version": "current"}
    repository.load_history = lambda _: []
    assert monitor.process(evaluation)["reason"] == "insufficient_lag_history"
    assert not anomalies


def test_storage_errors_are_retried_not_hidden():
    monitor, repository, evaluation, _, _ = monitor_fixture()
    def fail(*_):
        raise ConnectionError("database down")
    repository.save_prediction = fail
    with pytest.raises(ConnectionError):
        monitor.process(evaluation)


def test_main_handler_runs_monitor_and_preserves_quality_decision(tmp_path):
    handler, _, _, metrics, quality, publisher = make_handler(suites=load_suites(Path("config/quality")))
    repository = MemoryVolume()
    anomalies = []
    handler._order_volume = OrderVolumeMonitor(
        repository, metrics, SimpleNamespace(save_anomaly=anomalies.append), publisher,
    )
    loaded = event(raw_uri=write_batch(tmp_path, "orders", [
        {"order_id": 1, "created_at": START.isoformat(), "amount": 10.0, "payment_method": "card"},
    ]))
    evaluation = handler.handle_dataset_ingested(loaded)
    assert repository.predictions[evaluation.evaluation_id]["status"] == "SKIPPED"
    assert metrics.get_profile("mock_orders", loaded.batch_id).order_volume["count"] == 1
    assert evaluation.decision.status.value == "WARN"
    repeated = handler.handle_dataset_ingested(loaded)
    assert repeated.decision == evaluation.decision
