"""Run lagged boosting after deterministic checks, without changing their decision."""

import logging
from uuid import NAMESPACE_URL, uuid5

from contracts.state import Anomaly
from services.ml.volume import Predictor, OBSERVATION_VERSION, lag_features
from .publisher import build_anomaly_event

logger = logging.getLogger(__name__)


class OrderVolumeMonitor:
    def __init__(self, repository, profiles, anomalies, publisher):
        self.repository = repository
        self.profiles = profiles
        self.anomalies = anomalies
        self.publisher = publisher
        self.predictor = Predictor()

    def process(self, evaluation):
        event = evaluation.event
        if event.dataset_id != "mock_orders" or event.source_id != "mock":
            return
        result = self.repository.get_prediction(evaluation.evaluation_id)
        if result is None:
            profile = self.profiles.get_profile(event.dataset_id, event.batch_id)
            observation = profile.order_volume if profile else None
            if not observation or observation.get("version") != OBSERVATION_VERSION or "count" not in observation:
                result = {"status": "SKIPPED", "reason": (observation or {}).get("reason", "missing_hourly_observation")}
            else:
                stored = self.repository.find_model(event)
                if stored is None:
                    result = {"status": "SKIPPED", "reason": "insufficient_training_history"}
                else:
                    history = self.repository.load_history(event.period_start)
                    try:
                        inputs = lag_features(event.period_start, history)
                        result = ({"status": "SKIPPED", "reason": "insufficient_lag_history"}
                                  if inputs is None else self.predictor.predict(
                                      stored, event.period_start, observation["count"], inputs,
                                  ))
                    except Exception as error:
                        logger.exception("Order volume prediction failed for %s", event.batch_id)
                        result = {"status": "ERROR", "reason": str(error), "model_version": stored["model_version"]}
            result.update(period_start=event.period_start.isoformat() if event.period_start else None,
                          dataset_id=event.dataset_id, batch_id=event.batch_id)
            self.repository.save_prediction(evaluation.evaluation_id, result)
        logger.info("Order volume evaluation=%s result=%s", evaluation.evaluation_id, result)
        if result["status"] == "WARN":
            identity = f"order-volume:{evaluation.evaluation_id}:{result['model_version']}"
            anomaly = Anomaly(
                anomaly_id=str(uuid5(NAMESPACE_URL, identity)), dataset_id=event.dataset_id,
                batch_id=event.batch_id, metric="order_volume", expected=result["expected"],
                actual=float(result["actual"]), score=min(result["deviation"] / (2 * max(result["threshold"], 1e-9)), 1.0),
                detector="lag_gradient_boosting", model_version=result["model_version"],
                detected_at=evaluation.evaluated_at,
            )
            self.anomalies.save_anomaly(anomaly)
            self.publisher.publish(build_anomaly_event(anomaly))
        return result
