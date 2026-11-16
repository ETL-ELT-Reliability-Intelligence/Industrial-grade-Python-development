"""Train and register an order-volume model from persisted source observations."""

import argparse
from datetime import datetime, timedelta, timezone
import logging
import os

from data_quality.dates import aware_datetime
from services.ml.volume import prepare_training, train
from psycopg.types.json import Jsonb
from storage.postgres import connect, PostgresMetricRepository
from storage.postgres.order_volume import PostgresOrderVolumeRepository

logger = logging.getLogger(__name__)


def train_order_volume(connection, cutoff):
    """Train and persist a model using mock observations received by cutoff.

    Return TRAINED with model identity, or SKIPPED when continuous training
    history is insufficient. Training and storage failures propagate.
    """
    start = (cutoff.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
             - timedelta(hours=1)) - timedelta(days=21) - timedelta(hours=168)
    available = PostgresMetricRepository(connection).list_profiles_between(
        "mock_orders",
        source_id="mock",
        start=start,
        end=cutoff,
        as_of=cutoff,
        limit=10000,
    )
    rows, diagnostics = prepare_training(available, cutoff)
    previous = connection.execute(
        "SELECT created_at, cutoff, model_version FROM order_volume_models "
        "WHERE dataset_id = 'mock_orders' ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    result = {"status": "SKIPPED", "reason": "insufficient_training_history", "diagnostics": diagnostics,
              "cutoff": cutoff.isoformat(), "last_successful_training_at": previous["created_at"].isoformat() if previous else None,
              "last_model_cutoff": previous["cutoff"].isoformat() if previous else None}
    try:
        if rows:
            model = train(rows, cutoff)
            PostgresOrderVolumeRepository(connection).save_model(model)
            result.update(status="TRAINED", reason=None, model_version=model["model_version"],
                          interval_padding=model["metadata"]["interval_padding"],
                          last_successful_training_at=datetime.now(timezone.utc).isoformat(),
                          last_model_cutoff=cutoff.isoformat())
            result["warnings"] = (["wide_prediction_interval"]
                                  if model["metadata"]["interval_padding"] > 0.5 else [])
    except Exception as error:
        result.update(status="ERROR", reason=f"{type(error).__name__}: {error}")
        raise
    finally:
        connection.execute("INSERT INTO order_volume_training_runs(dataset_id, cutoff, result) VALUES (%s, %s, %s)",
                           ("mock_orders", cutoff, Jsonb(result)))
        logger.info("Order volume training: status=%s usable=%s/504 reason=%s", result["status"],
                    diagnostics["usable_targets"], result["reason"])
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--cutoff", type=aware_datetime, default=None,
                        help="Latest receipt timestamp allowed for training (ISO with timezone)")
    args = parser.parse_args()
    if not args.database_url:
        parser.error("DATABASE_URL is required")
    with connect(args.database_url) as connection:
        print(train_order_volume(connection, args.cutoff or datetime.now(timezone.utc)))


if __name__ == "__main__":
    main()
