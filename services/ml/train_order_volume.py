"""Train and register an order-volume model from persisted source observations."""

import argparse
from datetime import datetime, timedelta, timezone
import logging
import os

from data_quality.dates import aware_datetime
from services.ml.volume import training_rows, train
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
    if not available:
        result = {"status": "SKIPPED", "reason": "insufficient_training_history"}
    else:
        rows = training_rows(available, cutoff)
        if not rows:
            result = {"status": "SKIPPED", "reason": "insufficient_training_history"}
        else:
            model = train(rows, cutoff)
            PostgresOrderVolumeRepository(connection).save_model(model)
            result = {"status": "TRAINED", "model_version": model["model_version"],
                      "cutoff": cutoff.isoformat(), "interval_padding": model["metadata"]["interval_padding"]}
    logger.info("Order volume training: %s", result)
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
