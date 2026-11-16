"""Publish simulated hourly batches after training, for inspecting live ML evidence."""

from datetime import timedelta
import json
import os
from uuid import uuid4

from services.ingestion.publisher import KafkaPublisher
from services.ingestion.service import IngestionService
from services.ingestion.sources import MockApiSource
from services.ingestion.storage import store_from_env
from services.ml.benchmark import CUTOFF, TEST_START, generated_history
from services.ml.train_order_volume import train_order_volume
from services.ml.volume import HOUR
from storage.postgres import connect, PostgresMetricRepository


def main():
    history, labels = generated_history(42)
    with connect(os.environ["DATABASE_URL"]) as connection:
        repository = PostgresMetricRepository(connection)
        for profile in history:
            if profile.event.period_start < TEST_START:
                repository.save_profile(profile)
        trained = train_order_volume(connection, CUTOFF)
        if trained["status"] != "TRAINED":
            raise RuntimeError(trained)
    cases = [("normal", TEST_START, labels[TEST_START]["actual"]),
             ("spike", TEST_START + HOUR, 1200),
             ("missing_history", TEST_START + timedelta(days=30), 100)]
    store = store_from_env()
    publisher = KafkaPublisher(os.environ["KAFKA_BOOTSTRAP_SERVERS"])
    results = []
    for label, hour, count in cases:
        event = IngestionService(store, publisher, clock=lambda: hour + HOUR + timedelta(minutes=5)).ingest(
            MockApiSource(base_rows=count, period_start=hour, period_end=hour + HOUR),
            run_id=f"ui-ml-{label}-{uuid4().hex}",
        )
        results.append({"case": label, "dataset_id": event.dataset_id, "batch_id": event.batch_id})
    print(json.dumps(results))


if __name__ == "__main__":
    main()
