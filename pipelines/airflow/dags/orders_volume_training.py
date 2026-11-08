"""Refresh lagged boosting once a day from the source's actual history."""

from datetime import datetime, timedelta, timezone
import os

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from services.ml.train_order_volume import train_order_volume
from storage.postgres import connect


@dag(
    dag_id="orders_volume_training", schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc), catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)}, tags=["quality", "ml"],
)
def orders_volume_training():
    @task
    def train_model() -> dict:
        with connect(os.environ["DATABASE_URL"]) as connection:
            return train_order_volume(connection, get_current_context()["data_interval_end"])

    train_model()


orders_volume_training()
