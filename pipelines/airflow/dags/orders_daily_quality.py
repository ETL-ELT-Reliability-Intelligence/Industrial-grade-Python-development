"""Independent daily presence check; WARN is reported in task logs and XCom."""

from datetime import datetime, timedelta, timezone
import os

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from services.stream_processor.daily_orders import run_daily_orders
from storage.postgres.connection import connect


@dag(
    dag_id="orders_daily_quality",
    schedule="0 1 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["quality"],
)
def orders_daily_quality():
    @task
    def check_previous_day() -> dict:
        cutoff = get_current_context()["data_interval_end"]
        with connect(os.environ["DATABASE_URL"]) as connection:
            return run_daily_orders(connection, cutoff)

    check_previous_day()


orders_daily_quality()
