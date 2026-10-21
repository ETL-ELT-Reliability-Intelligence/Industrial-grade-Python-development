"""Olist ELT DAG: ingest every table -> load raw tables into PostgreSQL -> dbt run.

Triggered manually with a config, e.g. {"window": 2, "volume_factors": {"order_payments": 0.4}}:
`window` picks the month of the dataset, `volume_factors` simulates an upstream that silently returns
only part of a table. Lifecycle events (pipeline.started/finished/failed) go to Kafka like in the
orders_ingestion DAG.

Env: KAFKA_BOOTSTRAP_SERVERS, S3_* (see orders_ingestion), OLIST_DIR, DATABASE_URL (the warehouse),
DBT_PROJECT_DIR. The repo root must be on PYTHONPATH (docker-compose does this).
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context

from pipelines.airflow.config import make_publisher as _publisher
from pipelines.airflow.config import make_store as _store
from pipelines.airflow.load_postgres import load_dataset_batch
from pipelines.airflow.olist_pipeline import TABLES, ingest_table
from pipelines.airflow.pipeline_events import PipelineRunReporter

PIPELINE_ID = "olist_ingestion"
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/repo/pipelines/dbt")
# dbt lives in its own venv (installing it next to Airflow breaks Airflow's pinned dependencies). The image
# built from infra/airflow already contains it; the fallback creates it on the first run for other setups.
DBT_VENV = os.environ.get("DBT_VENV", "/home/airflow/dbt-venv")
DBT_COMMAND = (
    f"[ -x {DBT_VENV}/bin/dbt ] || (python -m venv {DBT_VENV} && {DBT_VENV}/bin/pip install -q dbt-postgres) "
    f"&& {DBT_VENV}/bin/dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"
)


def _on_failure(context) -> None:
    PipelineRunReporter(_publisher()).failed(
        PIPELINE_ID, context["run_id"], str(context.get("exception")), started_at=context["dag_run"].start_date
    )


@dag(
    dag_id=PIPELINE_ID,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={"window": 0, "volume_factors": {}},
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30), "on_failure_callback": _on_failure},
    tags=["ingestion", "olist"],
)
def olist_ingestion():
    @task
    def report_started() -> None:
        context = get_current_context()
        PipelineRunReporter(_publisher()).started(
            PIPELINE_ID, context["run_id"], started_at=context["dag_run"].start_date
        )

    @task
    def ingest(table: str) -> dict:
        context = get_current_context()
        return ingest_table(
            table,
            run_id=context["run_id"],
            window=int(context["params"]["window"]),
            volume_factors=context["params"]["volume_factors"],
            olist_dir=Path(os.environ.get("OLIST_DIR", "/data/olist")),
            store=_store(),
            publisher=_publisher(),
        )

    @task
    def load_to_postgres(ingested: dict) -> int:
        import psycopg

        with psycopg.connect(os.environ["DATABASE_URL"], autocommit=True) as connection:
            return load_dataset_batch(connection, _store(), ingested["dataset_id"], ingested["batch_id"])

    dbt_run = BashOperator(task_id="dbt_run", bash_command=DBT_COMMAND)

    @task
    def report_finished() -> None:
        context = get_current_context()
        PipelineRunReporter(_publisher()).finished(PIPELINE_ID, context["run_id"], context["dag_run"].start_date)

    ingested = ingest.expand(table=list(TABLES))
    loaded = load_to_postgres.expand(ingested=ingested)
    report_started() >> ingested
    loaded >> dbt_run >> report_finished()


olist_ingestion()
