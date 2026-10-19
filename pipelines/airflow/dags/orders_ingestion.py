"""Orders ingestion DAG: report start, ingest, report finish; failures emit pipeline.failed.

Config via env: KAFKA_BOOTSTRAP_SERVERS (optional), S3_ENDPOINT_URL (optional, else RAW_DATA_DIR),
S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, MOCK_VOLUME_FACTOR.
The repo root must be on PYTHONPATH (docker-compose does this).
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from pipelines.airflow.pipeline_events import PipelineRunReporter
from services.ingestion.publisher import InMemoryPublisher, KafkaPublisher
from services.ingestion.service import IngestionService
from services.ingestion.sources import MockApiSource
from services.ingestion.storage import LocalRawStore, RawStore, S3RawStore

PIPELINE_ID = "orders_ingestion"


def _publisher():
    servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    return KafkaPublisher(servers) if servers else InMemoryPublisher()


def _store() -> RawStore:
    endpoint = os.environ.get("S3_ENDPOINT_URL")
    if endpoint:
        return S3RawStore.from_endpoint(
            endpoint,
            os.environ.get("S3_BUCKET", "raw-data"),
            os.environ.get("S3_ACCESS_KEY", "minioadmin"),
            os.environ.get("S3_SECRET_KEY", "minioadmin"),
        )
    return LocalRawStore(Path(os.environ.get("RAW_DATA_DIR", "/data/raw")))


def _on_failure(context) -> None:
    PipelineRunReporter(_publisher()).failed(
        PIPELINE_ID, context["run_id"], str(context.get("exception")),
        started_at=context["dag_run"].start_date,
    )


@dag(
    dag_id=PIPELINE_ID,
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1), "on_failure_callback": _on_failure},
    tags=["ingestion"],
)
def orders_ingestion():
    @task
    def report_started() -> None:
        context = get_current_context()
        PipelineRunReporter(_publisher()).started(
            PIPELINE_ID, context["run_id"], started_at=context["dag_run"].start_date,
        )

    @task
    def ingest() -> dict:
        factor = float(os.environ.get("MOCK_VOLUME_FACTOR", "1.0"))
        service = IngestionService(_store(), _publisher())
        run_id = get_current_context()["run_id"]  # stable across task retries -> idempotent ingestion
        return service.ingest(MockApiSource(volume_factor=factor), run_id=run_id).model_dump(mode="json")

    @task
    def report_finished(_ingested: dict) -> None:
        context = get_current_context()
        PipelineRunReporter(_publisher()).finished(PIPELINE_ID, context["run_id"], context["dag_run"].start_date)

    report_started() >> report_finished(ingest())


orders_ingestion()
