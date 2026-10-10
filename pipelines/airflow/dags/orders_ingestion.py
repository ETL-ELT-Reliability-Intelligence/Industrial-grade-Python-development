"""Orders ingestion DAG: report start, ingest, report finish; failures emit pipeline.failed.

Config via env: KAFKA_BOOTSTRAP_SERVERS (optional), RAW_DATA_DIR, MOCK_VOLUME_FACTOR.
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
from services.ingestion.storage import LocalRawStore

PIPELINE_ID = "orders_ingestion"


def _publisher():
    servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    return KafkaPublisher(servers) if servers else InMemoryPublisher()


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
        service = IngestionService(LocalRawStore(Path(os.environ.get("RAW_DATA_DIR", "/data/raw"))), _publisher())
        return service.ingest(MockApiSource(volume_factor=factor)).model_dump(mode="json")

    @task
    def report_finished(_ingested: dict) -> None:
        context = get_current_context()
        PipelineRunReporter(_publisher()).finished(PIPELINE_ID, context["run_id"], context["dag_run"].start_date)

    report_started() >> report_finished(ingest())


orders_ingestion()
