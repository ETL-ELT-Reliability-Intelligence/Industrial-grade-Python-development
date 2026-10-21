"""Wiring of the ingestion pieces from environment variables, shared by the Airflow DAGs.

Airflow-independent. KAFKA_BOOTSTRAP_SERVERS unset -> events stay in memory; S3_ENDPOINT_URL unset ->
raw data goes to the local RAW_DATA_DIR.
"""

import os

from services.ingestion.publisher import EventPublisher, InMemoryPublisher, KafkaPublisher
from services.ingestion.storage import RawStore, store_from_env


def make_publisher() -> EventPublisher:
    servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    return KafkaPublisher(servers) if servers else InMemoryPublisher()


def make_store() -> RawStore:
    return store_from_env()
