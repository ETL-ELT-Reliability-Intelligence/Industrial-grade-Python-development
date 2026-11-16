"""Publish a unique S3-backed event without requiring a live database."""

import os
from uuid import uuid4

from services.ingestion.publisher import KafkaPublisher
from services.ingestion.service import IngestionService
from services.ingestion.sources import SourceBatch
from services.ingestion.storage import store_from_env


class Source:
    def __init__(self):
        self.dataset_id = f"outage-{uuid4().hex}"

    def fetch(self):
        return SourceBatch(self.dataset_id, [{"id": 1}, {"id": 2}])


if __name__ == "__main__":
    service = IngestionService(store_from_env(), KafkaPublisher(os.environ["KAFKA_BOOTSTRAP_SERVERS"]))
    print(service.ingest(Source(), run_id="outage-test").model_dump_json())
