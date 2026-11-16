"""Send 5 hourly mock_orders batches to Kafka to verify rule-based anomaly
detection and quality decisions (WARN on volume drop).
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

from services.ingestion.publisher import KafkaPublisher
from services.ingestion.service import IngestionService
from services.ingestion.sources import MockApiSource
from services.ingestion.storage import LocalRawStore


def main() -> None:
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    ts = int(now.timestamp())
    publisher = KafkaPublisher("localhost:9092")
    service = IngestionService(LocalRawStore(Path("data/raw")), publisher)

    for offset in (5, 4, 3, 2):
        start = now - timedelta(hours=offset)
        service.ingest(
            MockApiSource(
                base_rows=100,
                period_start=start,
                period_end=start + timedelta(hours=1),
            ),
            run_id=f"smoke-{ts}-{offset}",
        )

    start = now - timedelta(hours=1)
    service.ingest(
        MockApiSource(base_rows=20, period_start=start, period_end=now),
        run_id=f"smoke-{ts}-drop",
    )


if __name__ == "__main__":
    main()
