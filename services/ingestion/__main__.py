"""Usage: python -m services.ingestion [--rows N] [--volume-factor F] [--kafka HOST:PORT] [--s3-endpoint URL --s3-bucket B]"""

import argparse
from pathlib import Path

from .publisher import InMemoryPublisher, KafkaPublisher
from .service import IngestionService
from .sources import MockApiSource
from .storage import LocalRawStore, RawStore, S3RawStore


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--volume-factor", type=float, default=1.0)
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--kafka", help="bootstrap servers; omit to only print the event")
    parser.add_argument("--s3-endpoint", help="S3/MinIO endpoint, e.g. http://localhost:9000; omit to use --raw-dir")
    parser.add_argument("--s3-bucket", default="raw-data")
    parser.add_argument("--s3-access-key", default="minioadmin")
    parser.add_argument("--s3-secret-key", default="minioadmin")
    args = parser.parse_args()

    publisher = KafkaPublisher(args.kafka) if args.kafka else InMemoryPublisher()
    store: RawStore = (
        S3RawStore.from_endpoint(args.s3_endpoint, args.s3_bucket, args.s3_access_key, args.s3_secret_key)
        if args.s3_endpoint
        else LocalRawStore(args.raw_dir)
    )
    service = IngestionService(store, publisher)
    event = service.ingest(MockApiSource(args.rows, args.volume_factor))
    print(event.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
