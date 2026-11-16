"""Run source ingestion from the command line.

Use python -m services.ingestion --help for source, Kafka and S3 options.
"""

import argparse
from pathlib import Path

from .publisher import InMemoryPublisher, KafkaPublisher
from .service import IngestionService
from .sources import MockApiSource, OlistSource, Source
from .storage import LocalRawStore, RawStore, S3RawStore


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=("mock", "olist"), default="mock")
    parser.add_argument("--run-id", help="retrying the same run republishes its events instead of creating a new batch")
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--volume-factor", type=float, default=1.0)
    parser.add_argument("--olist-dir", type=Path, help="folder with the Olist CSV files (required for --source olist)")
    parser.add_argument("--table", choices=sorted(OlistSource.FILES), default="orders")
    parser.add_argument("--window", type=int, default=0, help="month index of the Olist dataset")
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--kafka", help="bootstrap servers; omit to only print the event")
    parser.add_argument("--s3-endpoint", help="S3/MinIO endpoint, e.g. http://localhost:9000; omit to use --raw-dir")
    parser.add_argument("--s3-bucket", default="raw-data")
    parser.add_argument("--s3-access-key", default="minioadmin")
    parser.add_argument("--s3-secret-key", default="minioadmin")
    return parser


def build_source(args: argparse.Namespace) -> Source:
    if args.source == "olist":
        if args.olist_dir is None:
            raise SystemExit("--olist-dir is required for --source olist")
        return OlistSource(args.table, args.olist_dir, window=args.window, volume_factor=args.volume_factor)
    return MockApiSource(args.rows, args.volume_factor)


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    source = build_source(args)

    publisher = KafkaPublisher(args.kafka) if args.kafka else InMemoryPublisher()
    store: RawStore = (
        S3RawStore.from_endpoint(args.s3_endpoint, args.s3_bucket, args.s3_access_key, args.s3_secret_key)
        if args.s3_endpoint
        else LocalRawStore(args.raw_dir)
    )
    event = IngestionService(store, publisher).ingest(source, run_id=args.run_id)
    print(event.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
