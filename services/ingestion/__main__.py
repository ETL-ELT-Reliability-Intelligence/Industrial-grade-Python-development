"""Usage: python -m services.ingestion [--rows N] [--volume-factor F] [--kafka HOST:PORT]"""

import argparse
from pathlib import Path

from .publisher import InMemoryPublisher, KafkaPublisher
from .service import IngestionService
from .sources import MockApiSource
from .storage import LocalRawStore


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--volume-factor", type=float, default=1.0)
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--kafka", help="bootstrap servers; omit to only print the event")
    args = parser.parse_args()

    publisher = KafkaPublisher(args.kafka) if args.kafka else InMemoryPublisher()
    service = IngestionService(LocalRawStore(args.raw_dir), publisher)
    event = service.ingest(MockApiSource(args.rows, args.volume_factor))
    print(event.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
