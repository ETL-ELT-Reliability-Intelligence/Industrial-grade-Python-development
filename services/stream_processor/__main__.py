"""Usage: python -m services.stream_processor [--kafka HOST:PORT] [--database-url DSN] [--group-id ID] [--max-messages N]"""

import argparse

from storage.postgres import (
    PostgresDatasetRepository,
    PostgresMetricRepository,
    PostgresPipelineRunRepository,
    PostgresQualityRepository,
    connect,
)

from .consumer import StreamProcessorConfig, StreamProcessorRunner, create_kafka_consumer
from .handlers import StreamEventHandler
from .policy import QualityThresholds
from .publisher import KafkaPublisher


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stream processor for dataset and pipeline events")
    parser.add_argument("--database-url", help="PostgreSQL DSN (defaults to DATABASE_URL)")
    parser.add_argument("--kafka", help="Kafka bootstrap servers (defaults to KAFKA_BOOTSTRAP_SERVERS or localhost:9092)")
    parser.add_argument("--group-id", help="Kafka consumer group ID (defaults to KAFKA_GROUP_ID or stream-processor)")
    parser.add_argument("--max-messages", type=int, help="Stop after processing N valid events")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = StreamProcessorConfig.from_env(
        database_url=args.database_url,
        kafka_bootstrap_servers=args.kafka,
        group_id=args.group_id,
    )
    thresholds = QualityThresholds.from_env()

    with connect(config.database_url) as connection:
        handler = StreamEventHandler(
            datasets=PostgresDatasetRepository(connection),
            pipelines=PostgresPipelineRunRepository(connection),
            metrics=PostgresMetricRepository(connection),
            quality=PostgresQualityRepository(connection),
            publisher=KafkaPublisher(config.kafka_bootstrap_servers),
            thresholds=thresholds,
        )
        consumer = create_kafka_consumer(config)
        runner = StreamProcessorRunner(consumer, handler, poll_timeout=config.poll_timeout)
        try:
            return runner.run(max_messages=args.max_messages)
        except KeyboardInterrupt:
            return 0
        finally:
            runner.close()


if __name__ == "__main__":
    main()
