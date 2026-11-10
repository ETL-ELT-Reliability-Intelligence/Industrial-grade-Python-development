"""Run the Kafka stream processor from the command line.

Use python -m services.stream_processor --help for runtime options.
"""

import argparse
from pathlib import Path

from storage.postgres import (
    PostgresAnomalyRepository,
    PostgresDatasetRepository,
    PostgresMetricRepository,
    PostgresPipelineRunRepository,
    PostgresQualityRepository,
    connect,
)

from .anomaly import DEFAULT_ANOMALY_CONFIG_PATH, load_anomaly_config
from .consumer import StreamProcessorConfig, StreamProcessorRunner, create_kafka_consumer
from .handlers import StreamEventHandler
from data_quality.config import load_suites
from .publisher import KafkaPublisher
from .feature_accumulation import load_feature_definitions
from .order_volume import OrderVolumeMonitor
from storage.postgres.order_volume import PostgresOrderVolumeRepository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stream processor for dataset and pipeline events")
    parser.add_argument("--database-url", help="PostgreSQL DSN (defaults to DATABASE_URL)")
    parser.add_argument("--kafka", help="Kafka bootstrap servers (defaults to KAFKA_BOOTSTRAP_SERVERS or localhost:9092)")
    parser.add_argument("--group-id", help="Kafka consumer group ID (defaults to KAFKA_GROUP_ID or stream-processor)")
    parser.add_argument("--max-messages", type=int, help="Stop after processing N valid events")
    parser.add_argument("--quality-config", type=Path, default=Path(__file__).resolve().parents[2] / "config/quality")
    parser.add_argument("--anomaly-config", type=Path, default=DEFAULT_ANOMALY_CONFIG_PATH)
    parser.add_argument("--feature-config", type=Path, help="Independent feature definitions (JSON)")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = StreamProcessorConfig.from_env(
        database_url=args.database_url,
        kafka_bootstrap_servers=args.kafka,
        group_id=args.group_id,
    )
    suites = load_suites(args.quality_config)
    anomaly_config = (
        load_anomaly_config(args.anomaly_config)
        if args.anomaly_config.exists()
        else None
    )

    with connect(config.database_url) as connection:
        metrics = PostgresMetricRepository(connection)
        anomalies = PostgresAnomalyRepository(connection)
        publisher = KafkaPublisher(config.kafka_bootstrap_servers)
        handler = StreamEventHandler(
            datasets=PostgresDatasetRepository(connection),
            pipelines=PostgresPipelineRunRepository(connection),
            metrics=metrics,
            quality=PostgresQualityRepository(connection),
            publisher=publisher,
            suites=suites,
            anomalies=anomalies,
            anomaly_config=anomaly_config,
            feature_definitions=load_feature_definitions(args.feature_config),
            order_volume_monitor=OrderVolumeMonitor(
                PostgresOrderVolumeRepository(connection), metrics, anomalies, publisher,
            ),
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
