"""Disposable consumer process for real-service crash and redelivery tests."""

import argparse
import os
import time

from confluent_kafka import Producer

from contracts.consumer import KafkaEventConsumer
from contracts.quality import CheckKind
from contracts.quality_config import CheckSpec, CheckSuite, RetrySettings
from services.stream_processor.handlers import StreamEventHandler
from services.stream_processor.publisher import KafkaPublisher
from storage.postgres import (
    connect, PostgresDatasetRepository, PostgresPipelineRunRepository,
    PostgresMetricRepository, PostgresQualityRepository,
)


class RecoveringPublisher:
    def __init__(self, bootstrap):
        self.good = KafkaPublisher(bootstrap)
        self.bad = KafkaPublisher(bootstrap)
        self.bad._producer = Producer({"bootstrap.servers": "127.0.0.1:1", "message.timeout.ms": 1000})
        self.calls = 0

    def publish(self, event):
        self.calls += 1
        if self.calls == 1:
            try:
                self.bad.publish(event)
            except (RuntimeError, ConnectionError) as error:
                raise ConnectionError("temporary notification broker outage") from error
            raise AssertionError("unreachable broker unexpectedly accepted a notification")
        self.good.publish(event)
        if self.calls == 2:
            raise ConnectionError("delivery succeeded but acknowledgement was lost")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("topic")
    parser.add_argument("group")
    parser.add_argument("dataset")
    parser.add_argument("mode", choices=["normal", "crash", "disconnect", "publish-failure"])
    args = parser.parse_args()
    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    consumer = KafkaEventConsumer(bootstrap, args.group, [args.topic])
    connection = connect(os.environ["TEST_DATABASE_URL"])
    publisher = RecoveringPublisher(bootstrap) if args.mode == "publish-failure" else KafkaPublisher(bootstrap)
    suite = CheckSuite(dataset_id=args.dataset,
                       checks=(CheckSpec(key="volume", kind=CheckKind.ROW_COUNT, params={"mode": "fixed", "min_count": 100}),),
                       retry=RetrySettings(delays_seconds=(0.0, 0.0)))
    handler = StreamEventHandler(PostgresDatasetRepository(connection), PostgresPipelineRunRepository(connection),
                                 PostgresMetricRepository(connection), PostgresQualityRepository(connection),
                                 publisher=publisher, suites={args.dataset: suite})
    try:
        # A crashed member may retain its assignment until Kafka's session timeout.
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            event = consumer.poll(1)
            if event is None:
                continue
            if args.mode == "disconnect":
                with connect(os.environ["TEST_DATABASE_URL"]) as killer:
                    killer.execute("SELECT pg_terminate_backend(%s)", (connection.info.backend_pid,))
            handler.handle(event)
            if args.mode == "crash":
                os._exit(73)  # durable result exists; no close, commit or finally
            consumer.commit()
            return
        raise TimeoutError("recovery worker received no event")
    finally:
        consumer.close()
        connection.close()


if __name__ == "__main__":
    main()
