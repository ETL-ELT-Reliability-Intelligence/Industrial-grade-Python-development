"""Bounded authenticated S3 readiness and idempotent Kafka topic creation."""

import os
import time
from uuid import uuid4

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError, ConnectionClosedError, ReadTimeoutError
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException, KafkaError

from contracts.events import EventType


def main():
    client = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT_URL"],
                          aws_access_key_id=os.environ["S3_ACCESS_KEY"],
                          aws_secret_access_key=os.environ["S3_SECRET_KEY"],
                          config=Config(connect_timeout=3, read_timeout=3, retries={"max_attempts": 0}))
    bucket = os.environ["S3_BUCKET"]
    deadline = time.monotonic() + 120
    while True:
        try:
            try:
                client.head_bucket(Bucket=bucket)
            except ClientError as error:
                if error.response["Error"]["Code"] not in {"404", "NoSuchBucket"}:
                    raise
                client.create_bucket(Bucket=bucket)
            # Verify actual access and conditional writes, not just an HTTP listener.
            key = f"_readiness/{uuid4().hex}"
            client.put_object(Bucket=bucket, Key=key, Body=b"ready", IfNoneMatch="*")
            try:
                client.put_object(Bucket=bucket, Key=key, Body=b"must-not-overwrite", IfNoneMatch="*")
            except ClientError as error:
                if error.response["Error"]["Code"] not in {"PreconditionFailed", "412"}:
                    raise
            else:
                raise RuntimeError("S3 server does not enforce conditional creation")
            if client.get_object(Bucket=bucket, Key=key)["Body"].read() != b"ready":
                raise RuntimeError("S3 read-after-write failed")
            client.delete_object(Bucket=bucket, Key=key)
            break
        except (EndpointConnectionError, ConnectionClosedError, ReadTimeoutError):
            if time.monotonic() >= deadline:
                raise TimeoutError("S3 readiness timeout")
            time.sleep(2)
    admin = AdminClient({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]})
    futures = admin.create_topics([NewTopic(t.value, num_partitions=1, replication_factor=1) for t in EventType],
                                 request_timeout=30)
    for topic, future in futures.items():
        try:
            future.result(timeout=35)
        except KafkaException as error:
            if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
        print(f"Ready: {topic}", flush=True)
    metadata = admin.list_topics(timeout=15)
    if any(t.value not in metadata.topics or metadata.topics[t.value].error for t in EventType):
        raise RuntimeError("Kafka topics are not ready")


if __name__ == "__main__":
    main()
