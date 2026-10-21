from datetime import datetime, timezone

import pytest

from contracts.events import DatasetIngestedEvent, Event, PipelineStartedEvent
from services.stream_processor.__main__ import build_parser
from services.stream_processor.consumer import (
    DEFAULT_TOPICS,
    StreamProcessorConfig,
    StreamProcessorRunner,
)
from tests.stream_processor.test_handlers import make_handler, write_batch

NOW = datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc)


class FakeConsumer:
    def __init__(self, events: list[Event | None]) -> None:
        self._events = list(events)
        self.commits = 0
        self.closed = False

    def poll(self, timeout: float = 1.0) -> Event | None:
        return self._events.pop(0) if self._events else None

    def commit(self) -> None:
        self.commits += 1

    def close(self) -> None:
        self.closed = True


def test_runner_processes_events_and_commits_offsets(tmp_path):
    handler, datasets, pipelines, _, _, _ = make_handler()
    uri = write_batch(tmp_path, "b1", [{"id": 1}])
    ingested = DatasetIngestedEvent(
        dataset_id="orders", batch_id="b1", records=1,
        received_at=NOW, schema_version=1, raw_uri=uri,
    )
    started = PipelineStartedEvent(pipeline_id="orders_daily", run_id="r1", started_at=NOW)
    consumer = FakeConsumer([None, ingested, started])
    runner = StreamProcessorRunner(consumer, handler)

    processed = runner.run(max_messages=2)
    runner.close()

    assert processed == 2
    assert consumer.commits == 2
    assert consumer.closed is True
    assert datasets.get_batch("orders", "b1") is not None
    assert pipelines.get_pipeline_run("orders_daily", "r1") is not None


def test_offset_is_not_committed_when_handler_raises(tmp_path):
    handler, _, _, _, _, _ = make_handler()
    bad_event = DatasetIngestedEvent(
        dataset_id="orders",
        batch_id="b-missing",
        records=1,
        received_at=NOW,
        schema_version=1,
        raw_uri=(tmp_path / "missing.jsonl").as_uri(),
    )
    consumer = FakeConsumer([bad_event])
    runner = StreamProcessorRunner(consumer, handler)

    with pytest.raises(Exception):
        runner.step()

    assert consumer.commits == 0


def test_config_reads_env_and_requires_database_url():
    with pytest.raises(ValueError, match="DATABASE_URL"):
        StreamProcessorConfig.from_env({})

    config = StreamProcessorConfig.from_env({
        "DATABASE_URL": "postgresql://u:p@localhost:5432/db",
        "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
        "KAFKA_GROUP_ID": "custom-group",
    })
    assert config.database_url == "postgresql://u:p@localhost:5432/db"
    assert config.kafka_bootstrap_servers == "broker:9092"
    assert config.group_id == "custom-group"
    assert config.topics == DEFAULT_TOPICS


def test_cli_parser_accepts_expected_flags():
    parser = build_parser()
    args = parser.parse_args(["--kafka", "localhost:9092", "--max-messages", "5"])
    assert args.kafka == "localhost:9092"
    assert args.max_messages == 5


def test_runner_rejects_non_positive_max_messages():
    handler, _, _, _, _, _ = make_handler()
    runner = StreamProcessorRunner(FakeConsumer([]), handler)
    with pytest.raises(ValueError, match="max_messages"):
        runner.run(max_messages=0)
