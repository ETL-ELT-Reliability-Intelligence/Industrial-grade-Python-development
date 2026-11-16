"""Contending writers must preserve raw bytes and agree on the winning run."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from services.ingestion.service import IngestionService
from services.ingestion.publisher import InMemoryPublisher
from services.ingestion.sources import SourceBatch
from services.ingestion.storage import LocalRawStore, S3RawStore
from tests.ingestion.test_storage import FakeS3Client


@pytest.fixture(params=["local", "s3"])
def store(request, tmp_path):
    return LocalRawStore(tmp_path) if request.param == "local" else S3RawStore(FakeS3Client(), "raw")


def test_raw_and_manifest_are_immutable(store):
    store.save_raw("orders", "batch", [{"id": 1}])
    with pytest.raises(FileExistsError):
        store.save_raw("orders", "batch", [{"id": 2}])
    assert store.load_raw("orders", "batch") == [{"id": 1}]
    store.save_run("orders", "run", ["first"])
    store.save_run("orders", "run", ["second"])
    assert store.load_run("orders", "run") == ["first"]


@pytest.mark.parametrize("same_run", [True, False])
def test_concurrent_ingestion_agrees_on_manifest_without_replacing_raw(store, same_run):
    barrier = Barrier(8)
    publisher = InMemoryPublisher()

    def ingest(index):
        class Source:
            dataset_id = "orders"

            def fetch(self):
                barrier.wait(timeout=10)
                return SourceBatch(self.dataset_id, [{"id": index}])

        return IngestionService(store, publisher).ingest(Source(), run_id="same" if same_run else str(index))

    with ThreadPoolExecutor(max_workers=8) as pool:
        events = list(pool.map(ingest, range(8)))
    if same_run:
        assert all(event == events[0] for event in events)
        assert len({event.event_id for event in publisher.events}) == 1
    else:
        assert len({event.batch_id for event in events}) == 8
        for index, event in enumerate(events):
            assert store.load_raw("orders", event.batch_id) == [{"id": index}]


def test_concurrent_schema_registration_preserves_every_version(store):
    with ThreadPoolExecutor(max_workers=8) as pool:
        versions = list(pool.map(lambda i: store.schema_version("orders", str(i))[0], range(8)))
    assert set(versions) == set(range(1, 9))
    assert [store.schema_version("orders", str(i))[0] for i in range(8)] == versions
