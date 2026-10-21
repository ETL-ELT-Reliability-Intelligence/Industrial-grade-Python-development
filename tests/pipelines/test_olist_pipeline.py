import pytest

from contracts.events import DatasetIngestedEvent
from pipelines.airflow.olist_pipeline import TABLES, ingest_table
from services.ingestion.olist_sample import write_sample
from services.ingestion.publisher import InMemoryPublisher
from services.ingestion.storage import LocalRawStore


@pytest.fixture
def env(tmp_path):
    olist = tmp_path / "olist"
    write_sample(olist, months=3, orders_per_month=40)
    return olist, LocalRawStore(tmp_path / "raw"), InMemoryPublisher()


def run(env, table, run_id="r1", window=0, volume_factors=None):
    olist, store, publisher = env
    return ingest_table(
        table, run_id=run_id, window=window, volume_factors=volume_factors or {},
        olist_dir=olist, store=store, publisher=publisher,
    )


def test_each_table_becomes_its_own_dataset(env):
    results = [run(env, table) for table in TABLES]

    assert [r["dataset_id"] for r in results] == list(TABLES)
    events = env[2].events
    assert [type(e) for e in events] == [DatasetIngestedEvent] * len(TABLES)
    assert results[0]["records"] == 40  # orders of the first month


def test_partial_load_of_one_table_is_visible_only_there(env):
    full = run(env, "order_payments", run_id="r1")
    partial = run(env, "order_payments", run_id="r2", volume_factors={"order_payments": 0.4})
    untouched = run(env, "orders", run_id="r2", volume_factors={"order_payments": 0.4})

    assert partial["records"] == int(full["records"] * 0.4)
    assert untouched["records"] == 40


def test_task_retry_with_same_run_id_keeps_one_batch(env):
    first = run(env, "orders", run_id="manual__1")
    retry = run(env, "orders", run_id="manual__1")

    assert retry["batch_id"] == first["batch_id"]
    assert len(list((env[1]._root / "orders").glob("*.jsonl"))) == 1


def test_window_selects_the_month(env):
    assert run(env, "orders", run_id="a", window=0)["batch_id"] != run(env, "orders", run_id="b", window=1)["batch_id"]
