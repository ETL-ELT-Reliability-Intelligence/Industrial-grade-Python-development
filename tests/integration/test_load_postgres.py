"""raw store -> PostgreSQL against a real database (TEST_DATABASE_URL, a dedicated test database)."""

import os
import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from pipelines.airflow.load_postgres import load_batch, load_dataset_batch  # noqa: E402
from services.ingestion.storage import LocalRawStore  # noqa: E402

URL = os.environ.get("TEST_DATABASE_URL")
pytestmark = [pytest.mark.integration, pytest.mark.skipif(not URL, reason="TEST_DATABASE_URL not set")]


@pytest.fixture
def connection():
    conn = psycopg.connect(URL, autocommit=True)
    schema = f"raw_test_{uuid.uuid4().hex[:8]}"
    conn.schema = schema
    yield conn
    conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    conn.close()


def rows(conn, table, order="id"):
    return conn.execute(f'SELECT _batch_id, id, name FROM "{conn.schema}"."{table}" ORDER BY {order}').fetchall()


def test_batch_is_loaded_as_text_with_nulls(connection):
    n = load_batch(
        connection, "orders", "b1", [{"id": 1, "name": "a"}, {"id": 2, "name": None}], schema=connection.schema
    )
    assert n == 2
    assert rows(connection, "orders") == [("b1", "1", "a"), ("b1", "2", None)]


def test_reloading_a_batch_replaces_it_and_keeps_other_batches(connection):
    load_batch(connection, "orders", "b1", [{"id": 1, "name": "a"}], schema=connection.schema)
    load_batch(connection, "orders", "b2", [{"id": 2, "name": "b"}], schema=connection.schema)
    load_batch(connection, "orders", "b1", [{"id": 1, "name": "a2"}, {"id": 3, "name": "c"}], schema=connection.schema)

    assert rows(connection, "orders", order="_batch_id, id") == [
        ("b1", "1", "a2"),
        ("b1", "3", "c"),
        ("b2", "2", "b"),
    ]


def test_new_source_columns_are_added(connection):
    load_batch(connection, "orders", "b1", [{"id": 1, "name": "a"}], schema=connection.schema)
    load_batch(connection, "orders", "b2", [{"id": 2, "name": "b", "extra": "x"}], schema=connection.schema)

    extra = connection.execute(
        f'SELECT _batch_id, extra FROM "{connection.schema}"."orders" ORDER BY _batch_id'
    ).fetchall()
    assert extra == [("b1", None), ("b2", "x")]


def test_empty_batch_clears_previous_rows(connection):
    load_batch(connection, "orders", "b1", [{"id": 1, "name": "a"}], schema=connection.schema)
    assert load_batch(connection, "orders", "b1", [], schema=connection.schema) == 0
    assert rows(connection, "orders") == []


def test_load_dataset_batch_reads_from_the_raw_store(connection, tmp_path):
    store = LocalRawStore(tmp_path)
    store.save_raw("orders", "b1", [{"id": 1, "name": "a"}])

    assert load_dataset_batch(connection, store, "orders", "b1", schema=connection.schema) == 1
    assert rows(connection, "orders") == [("b1", "1", "a")]
