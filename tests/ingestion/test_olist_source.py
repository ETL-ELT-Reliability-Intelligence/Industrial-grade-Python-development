import pytest

from services.ingestion.olist_sample import write_sample
from services.ingestion.sources import OlistSource


@pytest.fixture(scope="module")
def root(tmp_path_factory):
    path = tmp_path_factory.mktemp("olist")
    write_sample(path, months=3, orders_per_month=50)
    return path


def fetch(root, table, **kwargs):
    return OlistSource(table, root, **kwargs).fetch()


def test_orders_are_cut_by_month(root):
    first, second = fetch(root, "orders", window=0), fetch(root, "orders", window=1)
    assert len(first.records) == len(second.records) == 50
    assert {r["order_purchase_timestamp"][:7] for r in first.records} == {"2018-01"}
    assert {r["order_purchase_timestamp"][:7] for r in second.records} == {"2018-02"}


def test_child_tables_follow_the_orders_of_the_window(root):
    orders = fetch(root, "orders", window=1).records
    order_ids = {r["order_id"] for r in orders}
    for table in ("order_items", "order_payments"):
        records = fetch(root, table, window=1).records
        assert records and {r["order_id"] for r in records} <= order_ids
    assert {r["customer_id"] for r in fetch(root, "customers", window=1).records} == {
        r["customer_id"] for r in orders
    }


def test_empty_cells_become_none(root):
    records = fetch(root, "orders").records
    undelivered = [r for r in records if r["order_status"] != "delivered"]
    assert undelivered and all(r["order_delivered_customer_date"] is None for r in undelivered)


def test_volume_factor_silently_drops_rows(root):
    full, partial = fetch(root, "order_payments"), fetch(root, "order_payments", volume_factor=0.4)
    assert len(partial.records) == int(len(full.records) * 0.4)
    assert partial.dataset_id == "order_payments"


def test_rejects_unknown_table_and_window_out_of_range(root):
    with pytest.raises(ValueError):
        OlistSource("products", root)
    with pytest.raises(ValueError):
        fetch(root, "orders", window=3)
