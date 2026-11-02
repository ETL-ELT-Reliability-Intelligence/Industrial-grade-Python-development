import json

import pytest

from services.ingestion.__main__ import main
from services.ingestion.olist_sample import write_sample


def run(capsys, *argv):
    main(list(argv))
    return json.loads(capsys.readouterr().out)


def test_mock_source_is_the_default(tmp_path, capsys):
    event = run(capsys, "--rows", "7", "--raw-dir", str(tmp_path))
    assert (event["dataset_id"], event["records"]) == ("mock_orders", 7)


def test_olist_source_reports_its_month(tmp_path, capsys):
    olist = tmp_path / "olist"
    write_sample(olist, months=2, orders_per_month=10)
    event = run(
        capsys, "--source", "olist", "--olist-dir", str(olist), "--table", "order_items",
        "--window", "1", "--raw-dir", str(tmp_path / "raw"),
    )
    assert (event["dataset_id"], event["granularity"]) == ("order_items", "month")
    assert event["period_start"].startswith("2018-02-01")


def test_same_run_id_does_not_create_a_second_batch(tmp_path, capsys):
    args = ("--run-id", "run-1", "--raw-dir", str(tmp_path))
    first, second = run(capsys, *args), run(capsys, *args)
    assert first["batch_id"] == second["batch_id"]
    assert len(list((tmp_path / "mock_orders").glob("*.jsonl"))) == 1


def test_olist_requires_its_directory(tmp_path):
    with pytest.raises(SystemExit):
        main(["--source", "olist", "--raw-dir", str(tmp_path)])
