"""Verify the independent DAG's schedule and logical cutoff without Airflow."""

from contextlib import nullcontext
from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import sys
from types import ModuleType
from unittest.mock import Mock


def test_daily_dag_runs_independently_at_one_utc(monkeypatch):
    tasks = {}
    options = {}
    cutoff = datetime(2026, 11, 9, 1, tzinfo=timezone.utc)

    def task(function):
        tasks[function.__name__] = function
        return lambda: None

    def dag(**kwargs):
        options.update(kwargs)
        return lambda function: function

    for name in ("airflow", "airflow.decorators", "airflow.operators", "airflow.operators.python"):
        monkeypatch.setitem(sys.modules, name, ModuleType(name))
    monkeypatch.setattr(sys.modules["airflow.decorators"], "dag", dag, raising=False)
    monkeypatch.setattr(sys.modules["airflow.decorators"], "task", task, raising=False)
    monkeypatch.setattr(sys.modules["airflow.operators.python"], "get_current_context",
                        lambda: {"data_interval_end": cutoff}, raising=False)
    path = Path("pipelines/airflow/dags/orders_daily_quality.py")
    spec = importlib.util.spec_from_file_location("daily_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    connection = object()
    monkeypatch.setenv("DATABASE_URL", "test-dsn")
    connect = Mock(return_value=nullcontext(connection))
    run = Mock(return_value={"status": "WARN"})
    monkeypatch.setattr(module, "connect", connect)
    monkeypatch.setattr(module, "run_daily_orders", run)
    assert tasks["check_previous_day"]() == {"status": "WARN"}
    run.assert_called_once_with(connection, cutoff)
    connect.assert_called_once_with("test-dsn")
    assert options["schedule"] == "0 1 * * *"
    assert options["start_date"].tzinfo == timezone.utc
