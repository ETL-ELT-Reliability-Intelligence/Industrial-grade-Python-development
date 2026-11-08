"""Verify the independent DAG's schedule and logical cutoff without Airflow."""

from contextlib import nullcontext
from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import sys
from types import ModuleType
from unittest.mock import Mock
import pytest


@pytest.mark.parametrize("filename,callable_name,task_name,hour", [
    ("orders_daily_quality", "run_daily_orders", "check_previous_day", 1),
    ("orders_volume_training", "train_order_volume", "train_model", 2),
])
def test_daily_dag_schedule_and_cutoff(monkeypatch, filename, callable_name, task_name, hour):
    tasks = {}
    options = {}
    cutoff = datetime(2026, 11, 9, hour, tzinfo=timezone.utc)

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
    path = Path(f"pipelines/airflow/dags/{filename}.py")
    spec = importlib.util.spec_from_file_location("daily_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    connection = object()
    monkeypatch.setenv("DATABASE_URL", "test-dsn")
    connect = Mock(return_value=nullcontext(connection))
    run = Mock(return_value={"status": "WARN"})
    monkeypatch.setattr(module, "connect", connect)
    monkeypatch.setattr(module, callable_name, run)
    assert tasks[task_name]() == {"status": "WARN"}
    run.assert_called_once_with(connection, cutoff)
    connect.assert_called_once_with("test-dsn")
    assert options["schedule"] == f"0 {hour} * * *"
    assert options["start_date"].tzinfo == timezone.utc
