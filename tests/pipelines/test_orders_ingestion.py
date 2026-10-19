"""Exercise DAG callables without starting Airflow or external services."""

from datetime import datetime, timedelta, timezone
import importlib.util
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest

from contracts.events import EventType
from pipelines.airflow.pipeline_events import PipelineRunReporter
from services.ingestion.publisher import InMemoryPublisher

START = datetime(2026, 10, 10, tzinfo=timezone.utc)


@pytest.fixture
def dag_callables(monkeypatch):
    tasks = {}
    options = {}
    context = {"run_id": "run-1", "dag_run": SimpleNamespace(start_date=START),
               "exception": RuntimeError("boom")}

    class TaskReference:
        def __rshift__(self, other):
            return other

    def task(function):
        tasks[function.__name__] = function
        return lambda *args, **kwargs: TaskReference()

    def dag(**kwargs):
        options.update(kwargs)
        return lambda function: function

    for name in ("airflow", "airflow.decorators", "airflow.operators", "airflow.operators.python"):
        monkeypatch.setitem(sys.modules, name, ModuleType(name))
    monkeypatch.setattr(sys.modules["airflow.decorators"], "dag", dag, raising=False)
    monkeypatch.setattr(sys.modules["airflow.decorators"], "task", task, raising=False)
    monkeypatch.setattr(sys.modules["airflow.operators.python"], "get_current_context",
                        lambda: context, raising=False)
    path = Path(__file__).resolve().parents[2] / "pipelines/airflow/dags/orders_ingestion.py"
    spec = importlib.util.spec_from_file_location("orders_ingestion_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    publisher = InMemoryPublisher()
    monkeypatch.setattr(module, "_publisher", lambda: publisher)
    clock = Mock(return_value=START + timedelta(minutes=5))
    monkeypatch.setattr(module, "PipelineRunReporter",
                        lambda sink: PipelineRunReporter(sink, clock=clock))
    return tasks, options, context, publisher, clock


@pytest.mark.parametrize("outcome", ["finished", "failed"])
def test_dag_reports_share_actual_run_start(dag_callables, outcome):
    tasks, options, context, publisher, clock = dag_callables
    tasks["report_started"]()
    clock.assert_not_called()
    if outcome == "finished":
        tasks["report_finished"]({})
    else:
        options["default_args"]["on_failure_callback"](context)
    start, end = publisher.events
    assert start.event_type is EventType.PIPELINE_STARTED
    assert end.event_type is (EventType.PIPELINE_FINISHED if outcome == "finished"
                             else EventType.PIPELINE_FAILED)
    assert start.started_at == end.started_at == context["dag_run"].start_date
    assert start.run_id == end.run_id == context["run_id"]
    clock.assert_called_once_with()


def test_retry_configuration_and_repeated_start_report(dag_callables):
    tasks, options, context, publisher, clock = dag_callables
    defaults = options["default_args"]
    assert defaults["retries"] == 1
    assert defaults["retry_delay"] == timedelta(minutes=1)
    assert "on_retry_callback" not in defaults
    tasks["report_started"]()
    clock.return_value += timedelta(minutes=1)
    tasks["report_started"]()
    first, repeated = publisher.events
    assert first.started_at == repeated.started_at == context["dag_run"].start_date
    assert first.run_id == repeated.run_id
    assert first.event_id != repeated.event_id
