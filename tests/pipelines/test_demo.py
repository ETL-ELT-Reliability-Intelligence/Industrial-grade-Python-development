"""Airflow failures and transport failures must never become silent waiting."""

import pytest

from scripts.demo import run_state, wait_for


def test_exact_run_identity_and_terminal_states():
    assert run_state({"dag_run_id": "current", "state": "success"}, "current")
    assert run_state({"dag_run_id": "current", "state": "running"}, "current") is None
    for document in ({"dag_run_id": "old", "state": "success"},
                     {"dag_run_id": "current", "state": "failed"},
                     {"dag_run_id": "current", "state": None}):
        with pytest.raises(RuntimeError):
            run_state(document, "current")


def test_wait_has_deadline_and_propagates_request_errors():
    with pytest.raises(TimeoutError, match="quality"):
        wait_for("quality", lambda: None, 0)

    def failed_request():
        raise ConnectionError("API unavailable")

    with pytest.raises(ConnectionError):
        wait_for("DAG", failed_request, 100)
