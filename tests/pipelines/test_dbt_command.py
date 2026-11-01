"""Test dbt_command builds a correct shell string without importing Airflow."""

from pipelines.airflow.dbt_command import dbt_command


def test_dbt_command_contains_both_steps():
    cmd = dbt_command("/some/project", "/some/venv")
    assert "dbt run" in cmd
    assert "dbt test" in cmd


def test_dbt_command_contains_paths():
    cmd = dbt_command("/some/project", "/some/venv")
    assert "--project-dir /some/project" in cmd
    assert "--profiles-dir /some/project" in cmd


def test_dbt_command_uses_same_venv_for_both_steps():
    cmd = dbt_command("/some/project", "/some/venv")
    parts = cmd.split("&&")
    run_step = parts[1].strip()
    test_step = parts[2].strip()
    assert run_step.startswith("/some/venv/bin/dbt run")
    assert test_step.startswith("/some/venv/bin/dbt test")
