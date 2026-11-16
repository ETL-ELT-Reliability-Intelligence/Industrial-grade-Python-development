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


def test_dbt_command_writes_logs_and_target_outside_the_project():
    # Репозиторий смонтирован в контейнер Airflow и может принадлежать другому пользователю (CI-раннер).
    cmd = dbt_command("/some/project", "/some/venv", work_dir="/scratch")
    assert cmd.count("--log-path /scratch/logs") == 2
    assert cmd.count("--target-path /scratch/target") == 2
    assert "/some/project/logs" not in cmd
    assert "/some/project/target" not in cmd


def test_dbt_command_defaults_to_a_temporary_work_dir():
    cmd = dbt_command("/some/project", "/some/venv")
    assert "--log-path /tmp/dbt/logs" in cmd
    assert "--target-path /tmp/dbt/target" in cmd


def test_dbt_command_uses_same_venv_for_both_steps():
    cmd = dbt_command("/some/project", "/some/venv")
    # Шаг установки содержит вложенный "&&", поэтому проверяем вхождения, а не части строки.
    assert cmd.count("/some/venv/bin/dbt run") == 1
    assert cmd.count("/some/venv/bin/dbt test") == 1
