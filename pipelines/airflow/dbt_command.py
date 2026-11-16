"""Build and return the shell command for dbt run + dbt test."""


def dbt_command(project_dir: str, venv: str, work_dir: str = "/tmp/dbt") -> str:
    """Return a shell command that bootstraps the dbt venv, runs dbt run, then dbt test.

    The repository is bind-mounted into the Airflow container and may belong to another user
    (for example the CI runner), so dbt must not write its logs and target there.

    Args:
        project_dir: dbt project and profiles directory.
        venv: Virtual environment that contains dbt.
        work_dir: Writable directory for dbt logs and compiled artifacts.

    Returns:
        A shell command for BashOperator.
    """
    paths = f"--project-dir {project_dir} --profiles-dir {project_dir}"
    output = f"--log-path {work_dir}/logs --target-path {work_dir}/target"
    return (
        f"[ -x {venv}/bin/dbt ] || (python -m venv {venv} && {venv}/bin/pip install -q dbt-postgres) "
        f"&& {venv}/bin/dbt run {paths} {output} "
        f"&& {venv}/bin/dbt test {paths} {output}"
    )
