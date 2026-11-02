"""Build and return the shell command for dbt run + dbt test."""


def dbt_command(project_dir: str, venv: str) -> str:
    """Return a shell command that bootstraps the dbt venv, runs dbt run, then dbt test."""
    return (
        f"[ -x {venv}/bin/dbt ] || (python -m venv {venv} && {venv}/bin/pip install -q dbt-postgres) "
        f"&& {venv}/bin/dbt run --project-dir {project_dir} --profiles-dir {project_dir} "
        f"&& {venv}/bin/dbt test --project-dir {project_dir} --profiles-dir {project_dir}"
    )
