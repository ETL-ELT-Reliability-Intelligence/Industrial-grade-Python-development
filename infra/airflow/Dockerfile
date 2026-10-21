# Airflow with everything the DAGs need baked in, so containers start fast and need no pip at runtime.
FROM apache/airflow:2.10.2-python3.11

RUN pip install --no-cache-dir "pydantic>=2.9" "confluent-kafka>=2.5" "boto3>=1.34" "psycopg[binary]>=3.2,<4"

# dbt lives in its own venv: next to Airflow it breaks Airflow's pinned dependencies (protobuf).
RUN python -m venv /home/airflow/dbt-venv && /home/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-postgres
