# Airflow with everything the DAGs need baked in, so containers start fast and need no pip at runtime.
FROM apache/airflow:2.10.2-python3.11@sha256:7c68d8ac3cf1b2c69eeb91da64c1578dc8376d33abf34267bf6e547d071cae39

COPY infra/airflow/requirements.lock /tmp/requirements.lock
RUN python -m venv /home/airflow/runtime
ENV PATH="/home/airflow/runtime/bin:${PATH}"
RUN pip install --no-cache-dir --require-hashes -r /tmp/requirements.lock && pip check

# dbt lives in its own venv: next to Airflow it breaks Airflow's pinned dependencies (protobuf).
COPY infra/airflow/dbt.lock /tmp/dbt.lock
RUN python -m venv /home/airflow/dbt-venv && /home/airflow/dbt-venv/bin/pip install --no-cache-dir --require-hashes -r /tmp/dbt.lock && /home/airflow/dbt-venv/bin/pip check
ENV OMP_NUM_THREADS=2
