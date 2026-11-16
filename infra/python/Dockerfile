FROM python:3.11-slim@sha256:07d4824036af339fa1b6ef769cc99d9c10a141769538b52b3b597c84f7a7b535
WORKDIR /app
COPY infra/python/requirements.lock /tmp/requirements.lock
RUN pip install --no-cache-dir --require-hashes -r /tmp/requirements.lock && pip check
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1 OMP_NUM_THREADS=2
COPY . /app
