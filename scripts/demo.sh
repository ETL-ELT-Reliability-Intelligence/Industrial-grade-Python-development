#!/usr/bin/env bash
# End-to-end demo: Olist tables -> ingestion -> S3 + Kafka -> PostgreSQL raw -> dbt -> quality decisions -> dashboard.
#
# Three normal monthly runs build the history, the fourth silently loses 60% of order_payments while every
# pipeline step still succeeds. The quality bridge must catch it: BLOCK on the dashboard, and the revenue
# built by dbt for that month is visibly too low.
#
# Needs only Docker. Real Olist CSVs in data/olist are used if present, otherwise a small synthetic set in
# the same format is generated. POSTGRES_PORT (default 5433) avoids a clash with a local PostgreSQL on 5432.
set -euo pipefail
cd "$(dirname "$0")/.."

export POSTGRES_PORT="${POSTGRES_PORT:-5433}"
PSQL="docker compose exec -T postgres psql -U reliability -d reliability"
AIRFLOW="docker compose exec -T airflow airflow"
STAMP="$(date +%s)"
RUN_TIMEOUT="${RUN_TIMEOUT:-600}"

if [ ! -f data/olist/olist_orders_dataset.csv ]; then
  echo "== data/olist is empty: generating synthetic Olist-format data"
  python -m services.ingestion.olist_sample data/olist --months 4 --orders-per-month 300
fi

echo "== starting infrastructure"
docker compose up -d --wait postgres
docker compose up -d kafka rustfs migrate backend stream-processor kafka-ui adminer airflow airflow-scheduler

echo "== waiting for Airflow (the first start builds the image: a few minutes)"
until docker compose exec -T airflow curl -sf localhost:8080/health >/dev/null 2>&1; do sleep 5; done
until $AIRFLOW dags list 2>/dev/null | grep -q olist_ingestion; do sleep 3; done
$AIRFLOW dags unpause olist_ingestion >/dev/null 2>&1 || true

run() {  # run <label> <run-suffix> <conf-json>: trigger through the scheduler and wait for the result
  local label="$1" run_id="demo-$STAMP-$2" conf="$3" state="" waited=0
  echo "== $label"
  $AIRFLOW dags trigger olist_ingestion --run-id "$run_id" --conf "$conf" >/dev/null 2>&1 \
    || { echo "could not trigger the DAG" >&2; exit 1; }
  while [ "$waited" -lt "$RUN_TIMEOUT" ]; do
    state=$($AIRFLOW dags state olist_ingestion "$run_id" 2>/dev/null | tail -1 | tr -d '\r' || true)
    case "$state" in success|failed) break ;; esac
    sleep 5; waited=$((waited + 5))
  done
  echo "   state=${state:-unknown} (${waited}s)"
  if [ "$state" != "success" ]; then
    echo "DAG run '$label' did not succeed (state='${state:-unknown}'). See http://localhost:8080/dags/olist_ingestion/grid" >&2
    $AIRFLOW tasks states-for-dag-run olist_ingestion "$run_id" 2>&1 | tail -15 >&2 || true
    exit 1
  fi
}

run "month 1 (normal)" 1 '{"window": 0}'
run "month 2 (normal)" 2 '{"window": 1}'
run "month 3 (normal)" 3 '{"window": 2}'
run "month 4: upstream silently returns 40% of order_payments" 4 '{"window": 3, "volume_factors": {"order_payments": 0.4}}'

echo "== waiting for the quality bridge to judge the last batches"
for _ in $(seq 1 30); do
  n=$($PSQL -tAc "select count(*) from quality_decisions where dataset_id = 'order_payments'" 2>/dev/null || echo 0)
  [ "${n:-0}" -ge 4 ] && break
  sleep 2
done

echo
echo "================ S3 (raw data lake) ================"
docker compose exec -T airflow python /opt/airflow/repo/scripts/show_s3.py
echo
echo "================ PostgreSQL ================"
$PSQL < scripts/demo_postgres.sql

cat <<EOF

================ Where to look ================
  Dashboard (quality)  http://localhost:8000                 order_payments, last batch -> BLOCK
  Airflow              http://localhost:8080                 admin / admin   (local demo only)
  Kafka UI             http://localhost:8081                 topics -> dataset.ingested
  PostgreSQL (Adminer) http://localhost:8082/?pgsql=postgres&username=reliability&db=reliability
                                                             system PostgreSQL, user/password reliability
  S3 console (RustFS)  http://localhost:9001                 minioadmin / minioadmin, bucket raw-data
  Stop and remove everything: docker compose down -v
EOF
