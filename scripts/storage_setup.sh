#!/usr/bin/env bash
# Start PostgreSQL, install dependencies and apply migrations.
# Run from anywhere: bash scripts/storage_setup.sh
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose -f infra/docker-compose.yml up -d --wait postgres
python -m pip install -e '.[dev]' -r storage/requirements.txt

export DATABASE_URL="${DATABASE_URL:-postgresql://reliability:reliability@localhost:5432/reliability}"
python -m alembic -c storage/postgres/alembic.ini upgrade head
echo "Storage is ready: $DATABASE_URL"
