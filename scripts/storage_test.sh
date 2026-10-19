#!/usr/bin/env bash
# Run the whole test suite, including PostgreSQL integration tests.
# Requires a running database: bash scripts/storage_setup.sh
set -euo pipefail
cd "$(dirname "$0")/.."

export TEST_DATABASE_URL="${TEST_DATABASE_URL:-postgresql://reliability:reliability@localhost:5432/reliability_test}"
python -m pytest -q
