#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

test_db="${TEST_DB-orders_test}"
if [[ ! "${test_db}" =~ ^[a-z0-9_]+_test$ ]]; then
  echo "TEST_DB must be lowercase letters, digits or _ and end in _test, got '${test_db}'" >&2
  exit 2
fi
export DATABASE_URL="postgresql+asyncpg://orders:orders@localhost:55432/${test_db}"

test_valkey_db="${TEST_VALKEY_DB:-15}"
if [[ ! "${test_valkey_db}" =~ ^([1-9]|1[0-5])$ ]]; then
  echo "TEST_VALKEY_DB must be an integer from 1 to 15, got '${test_valkey_db}'" >&2
  exit 2
fi
export VALKEY_URL="redis://localhost:56379/${test_valkey_db}"
export RELAY_ENABLED=false

docker compose up -d --wait

exists=$(docker compose exec -T postgres psql -U orders -d orders -tAc \
  "SELECT 1 FROM pg_database WHERE datname = '${test_db}'")
if [[ "${exists}" != "1" ]]; then
  docker compose exec -T postgres createdb -U orders "${test_db}"
fi

poetry run alembic upgrade head
poetry run pytest "$@"
