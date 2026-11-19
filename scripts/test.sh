#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

test_db=orders_test
export DATABASE_URL="postgresql+asyncpg://orders:orders@localhost:55432/${test_db}"

docker compose up -d --wait

exists=$(docker compose exec -T postgres psql -U orders -d orders -tAc \
  "SELECT 1 FROM pg_database WHERE datname = '${test_db}'")
if [[ "${exists}" != "1" ]]; then
  docker compose exec -T postgres createdb -U orders "${test_db}"
fi

poetry run alembic upgrade head
poetry run pytest "$@"
