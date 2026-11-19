.PHONY: install up down reset migrate run test lint fmt seed demo-duplicates

install:
	poetry install

up:
	docker compose up -d --wait

down:
	docker compose down

reset:
	docker compose down -v

migrate:
	poetry run alembic upgrade head

run:
	poetry run uvicorn app.main:app --reload

test:
	./scripts/test.sh

lint:
	poetry run ruff check .
	poetry run ruff format --check .

fmt:
	poetry run ruff format .
	poetry run ruff check --fix .

seed demo-duplicates:
	poetry run python scripts/seed_and_burst.py
