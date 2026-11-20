.PHONY: install up down reset migrate run worker consumer test lint fmt seed demo-duplicates demo-outage

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

worker:
	poetry run python -m app.stock.worker

consumer:
	poetry run python scripts/feed_consumer.py

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

demo-outage:
	poetry run python scripts/outage_demo.py
