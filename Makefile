.PHONY: setup kafka-up kafka-topics kafka-down explore-yfinance explore-fred preview test lint format check preview-batch batch

setup:
	uv sync

kafka-up:
	docker compose up -d kafka

kafka-topics:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:19092 --list

kafka-down:
	docker compose down

explore-yfinance:
	uv run python scripts/explore_yfinance.py

explore-fred:
	uv run python scripts/explore_fred.py

preview:
	uv run python scripts/preview_models.py

test:
	uv run pytest -q

lint:
	uv run ruff check .

format:
	uv run ruff format .

check: lint test

preview-batch:
	uv run python scripts/preview_batch_sources.py

batch:
	uv run python -m market_pulse.batch
