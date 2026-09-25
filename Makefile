.PHONY: setup kafka-up kafka-topics kafka-create-topic kafka-down explore-yfinance explore-fred preview preview-batch batch publish-recent publish-fixture test lint format check

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

kafka-create-topic:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server broker:19092 \
		--create \
		--if-not-exists \
		--topic market-prices \
		--partitions 3 \
		--replication-factor 1 \
		--config retention.ms=21600000

publish-recent:
	uv run python -m market_pulse.kafka_prices

publish-fixture:
	uv run python scripts/publish_price_fixture.py
