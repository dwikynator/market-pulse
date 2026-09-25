SPARK_VERSION := 4.2.0
HADOOP_VERSION := 3.5.0
KAFKA_SPARK_PACKAGE := org.apache.spark:spark-sql-kafka-0-10_2.13:$(SPARK_VERSION)
HADOOP_AWS_PACKAGE := org.apache.hadoop:hadoop-aws:$(HADOOP_VERSION)
SPARK_PACKAGES := $(KAFKA_SPARK_PACKAGE),$(HADOOP_AWS_PACKAGE)
STREAM_ROOT ?= s3a://$(DATA_BUCKET)/streaming

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

spark-stream:
	uv run spark-submit \
		--packages "$(SPARK_PACKAGES)" \
		src/market_pulse/spark_prices.py \
		--output-root "$(STREAM_ROOT)"

spark-inspect:
	uv run spark-submit \
		--packages "$(HADOOP_AWS_PACKAGE)" \
		scripts/inspect_stream_output.py \
		--output-root "$(STREAM_ROOT)"

publish-spark-demo:
	uv run python scripts/publish_spark_demo.py
