import json
from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession

from market_pulse.spark_prices import MESSAGE_SCHEMA, build_outputs


# scope="module" means pytest starts one SparkSession for the entire file and
# reuses it across all tests in the module. Starting Spark takes ~20-40 s
# (JVM startup); module scope pays that cost once instead of once per test.
@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("spark-price-test")
        # Without binding to 127.0.0.1 Spark tries to resolve the machine
        # hostname, which can fail in CI environments or on some macOS setups.
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        # 2 shuffle partitions is enough for the tiny test data; the
        # production session uses 3 to match the Kafka topic partition count.
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    # yield keeps the session alive for the duration of the module, then
    # session.stop() runs automatically after the last test in the module ends.
    yield session
    session.stop()


# pytest injects both fixtures by matching parameter names to registered fixtures:
#   - `spark`    → the module-scoped SparkSession defined above
#   - `tmp_path` → a built-in pytest fixture that provides a fresh
#                  pathlib.Path pointing to a unique temporary directory
#                  for each test function (e.g. /tmp/pytest-of-user/pytest-42/test_stream0/).
#                  pytest automatically deletes it after the test session ends.
def test_stream_deduplicates_valid_events_and_quarantines_bad_json(
    spark: SparkSession,
    tmp_path,
) -> None:
    # A single valid AAPL 5-minute candle. This is the canonical "happy path"
    # payload that the production producer (kafka_prices.py) would publish.
    event = {
        "symbol": "AAPL",
        "interval": "5m",
        "event_time": "2026-09-23T08:00:00Z",
        "open": 229.10,
        "high": 230.25,
        "low": 228.95,
        "close": 229.80,
        "adjusted_close": None,  # optional field — allowed to be null
        "volume": 125_000,
        "source": "yfinance",
        "ingested_at": "2026-09-23T08:01:00Z",
        "event_id": "event-aapl-0800",
    }

    # kafka_timestamp is the time Kafka received the message. It is distinct
    # from event_time (when the market event occurred). Here both rows share
    # the same timestamp because they simulate the same message being retried.
    timestamp = datetime(2026, 9, 23, 8, 1, tzinfo=UTC)

    # Three rows that simulate what read_kafka() would produce from Kafka:
    #   row 0 — valid event, offset 0 (first delivery)
    #   row 1 — same valid event, offset 1 (producer retry / duplicate)
    #   row 2 — deliberately broken JSON (missing closing brace) at offset 2
    # Columns match MESSAGE_SCHEMA: (message_key, raw_value, topic, partition, offset, timestamp)
    rows = [
        ("AAPL", json.dumps(event), "market-prices", 0, 0, timestamp),
        ("AAPL", json.dumps(event), "market-prices", 0, 1, timestamp),
        ("AAPL", '{"symbol":"AAPL"', "market-prices", 0, 2, timestamp),
    ]

    # All paths live under tmp_path so each test run is isolated from others.
    # tmp_path is e.g. /tmp/pytest-of-user/pytest-42/test_stream...0/
    # The subdirectories do not need to be created manually — Spark and
    # the file-stream source create them on first write.
    input_path = tmp_path / "input"
    accepted_path = tmp_path / "accepted"
    quarantine_path = tmp_path / "quarantine"
    accepted_checkpoint = tmp_path / "checkpoints" / "accepted"
    quarantine_checkpoint = tmp_path / "checkpoints" / "quarantine"

    # Write the three rows as JSON files to disk so the file-stream source
    # can read them. coalesce(1) forces a single output file; without it
    # Spark might write multiple part files and the streaming reader could
    # pick them up in an unexpected order across micro-batches.
    spark.createDataFrame(rows, MESSAGE_SCHEMA).coalesce(1).write.json(str(input_path))

    # This test uses the JSON file-stream source instead of the real Kafka source.
    # The two sources produce identically shaped DataFrames (MESSAGE_SCHEMA),
    # so build_outputs() sees no difference. This lets the test run without
    # Docker or a live broker — just ordinary pytest.
    messages = spark.readStream.schema(MESSAGE_SCHEMA).json(str(input_path))
    accepted, quarantine = build_outputs(messages)

    # trigger(availableNow=True) processes all files currently on disk and
    # then stops automatically. Without this, the streaming query would run
    # forever waiting for new files — the test would never finish.
    accepted_query = (
        accepted.writeStream.format("parquet")
        .option("checkpointLocation", str(accepted_checkpoint))
        .trigger(availableNow=True)
        .start(str(accepted_path))
    )
    quarantine_query = (
        quarantine.writeStream.format("json")
        .option("checkpointLocation", str(quarantine_checkpoint))
        .trigger(availableNow=True)
        .start(str(quarantine_path))
    )

    # Block until both queries finish processing all available input.
    # awaitTermination() returns only after availableNow=True signals completion.
    accepted_query.awaitTermination()
    quarantine_query.awaitTermination()

    # Switch from streaming reads to batch reads to assert on the output.
    # spark.read (not spark.readStream) loads everything written so far.
    accepted_rows = spark.read.parquet(str(accepted_path)).collect()
    quarantine_rows = spark.read.json(str(quarantine_path)).collect()

    # Row 0 and row 1 have the same event_id → deduplication keeps only one.
    assert len(accepted_rows) == 1
    assert accepted_rows[0].event_id == "event-aapl-0800"

    # Row 2 (broken JSON) should land in quarantine with the correct reason.
    # Row 0 and 1 are valid, so only 1 quarantine record is expected.
    assert len(quarantine_rows) == 1
    assert quarantine_rows[0].rejection_reason == "MALFORMED_JSON"
