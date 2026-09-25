import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from market_pulse.config import get_settings
from market_pulse.kafka_prices import DEFAULT_TOPIC

MESSAGE_SCHEMA = StructType(
    [
        StructField("message_key", StringType(), True),
        StructField("raw_value", StringType(), True),
        StructField("kafka_topic", StringType(), True),
        StructField("kafka_partition", IntegerType(), True),
        StructField("kafka_offset", LongType(), True),
        StructField("kafka_timestamp", TimestampType(), True),
    ]
)

PRICE_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), True),
        StructField("interval", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("adjusted_close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("source", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
        StructField("event_id", StringType(), True),
        # Populated by Spark when JSON cannot be parsed
        StructField("_corrupt_record", StringType(), True),
    ]
)

# adjusted_close is intentionally absent — it is optional in MarketPrice
# and not required for routing.
REQUIRED_FIELDS = [
    "symbol",
    "interval",
    "event_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source",
    "ingested_at",
    "event_id",
]


def parse_and_classify(messages: DataFrame) -> DataFrame:
    # PERMISSIVE: unparseable JSON is kept in _corrupt_record instead of crashing the stream.
    # A wrong field type becomes null; the reason chain below handles both cases.
    event = F.from_json(
        "raw_value",
        PRICE_SCHEMA,
        {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
    )
    parsed = messages.withColumn("event", event)

    # F.col() expressions are lazy — they describe a computation, not a value.
    # We OR-combine them into a single Column that is True when any required field is null.
    missing_field = F.lit(False)
    for field_name in REQUIRED_FIELDS:
        missing_field = missing_field | F.col(f"event.{field_name}").isNull()

    invalid_ohlc = (
        (F.col("event.high") < F.col("event.open"))
        | (F.col("event.high") < F.col("event.low"))
        | (F.col("event.high") < F.col("event.close"))
        | (F.col("event.low") > F.col("event.open"))
        | (F.col("event.low") > F.col("event.high"))
        | (F.col("event.low") > F.col("event.close"))
    )

    # Only the first matching branch fires; order matters (structural errors before business rules).
    # null result means no rule matched → record is valid.
    reason = (
        F.when(F.col("event._corrupt_record").isNotNull(), "MALFORMED_JSON")
        .when(missing_field, "MISSING_OR_INVALID_FIELD")
        .when(F.col("message_key") != F.col("event.symbol"), "KEY_SYMBOL_MISMATCH")
        .when(F.col("event.interval") != "5m", "UNSUPPORTED_INTERVAL")
        .when(F.col("event.source") != "yfinance", "UNSUPPORTED_SOURCE")
        .when(
            (F.col("event.open") <= 0)
            | (F.col("event.high") <= 0)
            | (F.col("event.low") <= 0)
            | (F.col("event.close") <= 0),
            "NON_POSITIVE_PRICE",
        )
        .when(F.col("event.volume") < 0, "NEGATIVE_VOLUME")
        .when(invalid_ohlc, "INVALID_OHLC")
    )
    return parsed.withColumn("rejection_reason", reason)


def split_classified(classified: DataFrame) -> tuple[DataFrame, DataFrame]:
    accepted = classified.filter(F.col("rejection_reason").isNull()).select(
        "event.symbol",
        "event.interval",
        "event.event_time",
        "event.open",
        "event.high",
        "event.low",
        "event.close",
        "event.adjusted_close",
        "event.volume",
        "event.source",
        "event.ingested_at",
        "event.event_id",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        F.to_date("event.event_time").alias("event_date"),
    )
    quarantine = classified.filter(F.col("rejection_reason").isNotNull()).select(
        "message_key",
        "raw_value",
        "rejection_reason",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        F.current_timestamp().alias("quarantined_at"),
    )
    return accepted, quarantine


def build_outputs(
    messages: DataFrame,
    *,
    watermark_delay: str = "30 minutes",
) -> tuple[DataFrame, DataFrame]:
    accepted, quarantine = split_classified(parse_and_classify(messages))
    # withWatermark must come before dropDuplicatesWithinWatermark.
    # It tells Spark how long to keep event_id state; once state expires,
    # duplicates that arrive after the boundary are no longer caught.
    deduplicated = accepted.withWatermark(
        "event_time",
        watermark_delay,
    ).dropDuplicatesWithinWatermark(["event_id"])
    return deduplicated, quarantine


def read_kafka(
    spark: SparkSession,
    *,
    bootstrap_servers: str,
    topic: str,
) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")  # first run only; ignored once a checkpoint exists
        .option("failOnDataLoss", "true")  # fail visibly if retention removed unread offsets
        .load()
        .select(
            # key and value are binary; cast to UTF-8 strings before any JSON parsing.
            F.col("key").cast("string").alias("message_key"),
            F.col("value").cast("string").alias("raw_value"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
    )


def start_queries(
    accepted: DataFrame,
    quarantine: DataFrame,
    *,
    output_root: str,
    checkpoint_root: str,
    trigger_interval: str,
) -> list[StreamingQuery]:
    accepted_query = (
        accepted.writeStream.queryName("accepted-market-prices")
        .format("parquet")
        .outputMode("append")
        .partitionBy("event_date")
        .option("checkpointLocation", f"{checkpoint_root}/accepted-market-prices")
        .trigger(processingTime=trigger_interval)
        .start(f"{output_root}/accepted/market_prices")
    )
    quarantine_query = (
        quarantine.writeStream.queryName("quarantined-market-prices")
        .format("json")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_root}/quarantined-market-prices")
        .trigger(processingTime=trigger_interval)
        .start(f"{output_root}/quarantine/market_prices")
    )
    return [accepted_query, quarantine_query]


def create_spark_session(*, uses_s3: bool) -> SparkSession:
    builder = (
        SparkSession.builder.appName("market-price-stream")
        .master("local[2]")  # two threads: one per streaming query
        .config("spark.sql.session.timeZone", "UTC")
        # Matches 3 Kafka partitions; default 200 is wasteful locally
        .config("spark.sql.shuffle.partitions", "3")
    )
    if uses_s3:
        # Reads credentials from the AWS profile set by AWS_PROFILE env var — no keys in code.
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider",
        ).config("spark.hadoop.fs.s3a.endpoint.region", os.getenv("AWS_REGION", "ap-southeast-1"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def parse_args() -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Process Kafka market prices with Spark.")
    parser.add_argument("--bootstrap-servers", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--checkpoint-root")
    parser.add_argument("--watermark-delay", default="30 minutes")
    parser.add_argument("--trigger-interval", default="5 seconds")
    args = parser.parse_args()
    if args.checkpoint_root is None:
        args.checkpoint_root = f"{args.output_root}/checkpoints"
    return args


def main() -> None:
    args = parse_args()
    uses_s3 = args.output_root.startswith("s3a://")
    spark = create_spark_session(uses_s3=uses_s3)
    messages = read_kafka(
        spark,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
    )
    accepted, quarantine = build_outputs(
        messages,
        watermark_delay=args.watermark_delay,
    )
    queries = start_queries(
        accepted,
        quarantine,
        output_root=args.output_root.rstrip("/"),
        checkpoint_root=args.checkpoint_root.rstrip("/"),
        trigger_interval=args.trigger_interval,
    )
    print(f"Streaming {args.topic} to {args.output_root}. Press Ctrl+C to stop.")
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("Stopping Spark queries")
    finally:
        for query in queries:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
