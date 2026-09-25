import argparse

from market_pulse.spark_prices import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect Spark streaming output.")
    parser.add_argument("--output-root", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark_session(uses_s3=args.output_root.startswith("s3a://"))
    accepted = spark.read.parquet(f"{args.output_root}/accepted/market_prices")
    quarantine = spark.read.json(f"{args.output_root}/quarantine/market_prices")

    print(f"Accepted rows: {accepted.count()}")
    accepted.select(
        "event_id",
        "symbol",
        "event_time",
        "kafka_partition",
        "kafka_offset",
    ).orderBy("event_time").show(truncate=False)

    print(f"Quarantined rows: {quarantine.count()}")
    quarantine.select(
        "rejection_reason",
        "message_key",
        "raw_value",
        "kafka_partition",
        "kafka_offset",
    ).show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
