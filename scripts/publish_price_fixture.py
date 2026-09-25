import argparse
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd

from market_pulse.collectors.market import (
    dataframe_to_rows,
    normalize_market_rows,
)
from market_pulse.config import get_settings
from market_pulse.kafka_prices import DEFAULT_TOPIC, publish_price_events
from market_pulse.models import MarketPrice

FIXTURE_PATH = Path("tests/fixtures/market_prices.csv")


def build_fixture_events(*, repeat: int) -> list[MarketPrice]:
    """Transform historical CSV fixture records into a sequence of accelerated 5-minute events."""
    frame = pd.read_csv(FIXTURE_PATH)
    ingested_at = datetime.now(UTC)
    # Reuse existing batch parser and domain models to ensure identical validation and fields
    rows = dataframe_to_rows(frame, symbol="AAPL", interval="1d")
    source_records = normalize_market_rows(rows, ingested_at=ingested_at)
    # Align starting timestamp to minute boundaries for clean incremental intervals
    base_time = ingested_at.replace(second=0, microsecond=0)
    events: list[MarketPrice] = []

    for index in range(len(source_records) * repeat):
        # Modulo cycles through fixture rows when repeat > 1 while preserving actual price values
        source = source_records[index % len(source_records)]
        # Advance timestamps by 5-minute intervals to simulate an incoming live stream
        events.append(
            source.model_copy(
                update={
                    "interval": "5m",
                    "event_time": base_time + timedelta(minutes=5 * index),
                    "ingested_at": ingested_at,
                }
            )
        )

    return events


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for accelerated fixture replay into Kafka."""
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Replay fixture prices into Kafka.")
    # Target Kafka broker address (defaults to KAFKA_BOOTSTRAP_SERVERS, e.g. localhost:9092)
    parser.add_argument(
        "--bootstrap-servers",
        default=settings.kafka_bootstrap_servers,
    )
    # Destination Kafka topic (defaults to 'market-prices')
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    # Number of times to repeat the fixture sequence to produce a longer test stream
    parser.add_argument("--repeat", type=int, default=1)
    # Artificial pause between messages to simulate real-time arrival for Spark consumers
    parser.add_argument("--delay-seconds", type=float, default=0.5)
    args = parser.parse_args()
    if args.repeat < 1:
        parser.error("--repeat must be at least 1")
    if args.delay_seconds < 0:
        parser.error("--delay-seconds must not be negative")
    return args


def main() -> None:
    """Build accelerated fixture events and stream them to Kafka."""
    args = parse_args()
    events = build_fixture_events(repeat=args.repeat)
    print(f"Publishing {len(events)} accelerated fixture events")
    publish_price_events(
        events,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        delay_seconds=args.delay_seconds,
    )


if __name__ == "__main__":
    main()
