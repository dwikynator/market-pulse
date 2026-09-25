import argparse
import sys
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any

import yaml
from confluent_kafka import Producer

from market_pulse.collectors.market import collect_recent_market_prices
from market_pulse.config import get_settings
from market_pulse.models import MarketPrice

DEFAULT_TOPIC = "market-prices"


def serialize_price_event(event: MarketPrice) -> bytes:
    """Serialize a MarketPrice model into UTF-8 JSON bytes for the Kafka message payload."""
    # Kafka brokers are format-agnostic and transmit raw bytes over the wire, not Python strings
    return event.model_dump_json().encode("utf-8")


def price_message_key(event: MarketPrice) -> bytes:
    """Extract and encode the symbol into UTF-8 bytes for Kafka partition routing."""
    # Kafka partitioners (e.g. MurmurHash2) require raw bytes to hash and pin
    # a symbol to its target partition
    return event.symbol.encode("utf-8")


def report_delivery(
    error: Any,
    message: Any,
    *,
    errors: list[str],
) -> None:
    """Handle delivery confirmation callbacks from the Kafka producer."""
    if error is not None:
        errors.append(str(error))
        print(f"Delivery failed: {error}", file=sys.stderr)
        return

    print(f"Delivered {message.topic()} [{message.partition()}] at offset {message.offset()}")


def publish_price_events(
    events: Sequence[MarketPrice],
    *,
    bootstrap_servers: str,
    topic: str = DEFAULT_TOPIC,
    delay_seconds: float = 0.0,
) -> None:
    """Publish a sequence of MarketPrice events to Kafka with delivery confirmation."""
    if not events:
        raise ValueError("at least one price event is required")

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "market-pulse-price-publisher",
            # Prevent duplicate records caused by producer retries during transient network issues
            "enable.idempotence": True,
            # Wait for all in-sync replicas (1 broker in local dev) to acknowledge receipt
            "acks": "all",
        }
    )
    errors: list[str] = []
    # librdkafka expects an on_delivery callback with signature (err, msg).
    # functools.partial pre-binds errors=errors so the callback can record
    # failures without relying on global state.
    callback = partial(report_delivery, errors=errors)

    for event in events:
        # produce() is non-blocking: it buffers the message in librdkafka's internal queue
        producer.produce(
            topic=topic,
            key=price_message_key(event),
            value=serialize_price_event(event),
            on_delivery=callback,
        )
        # poll(0) serves any pending delivery callbacks immediately without blocking (timeout=0s),
        # preventing internal queue growth and triggering delivery callbacks as responses arrive
        producer.poll(0)
        if delay_seconds > 0:
            time.sleep(delay_seconds)

    # flush(10.0) blocks execution until all queued messages are sent and all delivery
    # callbacks complete, timing out after 10 seconds if any remain in flight
    remaining = producer.flush(10.0)
    if remaining:
        raise RuntimeError(f"{remaining} Kafka messages were not delivered before timeout")
    if errors:
        raise RuntimeError(f"Kafka delivery failed for {len(errors)} message(s): {errors[0]}")


def load_symbols(path: Path = Path("config/sources.yml")) -> list[str]:
    """Load tracked stock symbols from YAML configuration."""
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    return [item["symbol"] for item in config["market"]["symbols"]]


def parse_args() -> argparse.Namespace:
    """Parse CLI options for Kafka publishing target and event batch size."""
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Publish recent market prices to Kafka.")
    # Target Kafka broker address (defaults to KAFKA_BOOTSTRAP_SERVERS, e.g. localhost:9092)
    parser.add_argument(
        "--bootstrap-servers",
        default=settings.kafka_bootstrap_servers,
    )
    # Destination Kafka topic (defaults to 'market-prices')
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    # Number of recent 5-minute bars to fetch per symbol (defaults to 3 to keep demo stream small)
    parser.add_argument("--limit-per-symbol", type=int, default=3)
    return parser.parse_args()


def main() -> None:
    """Collect recent prices and publish them to Kafka as live sample events."""
    args = parse_args()
    events = collect_recent_market_prices(
        load_symbols(),
        ingested_at=datetime.now(UTC),
        limit_per_symbol=args.limit_per_symbol,
    )
    print(f"Publishing {len(events)} recent price events")
    # Print the first event to inspect payload schema and timestamps before publishing
    print(events[0].model_dump_json(indent=2))
    publish_price_events(
        events,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
    )


if __name__ == "__main__":
    main()
