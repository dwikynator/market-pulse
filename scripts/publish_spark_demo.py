import argparse
import time
from datetime import UTC, datetime, timedelta
from functools import partial

from confluent_kafka import Producer

from market_pulse.config import get_settings
from market_pulse.kafka_prices import DEFAULT_TOPIC, report_delivery
from market_pulse.models import MarketPrice


def make_event(*, event_time: datetime) -> MarketPrice:
    return MarketPrice(
        symbol="AAPL",
        interval="5m",
        event_time=event_time,
        open=229.10,
        high=230.25,
        low=228.95,
        close=229.80,
        adjusted_close=None,
        volume=125_000,
        ingested_at=datetime.now(UTC),
    )


def send_stage(
    producer: Producer,
    messages: list[tuple[str, bytes]],
    *,
    topic: str,
    errors: list[str],
) -> None:
    callback = partial(report_delivery, errors=errors)
    for key, value in messages:
        producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=value,
            on_delivery=callback,
        )
        producer.poll(0)

    remaining = producer.flush(10.0)
    if remaining or errors:
        raise RuntimeError("one or more demo messages were not delivered")


def parse_args() -> argparse.Namespace:
    settings = get_settings()
    parser = argparse.ArgumentParser(description="Publish Spark streaming demo events.")
    parser.add_argument("--bootstrap-servers", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--pause-seconds", type=float, default=7.0)
    args = parser.parse_args()
    if args.pause_seconds < 6:
        parser.error("--pause-seconds must be at least 6 for five-second Spark triggers")
    return args


def main() -> None:
    args = parse_args()
    now = datetime.now(UTC).replace(microsecond=0)
    current = make_event(event_time=now)
    out_of_order = make_event(event_time=now - timedelta(minutes=10))
    watermark_advance = make_event(event_time=now + timedelta(minutes=45))
    late = make_event(event_time=now - timedelta(minutes=45))
    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "client.id": "market-pulse-spark-demo",
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    errors: list[str] = []

    print("Stage 1: valid, duplicate, out-of-order, and malformed events")
    send_stage(
        producer,
        [
            ("AAPL", current.model_dump_json().encode("utf-8")),
            ("AAPL", current.model_dump_json().encode("utf-8")),
            ("AAPL", out_of_order.model_dump_json().encode("utf-8")),
            ("AAPL", b'{"symbol":"AAPL"'),
        ],
        topic=args.topic,
        errors=errors,
    )
    time.sleep(args.pause_seconds)

    print("Stage 2: advance the event-time watermark")
    send_stage(
        producer,
        [("AAPL", watermark_advance.model_dump_json().encode("utf-8"))],
        topic=args.topic,
        errors=errors,
    )
    time.sleep(args.pause_seconds)

    print(f"Stage 3: late event {late.event_id}")
    send_stage(
        producer,
        [("AAPL", late.model_dump_json().encode("utf-8"))],
        topic=args.topic,
        errors=errors,
    )


if __name__ == "__main__":
    main()
