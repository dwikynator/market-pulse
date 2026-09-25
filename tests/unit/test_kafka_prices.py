import json
from datetime import UTC, datetime

from market_pulse.kafka_prices import (
    price_message_key,
    report_delivery,
    serialize_price_event,
)
from market_pulse.models import MarketPrice


def sample_price() -> MarketPrice:
    """Create a deterministic MarketPrice fixture with lowercase symbol."""
    return MarketPrice(
        symbol="aapl",
        interval="5m",
        event_time=datetime(2026, 9, 23, 8, 0, tzinfo=UTC),
        open=229.10,
        high=230.25,
        low=228.95,
        close=229.80,
        adjusted_close=None,
        volume=125_000,
        ingested_at=datetime(2026, 9, 23, 8, 1, tzinfo=UTC),
    )


def test_price_event_serializes_as_json() -> None:
    """Verify MarketPrice serializes to JSON bytes with uppercase symbol and event_id."""
    event = sample_price()
    payload = json.loads(serialize_price_event(event))
    # Validate payload structure and ensure event_id is preserved for downstream deduplication
    assert payload["symbol"] == "AAPL"
    assert payload["event_id"] == event.event_id


def test_price_message_key_is_the_symbol() -> None:
    """Verify Kafka message key is extracted as UTF-8 encoded uppercase symbol bytes."""
    # Partitioning key must be uppercase bytes to preserve per-symbol ordering in Kafka
    assert price_message_key(sample_price()) == b"AAPL"


def test_delivery_failure_is_collected(capsys) -> None:
    """Verify delivery callback captures errors in accumulator and logs to stderr."""
    errors: list[str] = []
    # Callback captures failure for caller to inspect while printing visible error log
    report_delivery(RuntimeError("broker unavailable"), object(), errors=errors)
    assert errors == ["broker unavailable"]
    assert "Delivery failed: broker unavailable" in capsys.readouterr().err
