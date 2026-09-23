from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

from market_intelligence.models import EconomicObservation, MarketPrice


def valid_market_price(**changes: object) -> MarketPrice:
    values: dict[str, object] = {
        "symbol": "aapl",
        "interval": "5m",
        "event_time": datetime.fromisoformat("2026-09-22T16:00:00-04:00"),
        "open": 229.10,
        "high": 230.25,
        "low": 228.95,
        "close": 229.80,
        "adjusted_close": None,
        "volume": 125_000,
        "ingested_at": datetime(2026, 9, 23, 8, 0, tzinfo=UTC),
    }
    values.update(changes)
    return MarketPrice.model_validate(values)


def valid_observation(**changes: object) -> EconomicObservation:
    values: dict[str, object] = {
        "series_id": "dgs10",
        "observation_date": date(2026, 9, 22),
        "value": 4.15,
        "realtime_start": date(2026, 9, 23),
        "realtime_end": date(2026, 9, 23),
        "ingested_at": datetime(2026, 9, 23, 8, 0, tzinfo=UTC),
    }
    values.update(changes)
    return EconomicObservation.model_validate(values)


def test_market_price_normalizes_symbol_and_timestamp() -> None:
    price = valid_market_price()

    assert price.symbol == "AAPL"
    assert price.event_time == datetime(2026, 9, 22, 20, 0, tzinfo=UTC)


def test_same_market_event_has_same_id() -> None:
    first = valid_market_price(ingested_at=datetime(2026, 9, 23, 8, 0, tzinfo=UTC))
    retried = valid_market_price(ingested_at=datetime(2026, 9, 23, 8, 5, tzinfo=UTC))

    assert first.event_id == retried.event_id


def test_market_price_rejects_invalid_ohlc() -> None:
    with pytest.raises(ValidationError, match="high must be"):
        valid_market_price(high=228.00)


def test_market_price_rejects_negative_volume() -> None:
    with pytest.raises(ValidationError):
        valid_market_price(volume=-1)


def test_market_price_rejects_timestamp_without_timezone() -> None:
    with pytest.raises(ValidationError, match="timestamp must include a timezone"):
        valid_market_price(event_time=datetime(2026, 9, 22, 20, 0))


def test_economic_observation_allows_missing_value() -> None:
    observation = valid_observation(value=None)

    assert observation.value is None
    assert observation.series_id == "DGS10"


def test_revised_economic_observation_has_different_id() -> None:
    first = valid_observation(
        realtime_start=date(2026, 9, 23),
        realtime_end=date(2026, 9, 23),
    )
    revised = valid_observation(
        realtime_start=date(2026, 10, 1),
        realtime_end=date(2026, 10, 1),
    )

    assert first.observation_id != revised.observation_id


def test_economic_observation_rejects_reversed_realtime_window() -> None:
    with pytest.raises(ValidationError, match="realtime_end must not be"):
        valid_observation(
            realtime_start=date(2026, 9, 24),
            realtime_end=date(2026, 9, 23),
        )
