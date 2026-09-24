from datetime import UTC, date, datetime

from market_pulse.models import EconomicObservation, MarketPrice

ingested_at = datetime.now(UTC)

price = MarketPrice(
    symbol="aapl",
    interval="5m",
    event_time=datetime.fromisoformat("2026-09-22T16:00:00-04:00"),
    open=229.10,
    high=230.25,
    low=228.95,
    close=229.80,
    adjusted_close=None,
    volume=125_000,
    ingested_at=ingested_at,
)

observation = EconomicObservation(
    series_id="dgs10",
    observation_date=date(2026, 9, 22),
    value=4.15,
    realtime_start=date(2026, 9, 23),
    realtime_end=date(2026, 9, 23),
    ingested_at=ingested_at,
)

print("Market price:")
print(price.model_dump_json(indent=2))
print("\nEconomic observation:")
print(observation.model_dump_json(indent=2))
