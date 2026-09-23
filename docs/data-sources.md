# Data-source notes

This document records what we observed from the source APIs and the decisions
made for the portfolio pipeline. It should be updated when a provider or a
configured series changes.

## Scope

| Source   | Data in scope                                      | Purpose                                                 |
| -------- | -------------------------------------------------- | ------------------------------------------------------- |
| yfinance | Daily and five-minute OHLCV prices for six symbols | Demonstrate batch and streaming-shaped market ingestion |
| FRED     | DGS10 and CPIAUCSL observations                    | Add interest-rate and inflation context                 |

OHLCV means open, high, low, close, and volume.

## yfinance findings

- The source returns a pandas DataFrame with time in its index.
- Daily and five-minute results use different index labels.
- Intraday data can carry the exchange timezone.
- We request `auto_adjust=False` so raw close and adjusted close remain distinct.
- We request one symbol at a time and disable multi-level columns.
- Intraday history is bounded by the provider, so it is not a permanent archive.
- yfinance is appropriate for this educational portfolio, not a promised
  production market-data service.

### Normalized market-price record

One record represents one symbol, one interval, and one event time.

Required fields: symbol, interval, event_time, open, high, low, close, volume,
source, and ingested_at. Adjusted close is optional because it may be absent from
some source responses.

The stable identity is derived from source, symbol, interval, and event_time.

## FRED findings

- Series metadata supplies the title, frequency, and units needed to interpret
  an observation.
- Observation values arrive as strings.
- A period (`.`) means missing; it must become `None`, never zero.
- Observation dates are dates rather than event timestamps.
- `realtime_start` and `realtime_end` describe the data vintage and allow FRED
  revisions to remain distinguishable.

### Normalized economic-observation record

One record represents one series, one observation date, and one real-time
revision window.

Required fields: series_id, observation_date, value, realtime_start,
realtime_end, source, and ingested_at. Value is nullable for a FRED missing-value
marker.

The stable identity is derived from source, series_id, observation_date,
realtime_start, and realtime_end.

## Project-owned rules

These are our rules, not claims about every possible source response:

- symbols and series IDs are stored in uppercase;
- timestamps must include a timezone and are normalized to UTC;
- market prices must be positive;
- volume must be zero or greater;
- high cannot be below open, low, or close;
- low cannot be above open, high, or close; and
- a real-time end date cannot precede its start date.

## References

- [yfinance download parameters](https://ranaroussi.github.io/yfinance/reference/api/yfinance.download.html)
- [FRED series metadata endpoint](https://fred.stlouisfed.org/docs/api/fred/series.html)
- [FRED series observations endpoint](https://fred.stlouisfed.org/docs/api/fred/series_observations.html)
