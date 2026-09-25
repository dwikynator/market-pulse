from datetime import date, datetime, timedelta
from typing import Any, Literal

import pandas as pd
import yfinance as yf

from market_pulse.models import MarketPrice


def _python_value(value: Any) -> Any:
    """Convert pandas/numpy scalar types into native Python types for JSON serialization."""
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    return value


def dataframe_to_rows(
    frame: pd.DataFrame, *, symbol: str, interval: Literal["1d", "5m"]
) -> list[dict[str, Any]]:
    """Convert a yfinance OHLCV DataFrame into a list of standardized record dictionaries."""
    # Ensure Datetime/Date index becomes a regular column and standardize headers
    reset = frame.copy() if isinstance(frame.index, pd.RangeIndex) else frame.reset_index().copy()
    reset.columns = [str(column).lower().replace(" ", "_") for column in reset.columns]

    # Daily data uses 'date', intraday data uses 'datetime'
    time_column = "datetime" if "datetime" in reset.columns else "date"
    rows: list[dict[str, Any]] = []

    for source_row in reset.to_dict(orient="records"):
        values = {key: _python_value(value) for key, value in source_row.items()}
        timestamp = pd.Timestamp(values.pop(time_column))
        values.pop("symbol", None)

        # Fall back to US market timezone for naive timestamps (e.g. from CSV fixtures)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("America/New_York")

        rows.append(
            {
                "symbol": symbol.upper(),
                "interval": interval,
                "event_time": timestamp.isoformat(),
                **values,
            }
        )

    return rows


def normalize_market_rows(
    rows: list[dict[str, Any]],
    *,
    ingested_at: datetime,
) -> list[MarketPrice]:
    """Parse and validate raw record dictionaries into typed MarketPrice domain models."""
    # Validation errors fail fast here so corrupt batches are never published
    return [
        MarketPrice(
            symbol=row["symbol"],
            interval=row["interval"],
            event_time=datetime.fromisoformat(row["event_time"]),
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            adjusted_close=(None if row.get("adj_close") is None else float(row["adj_close"])),
            volume=int(row["volume"]),
            ingested_at=ingested_at,
        )
        for row in rows
    ]


def collect_market_prices(
    symbols: list[str],
    *,
    as_of: date,
    ingested_at: datetime,
) -> tuple[dict[str, Any], list[MarketPrice]]:
    """Fetch 1-year daily historical prices from yfinance with normalized models."""
    start = as_of - timedelta(days=365)
    # yfinance end date is exclusive, so add 1 day to capture the as_of date
    end_exclusive = as_of + timedelta(days=1)
    raw_rows: list[dict[str, Any]] = []

    for symbol in symbols:
        # Request one symbol at a time to keep columns flat and isolate failures
        frame = yf.download(
            tickers=symbol,
            start=start.isoformat(),
            end=end_exclusive.isoformat(),
            interval="1d",
            auto_adjust=False,
            actions=False,
            progress=False,
            threads=False,
            ignore_tz=False,
            multi_level_index=False,
            timeout=20,
        )
        if frame.empty:
            raise RuntimeError(f"yfinance returned no daily data for {symbol}")
        raw_rows.extend(dataframe_to_rows(frame, symbol=symbol, interval="1d"))

    raw_payload = {
        "provider": "yfinance",
        "window": {"start": start.isoformat(), "end": as_of.isoformat()},
        "rows": raw_rows,
    }

    return raw_payload, normalize_market_rows(raw_rows, ingested_at=ingested_at)


def collect_recent_market_prices(
    symbols: list[str],
    *,
    ingested_at: datetime,
    limit_per_symbol: int = 3,
) -> list[MarketPrice]:
    """Fetch recent 5-minute intraday prices from yfinance for Kafka event streaming."""
    if limit_per_symbol < 1:
        raise ValueError("limit_per_symbol must be at least 1")

    records: list[MarketPrice] = []

    for symbol in symbols:
        # Request up to 5 days to account for weekends, holidays, or early-session gaps
        frame = yf.download(
            tickers=symbol,
            period="5d",
            interval="5m",
            auto_adjust=False,
            actions=False,
            progress=False,
            threads=False,
            ignore_tz=False,
            multi_level_index=False,
            timeout=20,
        )
        if frame.empty:
            # Fail fast to prevent publishing an incomplete or misleading event set
            raise RuntimeError(f"yfinance returned no recent data for {symbol}")

        # Take only the latest bars per symbol to keep the local stream sample small
        rows = dataframe_to_rows(
            frame.tail(limit_per_symbol),
            symbol=symbol,
            interval="5m",
        )
        # Reuse existing normalization to compute consistent models and stable event_ids
        records.extend(normalize_market_rows(rows, ingested_at=ingested_at))

    return records
