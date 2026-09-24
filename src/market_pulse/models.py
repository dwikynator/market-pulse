from datetime import UTC, date, datetime
from hashlib import sha256
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator, model_validator


def _stable_id(*parts: object) -> str:
    text = "|".join(str(part) for part in parts)
    return sha256(text.encode("utf-8")).hexdigest()


def _require_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include a timezone")

    return value.astimezone(UTC)


class MarketPrice(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    symbol: str = Field(min_length=1, max_length=20)
    interval: Literal["1d", "5m"]
    event_time: datetime
    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    adjusted_close: float | None = Field(default=None, gt=0)
    volume: int = Field(ge=0)
    source: Literal["yfinance"] = "yfinance"
    ingested_at: datetime

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("event_time", "ingested_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _require_aware_utc(value)

    @model_validator(mode="after")
    def validate_ohlc(self) -> Self:
        if self.high < max(self.open, self.low, self.close):
            raise ValueError("high must be greater than or equal to open, low, and close")

        if self.low > min(self.open, self.high, self.close):
            raise ValueError("low must be less than or equal to open, high, and close")

        return self

    @computed_field
    @property
    def event_id(self) -> str:
        return _stable_id(
            self.source,
            self.symbol,
            self.interval,
            self.event_time.isoformat(),
        )


class EconomicObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    series_id: str = Field(min_length=1, max_length=50)
    observation_date: date
    value: float | None
    realtime_start: date
    realtime_end: date
    source: Literal["fred"] = "fred"
    ingested_at: datetime

    @field_validator("series_id")
    @classmethod
    def normalize_series_id(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("ingested_at")
    @classmethod
    def normalize_ingested_at(cls, value: datetime) -> datetime:
        return _require_aware_utc(value)

    @model_validator(mode="after")
    def validate_realtime_window(self) -> Self:
        if self.realtime_end < self.realtime_start:
            raise ValueError("realtime_end must not be before realtime_start")

        return self

    @computed_field
    @property
    def observation_id(self) -> str:
        return _stable_id(
            self.source,
            self.series_id,
            self.observation_date.isoformat(),
            self.realtime_start.isoformat(),
            self.realtime_end.isoformat(),
        )
