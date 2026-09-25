import copy
import json
from datetime import UTC, date, datetime
from io import BytesIO
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from market_pulse.batch import build_keys
from market_pulse.collectors.fred import normalize_fred_series
from market_pulse.collectors.market import (
    dataframe_to_rows,
    normalize_market_rows,
)
from market_pulse.storage import logical_records_hash, records_to_parquet

FIXTURES = Path("tests/fixtures")
INGESTED_AT = datetime(2026, 9, 23, 8, 0, tzinfo=UTC)


def market_records():
    """Load and normalize local AAPL CSV fixture rows."""
    frame = pd.read_csv(FIXTURES / "market_prices.csv")
    rows = dataframe_to_rows(frame, symbol="AAPL", interval="1d")
    return normalize_market_rows(rows, ingested_at=INGESTED_AT)


def fred_payload():
    """Load offline FRED JSON observations fixture."""
    return json.loads((FIXTURES / "fred_observations.json").read_text())


def test_market_fixture_normalizes_rows() -> None:
    """Verify market fixture rows normalize with valid symbols and UTC timestamps."""
    records = market_records()
    assert len(records) > 0
    assert records[0].symbol == "AAPL"
    assert records[0].event_time.tzinfo is UTC


def test_fred_missing_marker_becomes_none() -> None:
    """Verify FRED missing marker '.' maps to None instead of numeric zero or error."""
    payload = copy.deepcopy(fred_payload())
    payload["DGS10"]["observations"][0]["value"] = "."
    records = normalize_fred_series(payload, ingested_at=INGESTED_AT)
    dgs10 = next(record for record in records if record.series_id == "DGS10")
    assert dgs10.value is None


def test_market_records_round_trip_through_parquet() -> None:
    """Verify in-memory Parquet serialization preserves row counts and schema fields."""
    records = market_records()
    parquet_bytes = records_to_parquet(records)
    table = pq.read_table(BytesIO(parquet_bytes))
    assert table.num_rows == len(records)
    assert "event_id" in table.column_names


def test_logical_hash_ignores_ingestion_time() -> None:
    """Verify logical content hash is invariant to execution timestamp for idempotency."""
    first = market_records()
    later = [
        record.model_copy(update={"ingested_at": datetime(2026, 9, 23, 9, 0, tzinfo=UTC)})
        for record in first
    ]
    assert logical_records_hash(first) == logical_records_hash(later)


def test_batch_keys_are_stable_for_the_same_date() -> None:
    """Verify partition key generation is deterministic for a given as_of date."""
    keys = build_keys(date(2026, 9, 23))
    assert keys["market_raw"] == "raw/yfinance/as_of=2026-09-23/prices.json"
    assert keys == build_keys(date(2026, 9, 23))
