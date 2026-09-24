# Part 3: Batch Collection to S3

> **Project:** MarketPulse  
> **Estimated effort:** 3 focused hours  
> **Outcome:** Historical yfinance and FRED data collected, normalized, encoded as Parquet, and written to stable raw, curated, and audit paths in S3.

---

## 1. Outcome

Parts 1 and 2 established two important foundations:

- you inspected the real source responses before defining the models; and
- you provisioned one protected S3 bucket for the project.

This part connects those foundations into the first complete pipeline path.

You will:

1. implement a concrete yfinance historical-price collector;
2. implement a concrete FRED observation collector;
3. print normalized records locally before adding S3;
4. preserve source-shaped responses as raw JSON;
5. encode normalized records as Apache Parquet;
6. write one small audit summary with counts and a logical run ID;
7. avoid duplicate writes by comparing content hashes;
8. test normalization and Parquet using the fixtures from Part 1; and
9. run the pipeline twice and observe its idempotent behavior.

**Idempotent** means that retrying the same logical batch with unchanged source data leaves the same final result. In this implementation, an unchanged rerun reports `unchanged` instead of uploading another object version.

### What stays deliberately simple

This part uses one concrete collector for each source and one concrete S3 storage class. It does not introduce:

- a generic source-adapter framework;
- a storage interface with several fake implementations;
- a custom retry engine;
- quarantine handling for hypothetical failures; or
- a large audit and checksum subsystem.

Those abstractions would create more code than the two-source portfolio currently needs.

### Time plan

| Activity | Estimate |
| --- | ---: |
| Add dependencies and review the saved fixtures | 20 minutes |
| Implement and observe the yfinance collector | 40 minutes |
| Implement and observe the FRED collector | 35 minutes |
| Add Parquet and S3 storage | 35 minutes |
| Assemble the batch command | 25 minutes |
| Add focused tests | 15 minutes |
| Run twice, inspect S3, and commit | 10 minutes |
| **Total** | **180 minutes / 3 hours** |

Source or AWS availability problems can add time. Stop and read the actual error instead of adding speculative recovery code.

---

## 2. Concepts in Plain Language

### Batch collection

A **batch** processes a bounded group of data in one run. This part requests approximately one year of daily market prices and economic observations ending on one `as_of` date.

That differs from the Kafka path in Part 4, where individual recent-price events will be published continuously for a short demonstration.

### Raw and curated data

- **Raw data** preserves the source-shaped fields so an engineer can later inspect what the provider returned.
- **Curated data** has been renamed, typed, validated, and placed into the project's normalized model.

Keeping both helps answer two different questions:

- “What did the provider return?” → inspect raw JSON.
- “What can the application safely analyze?” → read curated Parquet.

Raw does not mean an exact copy of network bytes in this portfolio. yfinance returns a pandas table rather than an HTTP response body, so the collector converts that table into a faithful JSON-compatible row structure.

### Serialization

**Serialization** converts in-memory Python data into bytes that can be stored or transmitted.

This part uses:

- JSON for human-readable raw data and the audit summary; and
- Parquet for typed curated data.

### Apache Parquet

**Parquet** is a column-oriented file format designed for efficient storage and analytical reading. Column-oriented means values from the same column are stored together, which helps analytical engines read only the columns they need.

PyArrow converts the validated Pydantic records into an in-memory table and writes compressed Parquet bytes. See the [Apache Parquet overview](https://parquet.apache.org/) and [PyArrow Parquet documentation](https://arrow.apache.org/docs/python/parquet.html).

### S3 key and partition

An S3 **key** is the complete name of an object. A **partition** is a key segment that groups records by a useful value.

For example:

```text
curated/market_prices/as_of=2026-09-23/part-00000.parquet
```

`as_of=2026-09-23` is the partition. One logical batch date always produces the same five keys.

### Logical run and attempt

The run ID `batch-20260923` identifies the logical batch for September 23. If that batch is retried, the logical run ID remains the same even though the new attempt happens later.

This prevents a retry from creating a new directory merely because its wall-clock execution time changed.

### Content hash

A **hash** converts content into a fixed-length identifier. If the meaningful content is the same, its SHA-256 hash is the same.

Before uploading, the storage class asks S3 for the hash stored in the object's metadata:

- same hash → report `unchanged` and skip the upload;
- different or missing hash → write the object.

For curated records, the hash excludes `ingested_at`. A retry naturally has a later ingestion timestamp, but that alone does not make the business observations different.

### Audit summary

An **audit summary** is a small JSON document describing the logical run. Ours records:

- the run ID;
- the `as_of` date;
- accepted record counts; and
- the four data-object keys.

It does not duplicate every source field or create a second logging system.

### Library-supported retries

The code does not implement retry loops manually.

- HTTPX retries connection failures while contacting FRED.
- Boto3 uses its standard retry mode for temporary AWS errors.
- yfinance performs its own HTTP work behind its documented download function.

Retries remain bounded so a broken source fails clearly instead of waiting forever.

---

## 3. Before You Start

### Complete Parts 1 and 2

You need:

- `config/sources.yml` from Part 1;
- `MarketPrice` and `EconomicObservation` in `models.py`;
- `.env` containing `FRED_API_KEY`;
- the two saved fixtures under `tests/fixtures/`; and
- the S3 bucket created in Part 2.

If the fixtures are missing, run the two Part 1 exploration commands again:

```bash
make explore-yfinance
make explore-fred
```

The exact source values may differ from the Part 1 examples. The tests care about the structure and normalization rules, not specific market prices.

### Renew the AWS session

Use the temporary profiles from Part 2:

```bash
aws login --profile market-pulse-login
export AWS_PROFILE=market-pulse-dev
export AWS_REGION=ap-southeast-1
aws sts get-caller-identity
```

The returned identity must belong to the intended development account and must not be the root user.

### Read the bucket name from Terraform

From the repository root:

```bash
export DATA_BUCKET="$(terraform -chdir=infrastructure/terraform output -raw bucket_name)"
echo "$DATA_BUCKET"
```

`DATA_BUCKET` is a resource name, not a credential. The AWS SDK still obtains temporary credentials through `AWS_PROFILE`.

### Add the new dependencies

Run:

```bash
uv add boto3 pyarrow pyyaml
```

| Dependency | Responsibility |
| --- | --- |
| `boto3` | Uses the AWS SDK for Python to read and write S3 objects |
| `pyarrow` | Builds typed tables and encodes Parquet |
| `pyyaml` | Reads the small source scope from `config/sources.yml` |

Do not add a retry package, CLI framework, or generic data platform library. The existing standard library and focused dependencies are enough.

### Create the collector package

```bash
mkdir -p src/market_pulse/collectors
```

Create `src/market_pulse/collectors/__init__.py`:

```python
"""Concrete source collectors."""
```

---

## 4. Decide the S3 Paths Before Writing Code

The batch will produce exactly five objects for one `as_of` date:

| Data | S3 key pattern |
| --- | --- |
| Raw market response | `raw/yfinance/as_of=YYYY-MM-DD/prices.json` |
| Raw FRED response | `raw/fred/as_of=YYYY-MM-DD/observations.json` |
| Curated market records | `curated/market_prices/as_of=YYYY-MM-DD/part-00000.parquet` |
| Curated economic records | `curated/economic_observations/as_of=YYYY-MM-DD/part-00000.parquet` |
| Audit summary | `audit/as_of=YYYY-MM-DD/summary.json` |

The raw paths are organized by provider. The curated paths are organized by the normalized dataset because downstream consumers should not need to know every source-specific response detail.

`part-00000.parquet` looks similar to a distributed processing output name. This batch writes only one file per dataset, but using a part filename makes it natural for Spark or another engine to write several files later.

---

## 5. Implement the yfinance Collector

Create `src/market_pulse/collectors/market.py` in three chunks.

### 5.1 Convert pandas values into raw JSON rows

Start with the imports and conversion helpers:

```python
from datetime import date, datetime, timedelta
from typing import Any, Literal

import pandas as pd
import yfinance as yf

from market_pulse.models import MarketPrice


def _python_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    return value


def dataframe_to_rows(
    frame: pd.DataFrame,
    *,
    symbol: str,
    interval: Literal["1d", "5m"],
) -> list[dict[str, Any]]:
    reset = frame.copy() if isinstance(frame.index, pd.RangeIndex) else frame.reset_index().copy()
    reset.columns = [str(column).lower().replace(" ", "_") for column in reset.columns]

    time_column = "datetime" if "datetime" in reset.columns else "date"
    rows: list[dict[str, Any]] = []

    for source_row in reset.to_dict(orient="records"):
        values = {key: _python_value(value) for key, value in source_row.items()}
        timestamp = pd.Timestamp(values.pop(time_column))
        values.pop("symbol", None)

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
```

This function performs source-shape work only:

- moves the DataFrame index into `event_time`;
- makes field names predictable;
- converts NumPy and pandas scalar values into normal Python values;
- represents missing values as `None`; and
- preserves or adds the New York market timezone.

The timezone fallback is mainly for the saved CSV fixture, where pandas may read the date without timezone information. Live yfinance timestamps requested with `ignore_tz=False` retain their source timezone.

### 5.2 Convert the raw rows into validated models

Continue in the same file:

```python
def normalize_market_rows(
    rows: list[dict[str, Any]],
    *,
    ingested_at: datetime,
) -> list[MarketPrice]:
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
```

This is the boundary between provider-shaped data and project-owned data. Pydantic now enforces the price, volume, timestamp, and OHLC rules created in Part 1.

The code does not silently drop a row that fails validation. A bad historical batch stops with a visible error so it can be understood before partial curated data is published.

### 5.3 Request one bounded historical window

Finish `market.py`:

```python
def collect_market_prices(
    symbols: list[str],
    *,
    as_of: date,
    ingested_at: datetime,
) -> tuple[dict[str, Any], list[MarketPrice]]:
    start = as_of - timedelta(days=365)
    end_exclusive = as_of + timedelta(days=1)
    raw_rows: list[dict[str, Any]] = []

    for symbol in symbols:
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
```

yfinance treats `end` as exclusive, so the request uses the day after `as_of`. One explicit request per symbol keeps the resulting columns flat and makes a source failure identify the affected symbol.

This is intentionally not a generic `SourceAdapter` class. The function name and return values already explain what it does.

---

## 6. Implement the FRED Collector

Create `src/market_pulse/collectors/fred.py` in three chunks.

### 6.1 Add the safe HTTP helper

Start with:

```python
from datetime import date, datetime, timedelta
from typing import Any

import httpx

from market_pulse.models import EconomicObservation

BASE_URL = "https://api.stlouisfed.org/fred/"


def _request_json(
    client: httpx.Client,
    path: str,
    *,
    api_key: str,
    params: dict[str, str | int],
) -> dict[str, Any]:
    response = client.get(
        path,
        params={"api_key": api_key, "file_type": "json", **params},
    )
    if response.is_error:
        raise RuntimeError(f"FRED returned HTTP {response.status_code} for {path}")
    return response.json()
```

The error includes the endpoint and status code but not the full request URL, because the query string contains the FRED key.

### 6.2 Normalize observations and the missing marker

Continue:

```python
def normalize_fred_series(
    series_payloads: dict[str, dict[str, Any]],
    *,
    ingested_at: datetime,
) -> list[EconomicObservation]:
    records: list[EconomicObservation] = []

    for series_id, payload in series_payloads.items():
        for row in payload["observations"]:
            records.append(
                EconomicObservation(
                    series_id=series_id,
                    observation_date=date.fromisoformat(row["date"]),
                    value=None if row["value"] == "." else float(row["value"]),
                    realtime_start=date.fromisoformat(row["realtime_start"]),
                    realtime_end=date.fromisoformat(row["realtime_end"]),
                    ingested_at=ingested_at,
                )
            )

    return records
```

The important line is the explicit conversion of `.` to `None`. Zero would mean a measured economic value of zero, which is different from a missing observation.

The real-time start and end dates remain in every normalized record so FRED revisions are distinguishable.

### 6.3 Request metadata and one year of observations

Finish `fred.py`:

```python
def collect_fred_observations(
    series_ids: list[str],
    *,
    api_key: str,
    as_of: date,
    ingested_at: datetime,
) -> tuple[dict[str, Any], list[EconomicObservation]]:
    observation_start = as_of - timedelta(days=365)
    series_payloads: dict[str, dict[str, Any]] = {}
    transport = httpx.HTTPTransport(retries=2)

    with httpx.Client(
        base_url=BASE_URL,
        timeout=20.0,
        transport=transport,
        headers={"User-Agent": "market-pulse-portfolio/0.1"},
    ) as client:
        for series_id in series_ids:
            metadata = _request_json(
                client,
                "series",
                api_key=api_key,
                params={"series_id": series_id},
            )
            observations = _request_json(
                client,
                "series/observations",
                api_key=api_key,
                params={
                    "series_id": series_id,
                    "observation_start": observation_start.isoformat(),
                    "observation_end": as_of.isoformat(),
                    "realtime_start": as_of.isoformat(),
                    "realtime_end": as_of.isoformat(),
                    "sort_order": "asc",
                },
            )
            series_payloads[series_id] = {
                "metadata": metadata["seriess"][0],
                "observations": observations["observations"],
            }

    raw_payload = {
        "provider": "fred",
        "window": {
            "observation_start": observation_start.isoformat(),
            "observation_end": as_of.isoformat(),
            "realtime_date": as_of.isoformat(),
        },
        "series": series_payloads,
    }
    records = normalize_fred_series(series_payloads, ingested_at=ingested_at)
    return raw_payload, records
```

The observation window is bounded to one year, matching the market window closely enough for the portfolio question. FRED series still retain their natural frequency: daily DGS10 and monthly CPIAUCSL are not artificially expanded to the same calendar.

`HTTPTransport(retries=2)` retries connection errors and connection timeouts. It does not create a custom policy for every HTTP status. See the [HTTPX transport documentation](https://www.python-httpx.org/advanced/transports/).

---

## 7. Print the Normalized Data Before Adding Storage

At this point, both collectors should work independently of S3. Observe that first.

Create `scripts/preview_batch_sources.py`:

```python
from datetime import UTC, datetime
from pathlib import Path

import yaml

from market_pulse.collectors.fred import collect_fred_observations
from market_pulse.collectors.market import collect_market_prices
from market_pulse.config import get_settings


def main() -> None:
    settings = get_settings()
    if settings.fred_api_key is None:
        raise SystemExit("Set FRED_API_KEY in .env before running this script")

    config = yaml.safe_load(Path("config/sources.yml").read_text(encoding="utf-8"))
    symbols = [item["symbol"] for item in config["market"]["symbols"]]
    series_ids = [item["series_id"] for item in config["economics"]["series"]]
    ingested_at = datetime.now(UTC)
    as_of = ingested_at.date()

    _, market_records = collect_market_prices(
        symbols,
        as_of=as_of,
        ingested_at=ingested_at,
    )
    print(f"Market records: {len(market_records)}")
    print(market_records[0].model_dump_json(indent=2))

    _, economic_records = collect_fred_observations(
        series_ids,
        api_key=settings.fred_api_key.get_secret_value(),
        as_of=as_of,
        ingested_at=ingested_at,
    )
    print(f"\nEconomic observations: {len(economic_records)}")
    print(economic_records[0].model_dump_json(indent=2))


if __name__ == "__main__":
    main()
```

Run it from the repository root:

```bash
uv run python scripts/preview_batch_sources.py
```

The request can take several seconds because six symbols and two FRED series are collected. The result should resemble:

```text
Market records: approximately 1,500
{
  "symbol": "AAPL",
  "interval": "1d",
  "event_time": "...",
  ...
}

Economic observations: varies by source calendar
{
  "series_id": "DGS10",
  "observation_date": "...",
  "value": ...,
  ...
}
```

Do not compare prices or counts exactly. Confirm instead that:

- all configured symbols produced records;
- market timestamps include `Z` after model normalization to UTC;
- the FRED series IDs are present;
- values are numbers or `null`; and
- no key or AWS credential appears in the output.

Only after these records look correct should storage be added.

---

## 8. Encode Parquet and Write S3 Objects

Create `src/market_pulse/storage.py` in two chunks.

### 8.1 Add deterministic JSON, hashes, and Parquet

Start with:

```python
import json
from collections.abc import Sequence
from hashlib import sha256
from io import BytesIO
from typing import Any, Literal

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import ClientError
from pydantic import BaseModel


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()


def logical_records_hash(records: Sequence[BaseModel]) -> str:
    rows = [record.model_dump(mode="json", exclude={"ingested_at"}) for record in records]
    return sha256_bytes(canonical_json_bytes(rows))


def records_to_parquet(records: Sequence[BaseModel]) -> bytes:
    if not records:
        raise ValueError("cannot encode an empty record collection")

    rows = [record.model_dump(mode="python") for record in records]
    table = pa.Table.from_pylist(rows)
    output = BytesIO()
    pq.write_table(table, output, compression="snappy")
    return output.getvalue()
```

Canonical JSON always sorts object keys and removes insignificant spaces. That gives equivalent JSON structures the same bytes and therefore the same hash.

The curated logical hash excludes only `ingested_at`. Stable event and observation IDs remain included.

`BytesIO` is an in-memory byte buffer. The dataset is small enough that writing one Parquet file in memory keeps this implementation direct and readable.

### 8.2 Add one concrete S3 storage class

Finish `storage.py`:

```python
class S3Storage:
    def __init__(self, bucket: str) -> None:
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            config=Config(retries={"mode": "standard", "total_max_attempts": 3}),
        )

    def put_if_changed(
        self,
        *,
        key: str,
        body: bytes,
        content_type: str,
        logical_hash: str,
    ) -> Literal["written", "unchanged"]:
        try:
            existing = self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code")
            if code not in {"404", "NoSuchKey", "NotFound"}:
                raise
        else:
            metadata = existing.get("Metadata", {})
            if metadata.get("logical-sha256") == logical_hash:
                return "unchanged"

        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body,
            ContentType=content_type,
            Metadata={"logical-sha256": logical_hash},
        )
        return "written"
```

`head_object` reads metadata without downloading the object body. A missing object is normal on the first run. Other errors, including denied access, are raised rather than disguised as missing data.

Boto3's `standard` retry mode handles a bounded set of temporary AWS failures, and `total_max_attempts=3` includes the first request. See the [official Boto3 retry guide](https://docs.aws.amazon.com/boto3/latest/guide/retries.html).

There is no local-storage interface or fake S3 class. The pure JSON, hash, and Parquet functions can be tested directly, while one live batch demonstrates the actual S3 path.

---

## 9. Assemble the Batch Command

Create `src/market_pulse/batch.py` in four chunks.

### 9.1 Add imports, stable keys, and source configuration

Start with:

```python
import argparse
import os
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml

from market_pulse.collectors.fred import collect_fred_observations
from market_pulse.collectors.market import collect_market_prices
from market_pulse.config import get_settings
from market_pulse.storage import (
    S3Storage,
    canonical_json_bytes,
    logical_records_hash,
    records_to_parquet,
    sha256_bytes,
)


def build_keys(as_of: date) -> dict[str, str]:
    partition = f"as_of={as_of.isoformat()}"
    return {
        "market_raw": f"raw/yfinance/{partition}/prices.json",
        "fred_raw": f"raw/fred/{partition}/observations.json",
        "market_curated": (f"curated/market_prices/{partition}/part-00000.parquet"),
        "economic_curated": (f"curated/economic_observations/{partition}/part-00000.parquet"),
        "audit": f"audit/{partition}/summary.json",
    }


def load_scope(path: Path = Path("config/sources.yml")) -> tuple[list[str], list[str]]:
    config: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8"))
    symbols = [item["symbol"] for item in config["market"]["symbols"]]
    series_ids = [item["series_id"] for item in config["economics"]["series"]]
    return symbols, series_ids
```

The key builder is pure: the same date always returns the same paths. The configuration loader extracts only what this batch needs rather than introducing a hierarchy of configuration classes.

### 9.2 Add the small command-line interface

Continue:

```python
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect batch data and write it to S3.")
    parser.add_argument("--bucket", default=os.getenv("DATA_BUCKET"))
    parser.add_argument("--as-of", type=date.fromisoformat, default=datetime.now(UTC).date())
    args = parser.parse_args()
    if not args.bucket:
        parser.error("provide --bucket or set DATA_BUCKET")
    return args
```

The standard-library `argparse` module is enough for two inputs. The bucket can come from `DATA_BUCKET`, and `--as-of` accepts the clear ISO format `YYYY-MM-DD`.

### 9.3 Collect, display, and encode both datasets

Continue with the beginning of `main`:

```python
def main() -> None:
    args = parse_args()
    settings = get_settings()
    if settings.fred_api_key is None:
        raise SystemExit("Set FRED_API_KEY in .env before running the batch")

    symbols, series_ids = load_scope()
    ingested_at = datetime.now(UTC)
    keys = build_keys(args.as_of)

    market_raw, market_records = collect_market_prices(
        symbols,
        as_of=args.as_of,
        ingested_at=ingested_at,
    )
    fred_raw, economic_records = collect_fred_observations(
        series_ids,
        api_key=settings.fred_api_key.get_secret_value(),
        as_of=args.as_of,
        ingested_at=ingested_at,
    )

    print(f"Collected {len(market_records)} market-price records")
    print(market_records[0].model_dump_json(indent=2))
    print(f"\nCollected {len(economic_records)} economic observations")
    print(economic_records[0].model_dump_json(indent=2))

    market_raw_bytes = canonical_json_bytes(market_raw)
    fred_raw_bytes = canonical_json_bytes(fred_raw)
    market_parquet = records_to_parquet(market_records)
    economic_parquet = records_to_parquet(economic_records)
```

One ingestion timestamp is shared by all records from the attempt. The logical hashes later ignore that field, but retaining it in Parquet still shows when the stored version was produced.

### 9.4 Upload the four datasets and audit summary

Finish the `main` function and file:

```python
    storage = S3Storage(args.bucket)
    objects = [
        (
            "market raw",
            keys["market_raw"],
            market_raw_bytes,
            "application/json",
            sha256_bytes(market_raw_bytes),
        ),
        (
            "FRED raw",
            keys["fred_raw"],
            fred_raw_bytes,
            "application/json",
            sha256_bytes(fred_raw_bytes),
        ),
        (
            "market curated",
            keys["market_curated"],
            market_parquet,
            "application/vnd.apache.parquet",
            logical_records_hash(market_records),
        ),
        (
            "economic curated",
            keys["economic_curated"],
            economic_parquet,
            "application/vnd.apache.parquet",
            logical_records_hash(economic_records),
        ),
    ]

    for label, key, body, content_type, logical_hash in objects:
        result = storage.put_if_changed(
            key=key,
            body=body,
            content_type=content_type,
            logical_hash=logical_hash,
        )
        print(f"{label}: {result} -> s3://{args.bucket}/{key}")

    audit = {
        "run_id": f"batch-{args.as_of:%Y%m%d}",
        "as_of": args.as_of.isoformat(),
        "counts": {
            "market_prices": len(market_records),
            "economic_observations": len(economic_records),
        },
        "objects": {name: key for name, key in keys.items() if name != "audit"},
    }
    audit_bytes = canonical_json_bytes(audit)
    audit_result = storage.put_if_changed(
        key=keys["audit"],
        body=audit_bytes,
        content_type="application/json",
        logical_hash=sha256_bytes(audit_bytes),
    )
    print(f"audit: {audit_result} -> s3://{args.bucket}/{keys['audit']}")


if __name__ == "__main__":
    main()
```

The four data objects are written before the audit summary. If a source or upload fails, the command exits without publishing a misleading successful summary.

The audit document omits attempt time and write status. As a result, an unchanged retry produces the same audit bytes and can also be skipped.

---

## 10. Add Focused Fixture Tests

Create `tests/unit/test_batch_collection.py`. The tests use the small public fixtures created during Part 1 and do not call the internet or AWS.

### 10.1 Add imports and fixture helpers

Start with:

```python
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
    frame = pd.read_csv(FIXTURES / "market_prices.csv")
    rows = dataframe_to_rows(frame, symbol="AAPL", interval="1d")
    return normalize_market_rows(rows, ingested_at=INGESTED_AT)


def fred_payload():
    return json.loads((FIXTURES / "fred_observations.json").read_text())
```

The fixture helpers keep each test focused on one behavior. They are test code, not a second production data-access layer.

### 10.2 Test source normalization

Continue:

```python
def test_market_fixture_normalizes_rows() -> None:
    records = market_records()
    assert len(records) > 0
    assert records[0].symbol == "AAPL"
    assert records[0].event_time.tzinfo is UTC


def test_fred_missing_marker_becomes_none() -> None:
    payload = copy.deepcopy(fred_payload())
    payload["DGS10"]["observations"][0]["value"] = "."
    records = normalize_fred_series(payload, ingested_at=INGESTED_AT)
    dgs10 = next(record for record in records if record.series_id == "DGS10")
    assert dgs10.value is None
```

The FRED fixture may not naturally contain a missing value on the day it was saved. The test changes a copied observation to the documented `.` marker so the rule is always exercised without altering the fixture file.

### 10.3 Test Parquet and idempotency decisions

Finish the file:

```python
def test_market_records_round_trip_through_parquet() -> None:
    records = market_records()
    parquet_bytes = records_to_parquet(records)
    table = pq.read_table(BytesIO(parquet_bytes))
    assert table.num_rows == len(records)
    assert "event_id" in table.column_names


def test_logical_hash_ignores_ingestion_time() -> None:
    first = market_records()
    later = [
        record.model_copy(update={"ingested_at": datetime(2026, 9, 23, 9, 0, tzinfo=UTC)})
        for record in first
    ]
    assert logical_records_hash(first) == logical_records_hash(later)


def test_batch_keys_are_stable_for_the_same_date() -> None:
    keys = build_keys(date(2026, 9, 23))
    assert keys["market_raw"] == "raw/yfinance/as_of=2026-09-23/prices.json"
    assert keys == build_keys(date(2026, 9, 23))
```

The Parquet test performs a real in-memory write and read. It checks the behavior that matters without snapshotting binary bytes that may change across compatible PyArrow versions.

Run all tests from Parts 1 and 3:

```bash
uv run pytest -q
```

Expected result:

```text
.............                                                            [100%]
13 passed in ...s
```

Your Part 1 fixture contains five market rows instead of the two rows used during this guide's isolated validation, so `len(records) > 0` intentionally avoids a brittle exact count.

Run the normal code check as well:

```bash
uv run ruff check .
```

Expected result:

```text
All checks passed!
```

---

## 11. Add the Everyday Commands

Update the `.PHONY` line in `Makefile` so it includes `preview-batch` and `batch`:

```makefile
.PHONY: setup kafka-up kafka-topics kafka-down explore-yfinance explore-fred preview preview-batch batch test lint format check
```

Add these targets below the existing `preview` target:

```makefile
preview-batch:
	uv run python scripts/preview_batch_sources.py

batch:
	uv run python -m market_pulse.batch
```

The batch target reads `DATA_BUCKET` and `AWS_PROFILE` from the current terminal. It does not hide authentication or invent its own environment-loading system.

You can now use:

```bash
make preview-batch
make check
make batch
```

---

## 12. Run the Batch and Observe S3

Make sure the required values exist in the current terminal:

```bash
echo "$AWS_PROFILE"
echo "$AWS_REGION"
echo "$DATA_BUCKET"
```

None should be empty. `FRED_API_KEY` remains in the ignored `.env` file and should not be printed.

Choose the UTC date for this logical batch:

```bash
export AS_OF="$(date -u +%F)"
echo "$AS_OF"
```

### 12.1 Run it once

```bash
uv run python -m market_pulse.batch \
  --bucket "$DATA_BUCKET" \
  --as-of "$AS_OF"
```

After the two sample records, the final lines should resemble:

```text
market raw: written -> s3://.../raw/yfinance/as_of=YYYY-MM-DD/prices.json
FRED raw: written -> s3://.../raw/fred/as_of=YYYY-MM-DD/observations.json
market curated: written -> s3://.../curated/market_prices/as_of=YYYY-MM-DD/part-00000.parquet
economic curated: written -> s3://.../curated/economic_observations/as_of=YYYY-MM-DD/part-00000.parquet
audit: written -> s3://.../audit/as_of=YYYY-MM-DD/summary.json
```

### 12.2 List the resulting objects

```bash
aws s3 ls "s3://${DATA_BUCKET}/" --recursive
```

You should see the five keys defined in Section 4, plus the Part 2 smoke-test README if you kept it.

Read the small audit summary directly:

```bash
aws s3 cp \
  "s3://${DATA_BUCKET}/audit/as_of=${AS_OF}/summary.json" \
  -
```

The JSON should contain one run ID, two counts, and four data-object keys.

### 12.3 Run the same logical batch again

Use the exact same command and `AS_OF`:

```bash
uv run python -m market_pulse.batch \
  --bucket "$DATA_BUCKET" \
  --as-of "$AS_OF"
```

If neither provider changed its historical response between attempts, the final lines now report:

```text
market raw: unchanged -> ...
FRED raw: unchanged -> ...
market curated: unchanged -> ...
economic curated: unchanged -> ...
audit: unchanged -> ...
```

List S3 again:

```bash
aws s3 ls "s3://${DATA_BUCKET}/" --recursive
```

The same five logical batch keys remain; there is no new timestamp-named directory.

If one source publishes a correction between the two attempts, the related objects can correctly report `written`. They still use the same stable key, and S3 versioning from Part 2 retains the previous object version. That is a source change, not a duplicate logical batch.

---

## 13. Final Part 3 Structure

The relevant repository structure is now:

```text
market-pulse/
├── config/
│   └── sources.yml
├── infrastructure/
│   └── terraform/
├── scripts/
│   ├── explore_fred.py
│   ├── explore_yfinance.py
│   ├── preview_batch_sources.py
│   └── preview_models.py
├── src/
│   └── market_pulse/
│       ├── collectors/
│       │   ├── __init__.py
│       │   ├── fred.py
│       │   └── market.py
│       ├── __init__.py
│       ├── batch.py
│       ├── config.py
│       ├── models.py
│       ├── storage.py
│       └── py.typed
├── tests/
│   ├── fixtures/
│   │   ├── fred_observations.json
│   │   └── market_prices.csv
│   └── unit/
│       ├── test_batch_collection.py
│       └── test_models.py
├── .env
├── .env.example
├── .gitignore
├── Makefile
├── pyproject.toml
└── uv.lock
```

The S3 structure for one date resembles:

```text
s3://<data-bucket>/
├── raw/
│   ├── fred/as_of=YYYY-MM-DD/observations.json
│   └── yfinance/as_of=YYYY-MM-DD/prices.json
├── curated/
│   ├── economic_observations/as_of=YYYY-MM-DD/part-00000.parquet
│   └── market_prices/as_of=YYYY-MM-DD/part-00000.parquet
└── audit/
    └── as_of=YYYY-MM-DD/summary.json
```

---

## 14. Common Problems

### A fixture file is missing

Run the corresponding Part 1 exploration script:

```bash
make explore-yfinance
make explore-fred
```

Then confirm both files exist under `tests/fixtures/` before rerunning pytest.

### yfinance reports no data for a symbol

Confirm the symbol in `config/sources.yml`, your network connection, and the chosen `as_of` date. A future date or temporary Yahoo Finance problem can produce an empty response.

The collector stops instead of publishing a partial six-symbol batch without explanation.

### FRED returns HTTP 400 or 403

Check that `.env` contains a valid `FRED_API_KEY` with no extra spaces. Do not add the key to the command line, because shell history could retain it.

### AWS reports expired credentials

Renew the temporary session and export the process profile again:

```bash
aws login --profile market-pulse-login
export AWS_PROFILE=market-pulse-dev
export AWS_REGION=ap-southeast-1
aws sts get-caller-identity
```

### The batch says `provide --bucket or set DATA_BUCKET`

Read the Terraform output again:

```bash
export DATA_BUCKET="$(terraform -chdir=infrastructure/terraform output -raw bucket_name)"
```

Then rerun the batch.

### S3 returns `AccessDenied`

First check the caller with `aws sts get-caller-identity`. During local development, the `market-pulse-dev` profile from Part 2 performs the upload.

The narrower pipeline policy created in Part 2 is intended for a deployed runtime identity later; it is not a credential by itself.

### The second run says `written` instead of `unchanged`

Compare which object labels changed. A provider may have published a new observation, corrected a historical value, or changed response metadata between attempts.

Run `aws s3 ls` and confirm that the stable keys remain the same. A meaningful source correction should update the existing logical key rather than create a duplicate dated by attempt time.

### PyArrow cannot encode an empty collection

Read the earlier collector output. This error protects the pipeline from publishing an empty Parquet file after a source unexpectedly returned no accepted records.

---

## 15. Review and Commit

Run the normal local checks:

```bash
make check
git status --short
```

Read the changed-file list. It should include source code, tests, `pyproject.toml`, `uv.lock`, and the Makefile. It must not include `.env`, AWS credential files, Terraform state, or downloaded S3 data.

Commit the completed vertical slice:

```bash
git add src scripts tests Makefile pyproject.toml uv.lock
git commit -m "Build the batch collection pipeline"
```

---

## 16. What You Can Explain After This Part

You now have a concise interview explanation:

> I first normalized both sources locally and printed representative records before adding storage. Each logical date writes source-shaped JSON, validated Parquet, and one audit summary to stable S3 keys. The writer stores a logical content hash in S3 metadata, so an unchanged retry skips the upload. A real source correction updates the same key, while S3 versioning preserves the prior object. I kept one concrete collector per source because two sources did not justify a generic adapter framework.

You should be able to demonstrate:

- the source-specific normalization code;
- FRED's `.` missing marker becoming `None`;
- a real Parquet round trip in pytest;
- raw, curated, and audit objects in S3;
- the first run reporting `written`; and
- an unchanged rerun reporting `unchanged` without creating new logical keys.

Part 4 will reuse `MarketPrice` for recent-price Kafka events. It will not reuse the historical batch collector as a fake streaming system.

---

## 17. Reference Documentation

- [yfinance download parameters](https://ranaroussi.github.io/yfinance/reference/api/yfinance.download.html)
- [FRED series metadata endpoint](https://fred.stlouisfed.org/docs/api/fred/series.html)
- [FRED series observations endpoint](https://fred.stlouisfed.org/docs/api/fred/series_observations.html)
- [HTTPX transports and connection retries](https://www.python-httpx.org/advanced/transports/)
- [AWS SDK for Python documentation](https://docs.aws.amazon.com/boto3/latest/)
- [Boto3 retry configuration](https://docs.aws.amazon.com/boto3/latest/guide/retries.html)
- [Boto3 S3 client reference](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html)
- [Apache Parquet overview](https://parquet.apache.org/)
- [PyArrow Parquet documentation](https://arrow.apache.org/docs/python/parquet.html)
- [PyArrow `write_table`](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html)
