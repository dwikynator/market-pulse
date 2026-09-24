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
    """Construct deterministic S3 object keys partitioned by the as_of date."""
    partition = f"as_of={as_of.isoformat()}"
    return {
        "market_raw": f"raw/yfinance/{partition}/prices.json",
        "fred_raw": f"raw/fred/{partition}/observations.json",
        "market_curated": (f"curated/market_prices/{partition}/part-00000.parquet"),
        "economic_curated": (f"curated/economic_observations/{partition}/part-00000.parquet"),
        "audit": f"audit/{partition}/summary.json",
    }


def load_scope(path: Path = Path("config/sources.yml")) -> tuple[list[str], list[str]]:
    """Load configured market symbols and FRED series IDs from YAML config."""
    config: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8"))
    symbols = [item["symbol"] for item in config["market"]["symbols"]]
    series_ids = [item["series_id"] for item in config["economics"]["series"]]
    return symbols, series_ids


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for target S3 bucket and collection date."""
    parser = argparse.ArgumentParser(description="Collect batch data and write it to S3.")
    parser.add_argument("--bucket", default=os.getenv("DATA_BUCKET"))
    parser.add_argument("--as-of", type=date.fromisoformat, default=datetime.now(UTC).date())
    args = parser.parse_args()
    if not args.bucket:
        parser.error("provide --bucket or set DATA_BUCKET")
    return args


def main() -> None:
    """Execute batch collection, normalization, Parquet encoding, and S3 upload."""
    args = parse_args()
    settings = get_settings()
    if settings.fred_api_key is None:
        raise SystemExit("Set FRED_API_KEY in .env before running the batch")

    symbols, series_ids = load_scope()
    ingested_at = datetime.now(UTC)
    keys = build_keys(args.as_of)

    # Collect source datasets using a shared ingestion timestamp
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

    # Serialize raw payloads as canonical JSON and curated records as Parquet
    market_raw_bytes = canonical_json_bytes(market_raw)
    fred_raw_bytes = canonical_json_bytes(fred_raw)
    market_parquet = records_to_parquet(market_records)
    economic_parquet = records_to_parquet(economic_records)

    # Upload datasets idempotently using content hashes
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

    # Write audit summary only after all data objects are safely written
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

