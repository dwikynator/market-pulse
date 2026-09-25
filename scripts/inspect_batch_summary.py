import argparse
import json
import os
from datetime import date
from typing import Any

import boto3

REQUIRED_OBJECTS = {
    "market_raw",
    "fred_raw",
    "market_curated",
    "economic_curated",
}

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect one batch audit summary is S3.")
    parser.add_argument("--bucket", default=os.getenv("DATA_BUCKET"))
    parser.add_argument("--as-of", required=True, type=date.fromisoformat)
    args = parser.parse_args()
    if not args.bucket:
        parser.error("provide --bucket or set DATA_BUCKET")
    return args

def read_summary(bucket: str, as_of: date) -> tuple[str, dict[str, Any]]:
    key = f"audit/as_of={as_of.isoformat()}/summary.json"
    response = boto3.client("s3").get_object(Bucket=bucket, Key=key)
    summary = json.loads(response["Body"].read())
    return key, summary

def validate_summary(summary: dict[str, Any], as_of: date) -> None:
    if summary.get("as_of") != as_of.isoformat():
        raise ValueError("audit as_of does not match the requested date")

    counts = summary.get("counts", {})
    if not isinstance(counts.get("market_prices"), int):
        raise ValueError("audit is missing the market_prices count")
    if not isinstance(counts.get("economic_observations"), int):
        raise ValueError("audit is missing the economic_observations count")

    objects = summary.get("objects", {})
    missing = REQUIRED_OBJECTS - objects.keys()
    if missing:
        raise ValueError(f"audit is missing object keys: {sorted(missing)}")

def main() -> None:
    args = parse_args()
    key, summary = read_summary(args.bucket, args.as_of)
    validate_summary(summary, args.as_of)

    print(f"Batch {summary['run_id']} is ready for downstream loading")
    print(f"Audit: s3://{args.bucket}/{key}")
    print(f"Market prices: {summary['counts']['market_prices']}")
    print(f"Economic observations: {summary['counts']['economic_observations']}")
    for name, object_key in sorted(summary["objects"].items()):
        print(f"{name}: s3://{args.bucket}/{object_key}")

if __name__ == "__main__":
    main()
