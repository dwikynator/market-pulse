import json
from pathlib import Path
from typing import Any

import httpx

from market_intelligence.config import get_settings

BASE_URL = "https://api.stlouisfed.org/fred/"
SERIES_IDS = ("DGS10", "CPIAUCSL")
FIXTURE_PATH = Path("tests/fixtures/fred_observations.json")


def request_json(
    client: httpx.Client,
    path: str,
    *,
    api_key: str,
    params: dict[str, str | int],
) -> dict[str, Any]:
    response = client.get(path, params={"api_key": api_key, "file_type": "json", **params})

    if response.is_error:
        print(response.json())
        raise RuntimeError(f"FRED returned HTTP {response.status_code} for {path}")
    return response.json()


def show_series(
    series_id: str,
    metadata_payload: dict[str, Any],
    observations_payload: dict[str, Any],
) -> dict[str, Any]:
    metadata = metadata_payload["seriess"][0]
    observations = observations_payload["observations"]
    missing_count = sum(row["value"] == "." for row in observations)

    print(f"\n=== {series_id}: {metadata['title']} ===")
    print(f"Frequency: {metadata['frequency']}")
    print(f"Units: {metadata['units']}")
    print(f"Observation fields: {list(observations[0])}")
    print(f"Missing-value markers in sample: {missing_count}")
    print("Latest three observations:")

    for row in observations[:3]:
        print(row)

    return {"metadata": metadata, "observations": observations}


def main() -> None:
    settings = get_settings()

    if settings.fred_api_key is None:
        raise SystemExit("Set FRED_API_KEY in .env before running this script")

    saved_series: dict[str, Any] = {}

    with httpx.Client(
        base_url=BASE_URL, timeout=20.0, headers={"User-Agent": "market-intelligence-portflio/0.1"}
    ) as client:
        for series_id in SERIES_IDS:
            metadata = request_json(
                client,
                "series",
                api_key=settings.fred_api_key.get_secret_value(),
                params={"series_id": series_id},
            )
            observations = request_json(
                client,
                "series/observations",
                api_key=settings.fred_api_key.get_secret_value(),
                params={"series_id": series_id, "sort_order": "desc", "limit": 10},
            )
            saved_series[series_id] = show_series(series_id, metadata, observations)

    FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE_PATH.write_text(
        json.dumps(saved_series, indent=2),
        encoding="utf-8",
    )
    print(f"\nSaved public sample data to {FIXTURE_PATH}")


if __name__ == "__main__":
    main()
