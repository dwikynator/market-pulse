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
    """Execute a GET request against the FRED API and return the JSON response."""
    response = client.get(
        path,
        params={"api_key": api_key, "file_type": "json", **params},
    )
    if response.is_error:
        # Include path instead of full URL to prevent exposing api_key in error logs
        raise RuntimeError(f"FRED returned HTTP {response.status_code} for {path}")
    return response.json()


def normalize_fred_series(
    series_payloads: dict[str, dict[str, Any]],
    *,
    ingested_at: datetime,
) -> list[EconomicObservation]:
    """Parse raw FRED observation responses into typed EconomicObservation domain models."""
    records: list[EconomicObservation] = []

    for series_id, payload in series_payloads.items():
        for row in payload["observations"]:
            records.append(
                EconomicObservation(
                    series_id=series_id,
                    observation_date=date.fromisoformat(row["date"]),
                    # FRED uses "." to represent missing/unreported observation values
                    value=None if row["value"] == "." else float(row["value"]),
                    # Retain realtime dates so revisions remain distinguishable
                    realtime_start=date.fromisoformat(row["realtime_start"]),
                    realtime_end=date.fromisoformat(row["realtime_end"]),
                    ingested_at=ingested_at,
                )
            )

    return records



def collect_fred_observations(
    series_ids: list[str],
    *,
    api_key: str,
    as_of: date,
    ingested_at: datetime,
) -> tuple[dict[str, Any], list[EconomicObservation]]:
    """Fetch 1-year economic observations from FRED with normalized models."""
    observation_start = as_of - timedelta(days=365)
    series_payloads: dict[str, dict[str, Any]] = {}
    transport = httpx.HTTPTransport(retries=2)

    with httpx.Client(
        base_url=BASE_URL,
        timeout=20.0,
        transport=transport,
        headers={"User-Agent": "market-pulse-portfolio/0.1"}
    ) as client:
        for series_id in series_ids:
            metadata = _request_json(
                client,
                "series",
                api_key=api_key,
                params={"series_id": series_id}
            )
            # Fetch point-in-time observations as known on the as_of date
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
                }
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

