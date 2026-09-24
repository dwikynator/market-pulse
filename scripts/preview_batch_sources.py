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
