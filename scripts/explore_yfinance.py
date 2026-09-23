from pathlib import Path

import pandas as pd
import yfinance as yf

SYMBOL = "AAPL"
FIXTURE_PATH = Path("tests/fixtures/market_prices.csv")


def show_frame(label: str, frame: pd.DataFrame) -> None:
    print(f"\n=== {label} ===")
    print(f"Rows: {len(frame)}")
    print(f"Columns: {list(frame.columns)}")
    print(f"Index name: {frame.index.name}")
    print(f"Index timezone: {getattr(frame.index, 'tz', None)}")
    print("\nData types:")
    print(frame.dtypes.to_string())
    print("\nFirst three rows:")
    print(frame.head(3).to_string())
    print(f"\nMissing values: {frame.isna().sum().to_dict()}")


def download_prices(*, period: str, interval: str) -> pd.DataFrame:
    frame = yf.download(
        tickers=SYMBOL,
        period=period,
        interval=interval,
        auto_adjust=False,
        actions=False,
        progress=False,
        threads=False,
        ignore_tz=False,
        multi_level_index=False,
        timeout=15,
    )
    if frame.empty:
        raise RuntimeError(f"yfinance returned no {interval} data for {SYMBOL}")
    return frame


def save_fixture(frame: pd.DataFrame) -> None:
    fixture = frame.reset_index().copy()
    fixture.columns = [str(column).lower().replace(" ", "_") for column in fixture.columns]
    fixture.insert(0, "symbol", SYMBOL)

    FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
    fixture.head(5).to_csv(FIXTURE_PATH, index=False)
    print(f"\nSaved five daily rows to {FIXTURE_PATH}")


def main() -> None:
    daily = download_prices(period="1mo", interval="1d")
    recent = download_prices(period="5d", interval="5m")

    show_frame("Daily prices", daily)
    show_frame("Five-minute prices", recent)
    save_fixture(daily)


if __name__ == "__main__":
    main()
