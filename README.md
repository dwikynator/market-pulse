# MarketPulse

A compact data-engineering portfolio project that combines daily batch data
with a small Kafka and Spark streaming path. The workload is intentionally
small; Kafka and Spark are included to demonstrate the engineering pattern,
not because six symbols require distributed infrastructure.

## Current scope

- Market prices: AAPL, JPM, JNJ, AMZN, CAT, and XOM from yfinance
- Economic context: DGS10 and CPIAUCSL from FRED
- Python 3.12 with uv
- Local Apache Kafka in Docker

Part 1 explores the sources and defines normalized data models. Later parts add
S3, collectors, Kafka producers, Spark Structured Streaming, Airflow,
Snowflake, and dbt in that order.

## Local setup

```bash
cp .env.example .env
# Add your FRED_API_KEY to .env.
make setup
make kafka-up
make explore-yfinance
make explore-fred
make preview
make check
```

Stop Kafka when you are finished:

```bash
make kafka-down
```

## Data-source note

yfinance is used for research and education. This project does not claim that
it is a production-grade licensed market-data feed. See `docs/data-sources.md`
for observed source behavior and normalization decisions.
