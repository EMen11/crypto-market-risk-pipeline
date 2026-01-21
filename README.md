
# Crypto Market Risk Pipeline (Databricks + Coinbase)

This project builds a Medallion (Bronze/Silver/Gold) pipeline on Databricks to ingest daily crypto market candles from the Coinbase Exchange API and generate risk analytics signals.

## Stack
- Databricks (Spark, Delta Lake, SQL)
- Python (ingestion, transforms, robust statistics)
- SQL (analytics queries)

## Data Source
- Coinbase Exchange API – product candles (daily granularity)

## Outputs (Gold)
- Daily log returns
- 30-day rolling volatility
- Robust anomaly flags (MAD Z-score)
