# CLAUDE.md

## Project overview

Bias-aware market data infrastructure for algo trading research, backtesting, and live trading. Alpha Vantage is the sole required data provider (historical setup + daily updates). Optionally enhanced with FirstRate Data for survivorship bias-free prices (16k+ tickers including 7k+ delisted, back to 2000).

## Core concepts

- **Point-in-time (PIT) fundamentals**: Homegrown daily snapshot pipeline captures fundamental data before restatements overwrite it. Raw data is append-only, never overwritten. Pre-collection history uses 90-day reporting lag approximation.
- **Survivorship bias**: FirstRate Data (optional, one-time purchase) adds delisted securities that Alpha Vantage doesn't cover.
- **Yield-aware API management**: Asset catalog tracks per-ticker, per-endpoint yield status. Empty tickers skipped daily, re-checked weekly. ~75 API calls/min budget.

## Architecture

- **GCP Cloud container** runs daily ingestion scripts, writes to a single GCS bucket (`gs://<project-id>-algo-trading/`)
- **Local sync script** mirrors GCS bucket contents for transformation and research
- All data stored as `.parquet` (both daily and historical)
- Restatement detection via `deepdiff` comparing new data against previous day's data

## Tech stack

- Python 3.10+
- `google-cloud-storage`, `pandas`, `polars`, `numpy`, `pyarrow`, `requests`, `deepdiff`, `beautifulsoup4`, `lxml`

## Folder structure

```
secrets/                      # NOT IN GIT - API keys, GCS credentials
config/                       # settings.py (paths, constants), gcp.py (GCP config)
asset_catalog_service/        # Ticker/asset catalogs and API yield tracking code
historical_data_setup/        # One-time historical data download (AV + optional FirstRate)
daily_data_service/           # Daily incremental AV pull (setup_daily) + weekend retry pass (adjust_weekly)
data_transformation/          # Transforms raw data into AssetData instances
scheduled_scripts/            # Cloud Run entrypoints (run_daily.py, run_weekend.py)
maintainance_scripts/         # Shared utility modules, GCS client, API key resolution
monitoring_service/           # End-of-run database snapshot + delta-vs-previous report
tests/                        # Unified test directory (one subdir per service, plus call_speedtests/ and integration_tests/)
```

## Data storage layout (GCS + local mirror)

- `catalog/` - Mutable ticker metadata + yield status (`.parquet`)
- `historical/` - One-time load, append-only. Per-ticker `.parquet` files. Subfolders: `stocks/{prices,prices_daily,income_statement,balance_sheet,cash_flow,earnings,earnings_estimates,insider,sentiment}`, `etfs/{prices,prices_daily,etf_profile}`, `forex/`, `indices/`, `cryptocurrencies/`, `commodities/`, `economic/`
- `daily/YYYY-MM-DD/` - Append-only daily pulls. Same subfolder structure as historical. Files are `.parquet`, one per ticker

## Key rules

- `daily/` is append-only - past days are never modified
- `catalog/` is the only mutable storage area
- Raw data in GCS is processed once, then never modified
- Historical data from FirstRate only overwrites Alpha Vantage data if overlapping data agrees; conflicts are flagged for review
- The yield status can tell what tickers are pulled daily
- No em dashes in log messages

## Alpha Vantage endpoints

Intraday prices (`TIME_SERIES_INTRADAY`), daily prices (`TIME_SERIES_DAILY_ADJUSTED`), fundamentals (`INCOME_STATEMENT`, `BALANCE_SHEET`, `CASH_FLOW`, `EARNINGS`, `EARNINGS_ESTIMATES`), `INSIDER_TRANSACTIONS`, `NEWS_SENTIMENT`, `INDEX_DATA`, `ETF_PROFILE`, commodities (WTI, BRENT, etc.), economic indicators (GDP, CPI, etc.)
