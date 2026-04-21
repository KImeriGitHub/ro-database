# Algo Trading Database

A bias-aware market data infrastructure for quantitative strategy research, backtesting, and live trading. Built on Alpha Vantage as the sole required data provider — for both historical setup and ongoing daily updates — with a homegrown point-in-time snapshot pipeline. Optionally enhanced with FirstRate Data (survivorship bias-free prices for 16k+ tickers including delisted securities) to fill gaps that Alpha Vantage alone cannot cover.

## Why this project exists

Building a reliable algo trading database is harder than it looks. After evaluating 14 data providers - Alpha Vantage, Norgate Data, CRSP, Compustat, Kibot, Polygon.io, Databento, Tiingo, EODHD, Finnhub, Financial Modeling Prep, Nasdaq Data Link (Quandl), FirstRate Data, and QuantConnect - we found that no single affordable provider solves all the problems a serious quant needs solved.

The critical issues:

- **Survivorship bias in prices:** Most providers only include currently-listed stocks. Backtests on this data are overly optimistic because they exclude companies that went bankrupt, were acquired, or delisted.
- **Look-ahead bias in fundamentals:** Every retail-accessible fundamental data provider (Alpha Vantage, FMP, Finnhub, Tiingo, EODHD) serves the *latest* version of financial statements, silently overwriting restated values. Your backtest uses corrected numbers that weren't available at the time.
- **Point-in-time (PIT) fundamentals are institutional-only:** True PIT data - where original and restated values are preserved with timestamps - is only available from Compustat PIT ($5k–25k+/yr via WRDS), LSEG/Refinitiv PIT, or S&P Capital IQ Premium. None are accessible to independent researchers at retail pricing.
This project addresses these gaps with a practical, layered approach:

1. **Alpha Vantage** as the sole required data provider — used for both the initial historical data setup (downloading full price and fundamental history) and all ongoing daily updates (prices, fundamentals, alternative data), with a custom daily snapshot pipeline that builds a homegrown PIT layer over time.
2. **FirstRate Data** (optional) to enhance the Alpha Vantage historical data — adds survivorship bias-free prices (intraday + daily, including 7,000+ delisted tickers back to 2000) that Alpha Vantage does not cover. One-time purchase; no ongoing subscription.

The historical setup and the daily raw data pipeline are independent concerns. You can build a fully functional historical database from Alpha Vantage alone, and separately configure the daily pipeline. FirstRate Data, if purchased, is layered on top of the Alpha Vantage historical data to add delisted securities and intraday granularity.

## Data sources and rationale

### FirstRate Data (optional historical enhancement - one-time load)

**Why FirstRate Data:** It is the best combination of survivorship bias-free data, intraday granularity, data quality, and pricing to supplement the Alpha Vantage historical data. Key features:

- 16,245 stock tickers including 7,000+ delisted securities with full price history back to Jan 2000
- Covers all current and former S&P 500, NASDAQ 100, DJIA, and Russell 3000 members
- 1-minute, 5-minute, 30-minute, 1-hour, and daily bars
- Tick data available (10 years)
- Daily bars in three variants (unadjusted, split-adjusted, split+dividend-adjusted); 1-minute bars are unadjusted
- Out-of-hours (pre/post market) trades included
- Data sourced directly from major exchanges and 4 dark pools
- 5,150+ ETFs, 130 futures, 115 US indices, 110 international indices, 70 FX crosses, 50 crypto
- Used by NBER, Boston/Chicago Federal Reserve, Cambridge, NYU, Stanford
- Dedicated QA team since 2023; daily screening for gaps, duplicates, anomalies
- One-time purchase model - you own the data files permanently

**Our usage:** FirstRate Data is an optional one-time purchase that supplements the Alpha Vantage historical data. It adds survivorship bias-free prices for 7,000+ delisted securities and intraday bars that Alpha Vantage does not provide. After ingestion, we do not maintain a FirstRate subscription — Alpha Vantage handles all ongoing daily updates.

**Why not Norgate Data (the other survivorship-free option):**
Norgate also offers survivorship bias-free data with excellent historical index constituents, but requires Windows (or a Windows VM) for the NDU updater application, provides end-of-day data only (no intraday), and locks you into a subscription where data access stops if you lapse. FirstRate offers deeper intraday history, runs on any OS via flat CSV files, and uses a one-time purchase model.

### Alpha Vantage (sole required provider - historical setup + ongoing daily data)

**Why Alpha Vantage:** Broad coverage across prices, fundamentals, and alternative data at accessible pricing. NASDAQ-licensed for commercial use. Alpha Vantage is the only required data provider — it powers both the initial historical data setup (full price and fundamental history for all active tickers) and all ongoing daily updates. It provides pre- and post-market daily data.

**Data we pull:**

| Category | Endpoints | Update frequency |
|---|---|---|
| Intraday prices | `TIME_SERIES_INTRADAY` | Daily (active universe only) |
| Daily prices | `TIME_SERIES_DAILY_ADJUSTED` | Daily (active universe only) |
| Fundamentals | `INCOME_STATEMENT`, `BALANCE_SHEET`, `CASH_FLOW`, `EARNINGS`, `EARNINGS_ESTIMATES` | Daily snapshot (PIT pipeline) |
| Insider transactions | `INSIDER_TRANSACTIONS` | Daily |
| Market News & Sentiment | `NEWS_SENTIMENT` | Daily |
| Indices | `INDEX_DATA` — direct index prices (S&P 500, DJIA, VIX, etc.). Universe discovered via `INDEX_CATALOG`. | Daily |
| ETF profiles | `ETF_PROFILE` — net assets, holdings, expense ratio. Used to filter out low-net-asset ETFs. No historical PIT; latest snapshot only. | Daily |
| Commodities | `WTI`, `BRENT`, `NATURAL_GAS`, gold, silver, copper | Daily |
| Economic indicators | `REAL_GDP`, `CPI`, `UNEMPLOYMENT`, `FEDERAL_FUNDS_RATE`, treasury yields | Per release schedule |


### Providers we evaluated but did not select

| Provider | Reason not selected |
|---|---|
| **Norgate Data** | Excellent survivorship-free EOD and historical index constituents. But Windows-only (NDU updater), no intraday data, subscription-only (data inaccessible if lapsed), fundamentals are not PIT. FirstRate Data provides better intraday coverage on any OS. |
| **CRSP** | Gold standard for academic research, survivorship-free back to 1925. But ~$5k–25k+/yr institutional pricing, requires WRDS access. Not accessible to independents. |
| **Compustat** | Best fundamental database (99k securities, back to 1950). Offers true PIT from 1987. But same institutional pricing/access barrier as CRSP. |
| **Kibot** | Deep intraday history (28+ years, one-time ~$990). But mixed data quality reviews, no survivorship bias handling, no fundamentals, 8–12 hour update delay. FirstRate Data is higher quality with delisted tickers included. |
| **Polygon.io** | Excellent real-time/low-latency data. But $29–$499/mo subscription, no survivorship bias handling. Better suited as a live-trading feed, not a database backbone. |
| **Databento** | Institutional-grade tick data with nanosecond timestamps. But $199/mo+ for live, no fundamentals. Overkill for non-HFT strategies. |
| **Tiingo** | Great budget option ($10/mo), clean data. But no survivorship bias handling, limited fundamentals. |
| **EODHD** | Best value for global coverage ($19.99–$79.99/mo). But no PIT fundamentals, only partial survivorship handling. |
| **Finnhub** | Best free tier (60 calls/min), unique alternative data (congressional trades, lobbying). But shallow historical depth, no survivorship bias, unreliable WebSocket news, poor support. |
| **FMP** | Comprehensive fundamentals from SEC EDGAR. Has "as-reported" endpoints. But not true PIT. Survivorship-free EOD exists but is secondary to their fundamentals focus. |
| **QuantConnect** | All-in-one platform. But tick timestamps capped at milliseconds, fundamental survivorship bias gaps for delisted stocks, you don't own the data. |

## The point-in-time problem (and our solution)

### The problem

Every affordable fundamental data provider serves "latest available" financials. When a company restates its 2022 Q3 earnings in 2024, the data provider replaces the old Q3 2022 values with the corrected ones. Your backtest in 2022 then uses numbers that didn't exist until 2024.

Beyond restatements, there is a more fundamental issue: **financial data is never available on the fiscal period end date.** A company's fiscal quarter may end on December 31, but the 10-Q isn't filed with the SEC until weeks or months later — often mid-February or later. Without PIT awareness, a backtest can use December earnings data in a January trading signal, even though no market participant could have known those numbers yet. This reporting-lag bias means any backtest on fundamentals that doesn't account for actual data availability dates is effectively using future information to make past decisions.

Research by S&P Global shows this can move a company from the 5th percentile to the 88th percentile in ROE ranking - completely inverting a factor signal.

True PIT databases (Compustat PIT, LSEG/Refinitiv PIT, S&P Capital IQ Premium) cost $5k–25k+/year with institutional contracts.

### Our solution: build it ourselves

We run two daily snapshot pipelines:

**Fundamentals PIT pipeline:**
1. Pulls fundamental data from Alpha Vantage for every ticker in our universe
2. Stores each api return. Clearly indicates the `observed_date`
3. Never overwrites previous values

After several years of collection, this produces a genuine PIT dataset for the covered period. For the historical period before collection started, we apply a conservative 90-day reporting lag as an approximation.


## Data pipeline architecture

Daily data fetching runs in a **GCP Cloud container** (e.g., Cloud Run), not on the local machine. The container executes the ingestion scripts on a schedule and writes to a single GCS bucket (`gs://<project-id>-algo-trading/`), following the same folder structure described in [Data storage structure](#data-storage-structure). All raw files are append-only and never modified or deleted. This is the permanent record.

The one-time historical setup runs **locally** (it is a multi-hour job that benefits from local disk and easy restarts), and the resulting `historical/` and `catalog/` trees are pushed to the same GCS bucket once the setup finishes. After that initial upload, the container takes over for all ongoing daily work.

A local sync script downloads data from the GCS bucket to a local mirror. This local data is then transformed into `AssetData` instances (a standardized schema defining what information each asset should contain) and processed into features for strategy research.


### API call management

**Rate limit:** Alpha Vantage allows **75 API calls per minute**. The pipeline's sliding-window limiter is configured to **74 calls per minute**, leaving 1 call as a safety margin. Some of this budget may need to be reserved for live trading hours (8:00 AM – 5:00 PM ET), so daily batch ingestion should be scheduled outside this window when possible.

Full financial statements (income statement, balance sheet, cash flow) are saved as complete responses — no field filtering at ingestion time. To stay within Alpha Vantage API call budgets, the asset catalog tracks per-ticker, per-endpoint yield status:

- **Daily pulls** include only tickers that are known to return data for a given endpoint (e.g., `INCOME_STATEMENT`, `INSIDER_TRANSACTIONS`, `NEWS_SENTIMENT`).
- **Tickers that return empty or no data** are skipped during daily runs and re-checked on a weekly sweep to detect newly available coverage.
- **Tickers that previously returned data but stopped** (delisted, acquired, coverage dropped) are flagged in the catalog so they are no longer pulled daily.

This applies uniformly to financial statements, insider transactions, and news sentiment. The yield status metadata lives in the asset catalog alongside ticker lifecycle information (active/delisted, start/end dates).

**Restatement detection:** When daily data is transformed into `AssetData`, the new data is compared against the previous day's data using `deepdiff`. If values changed for a previously-recorded fiscal period, the change is flagged and incorporated into `AssetData`.

```
GCP Cloud Container                   GCS Bucket                          Local
┌──────────────────────┐        ┌──────────────────────────┐     ┌─────────────────────┐
│ Ingestion scripts    │        │ catalog/                 │     │ catalog/            │
│                      │──────► │ historical/              │     │ historical/         │
│ • Alpha Vantage pull │        │ daily/                   │◄───►│ daily/              │
│                      │        │                          │     │                     │
└──────────────────────┘        └──────────────────────────┘     │ Sync script pulls   │
                                                                 │ from GCS            │
                                                                 │                     │
                                                                 │ Process & transform │
                                                                 │ locally             │
                                                                 └─────────────────────┘
```

### Recovery and resume

The historical setup is a long-running job (tens of hours for a full Alpha Vantage pull) and is designed to be crash-tolerant. If the process dies for any reason -- OOM, network blip, unhandled exception, manual kill -- just rerun the same command.

- **File-level resume.** Each endpoint skips symbols that already have a parquet file on disk, so a restart only re-fetches what is missing. No task ledger or manual cleanup is required.
- **Stable start date across resumes.** The original run's start timestamp is preserved in `historical/.setup_started_at`, so the `data_complete_date` written to `yield_status` reflects when the setup actually began, not when it was last restarted.
- **Per-task isolation.** A failure in one `(asset_type, endpoint)` task does not abort the rest; other tasks keep running under the shared rate limiter and the failed one retries on the next run.
- **Finalize only on clean full runs.** `yield_status` is only finalized when the full setup completes with no subsetting flags, so a partial or failed run never corrupts the catalog.

See [historical_data_setup/README.md](historical_data_setup/README.md) for the full recovery behavior, including how to force a clean restart.

## Setup

### Prerequisites

- Python 3.10+ (any OS - no Windows dependency)
- Alpha Vantage API key (paid plan required - sole required provider for both historical setup and ongoing daily updates)
- FirstRate Data purchase (optional - Stocks Complete bundle recommended to add survivorship bias-free data and intraday bars to the historical database)
- **GCP account** with a project and billing enabled
- **GCP services:** Cloud Storage, Cloud Run (or equivalent container runtime), Secret Manager
- `gcloud` CLI installed and authenticated

### Python dependencies

All runtime dependencies (GCP client libraries, dataframe stack, HTML/parquet tooling) are pinned as minimums in [`requirements.txt`](requirements.txt). Install with `pip install -r requirements.txt`.

### API key resolution

`maintainance_scripts.get_api_key.get_alpha_vantage_key(tier)` tries the local `secrets/alpha_vantage_keys` file first. If the file is missing, the tier entry is absent, or the value is still a placeholder, it falls back to GCP Secret Manager **only when** the flag `USE_SECRET_MANAGER_FOR_AV_KEYS=true` is set (defined in `config/gcp.py`). The secret names it reads are also configurable from that module: `SECRET_AV_KEY_STANDARD` and `SECRET_AV_KEY_PREMIUM` (defaults: `alpha-vantage-key-standard`, `alpha-vantage-key-premium`). The container runs with this flag on; local dev keeps the default of off so runs fail loudly when the local file is misconfigured.


## Key design decisions

1. **Alpha Vantage as the sole required provider.** Alpha Vantage handles both the initial historical data setup (full price and fundamental history) and all ongoing daily updates (prices, fundamentals, alternative data). The system is fully functional with Alpha Vantage alone.

2. **Historical setup is independent of the daily pipeline.** Building the historical database and running daily data ingestion are separate concerns. You can set up history without configuring daily pulls, and vice versa.

3. **FirstRate Data as an optional historical enhancement.** If purchased, FirstRate Data adds survivorship bias-free prices (7,000+ delisted tickers, intraday bars back to 2000) on top of the Alpha Vantage historical data. No ongoing subscription needed.

4. **FirstRate Data over Norgate Data (if adding survivorship-free data).** Both offer survivorship bias-free prices. FirstRate wins on: any OS (no Windows lock-in), intraday data (Norgate is EOD-only), deeper intraday history (26 years of 1-min data). Norgate wins on: historical index constituents and history depth (back to 1950 vs 2000).

5. **Homegrown PIT over paying for institutional data.** True PIT databases cost $5k–25k+/year. Our daily snapshot approach builds PIT organically. The trade-off is a cold-start period of several years.

6. **Conservative reporting lag for pre-PIT history.** Before our collection started, we assume fundamentals weren't available until 90 days after the fiscal period end.

7. **Append-only storage for raw data.** Fundamental data is never updated in place. New values create new rows. This is the foundation of the PIT layer.

8. **GCP container for fetching, local for processing.** Daily ingestion runs in a GCP Cloud container that writes to a single append-only GCS bucket. A local sync script mirrors the bucket contents for further processing and transformation. This separates the reliability of cloud-based fetching from the flexibility of local analytical workflows.

9. **Raw data archived immutably in GCS.** Every API response is processed once into parquet and never modified. Schema violations are logged before data enters the pipeline.

10. **Yield-aware API call management.** The asset catalog tracks which tickers return data for each Alpha Vantage endpoint. Tickers with no data are skipped daily and re-checked weekly. This applies to financial statements, insider transactions, and news sentiment. Avoids wasting API calls on tickers where Alpha Vantage has no coverage.

## Folder structure

```
secrets/                      # NOT IN GIT - optional locally; container pulls from Secret Manager
├── alpha_vantage_keys        # Alpha Vantage API keys (standard= / premium=)
└── gcs_credentials.json      # GCP service-account key (local dev only; Cloud Run uses ADC)

config/
├── settings.py               # Local paths, constants (PIT_COLLECTION_START_DATE)
└── gcp.py                    # GCP project, region, bucket, instance config

asset_catalog_service/        # Manages ticker/asset catalogs and API yield tracking
                              # Catalog data and yield status live in storage (GCS + local),
                              # not in this directory - this is code only

historical_data_setup/        # Independent of the daily pipeline
                              # Downloads Alpha Vantage historical data (prices + fundamentals)
                              # Optionally ingests FirstRate Data to supplement Alpha Vantage history
                              # Transforms and moves data to raw data storages

daily_data_service/           # Daily incremental AV pull (mirrors historical_data_setup
                              # at a truncated recent window, no FirstRate Data).
                              # Writes parquet under daily/YYYY-MM-DD/, finalizes yield_status.

data_transformation/          # Transforms daily raw data into AssetData instances

scheduled_scripts/            # Orchestration scripts for download runs and API budget tracking
maintainance_scripts/         # common py files used throughout the repo

consistency_tests/            # Validates raw and transformed data against other sources
                              # e.g., checks that intraday open matches daily open

tests/                        # Unified test directory
├── asset_catalog_service/    # Tests for asset_catalog_service
├── historical_data_setup/    # Tests for historical_data_setup (placeholder)
└── call_speedtests/          # Scripts that measure real API call performance
```

## Data storage structure

Both GCS (archive + daily staging) and local storage follow the same directory layout. The `catalog/` directory contains mutable metadata, not immutable market data. The `historical/` section is populated during the initial setup (from Alpha Vantage, optionally supplemented with FirstRate Data) and is independent of the `daily/` section. Each day's Alpha Vantage pull creates a dated folder under `daily/`.

```
catalog/
├── stocks.parquet
├── etfs.parquet
├── indices.parquet
├── forex.parquet
├── cryptocurrencies.parquet
├── commodities.parquet
├── economic.parquet
├── yield_status.parquet
└── earnings_calendar.parquet

historical/
├── .setup_started_at            # mtime = original start time; preserved across resumes
├── ingestion_report.parquet     # per-run issue log (overwritten each run)
├── stocks/
│   ├── prices/
│   ├── prices_daily/
│   ├── income_statement/
│   ├── balance_sheet/
│   ├── cash_flow/
│   ├── earnings/
│   ├── earnings_estimates/
│   ├── insider/
│   └── sentiment/
├── etfs/
│   ├── prices/
│   ├── prices_daily/
│   └── etf_profile/
├── forex/
├── indices/
├── cryptocurrencies/
├── commodities/
└── economic/

daily/
├── .setup_started_at            # mtime = folder-date anchor; preserved across resumes
└── YYYY-MM-DD/
    ├── ingestion_report.parquet # per-run issue log for this date
    ├── stocks/
    │   ├── prices/
    │   ├── prices_daily/
    │   ├── income_statement/
    │   ├── balance_sheet/
    │   ├── cash_flow/
    │   ├── earnings/
    │   ├── earnings_estimates/
    │   ├── insider/
    │   └── sentiment/
    ├── etfs/
    │   ├── prices/
    │   ├── prices_daily/
    │   └── etf_profile/
    ├── forex/
    ├── indices/
    ├── cryptocurrencies/
    ├── commodities/
    └── economic/
```

### Directory details

**`catalog/`** — Asset catalog data, managed by `asset_catalog_service`. Each `.parquet` file tracks tickers/symbols with status, start date, and end date for its asset class (stocks, ETFs, indices, forex, cryptocurrencies, commodities, economic indicators).

- `yield_status.parquet` — Per-ticker, per-endpoint API yield tracking (has data / empty / stopped returning data).
- `earnings_calendar.parquet` — Future earnings dates.

**`historical/`** — Historical load, ideally append-only. Every row corresponds to one day and/or one minute, sorted. Best possible approximation of what people would have seen on that day. Files are compressed `.parquet`. Price endpoints (`prices`, `prices_daily`), `insider`, `sentiment`, and `etf_profile` use one file per ticker (e.g., `AAPL.parquet`). Fundamental endpoints (`income_statement`, `balance_sheet`, `cash_flow`, `earnings`, `earnings_estimates`) use two files per ticker: `SYMBOL_annual.parquet` and `SYMBOL_quarterly.parquet`.

| Subfolder | API endpoint | Notes |
|-----------|-------------|-------|
| `stocks/prices/` | TIME_SERIES_INTRADAY | Datetime (1min), Open, High, Low, Close, Volume |
| `stocks/prices_daily/` | TIME_SERIES_DAILY_ADJUSTED | Date, Open, High, Low, Close, Volume, DividendAmount, SplitCoefficient |
| `stocks/income_statement/` | INCOME_STATEMENT | Daily interval |
| `stocks/balance_sheet/` | BALANCE_SHEET | Daily interval |
| `stocks/cash_flow/` | CASH_FLOW | Daily interval |
| `stocks/earnings/` | EARNINGS | Daily interval |
| `stocks/earnings_estimates/` | EARNINGS_ESTIMATES | Daily interval |
| `stocks/insider/` | INSIDER_TRANSACTIONS | Daily interval |
| `stocks/sentiment/` | NEWS_SENTIMENT | Daily interval |
| `etfs/prices/` | TIME_SERIES_INTRADAY | Datetime (1min), Open, High, Low, Close, Volume |
| `etfs/prices_daily/` | TIME_SERIES_DAILY_ADJUSTED | Date, Open, High, Low, Close, Volume, DividendAmount, SplitCoefficient |
| `etfs/etf_profile/` | ETF_PROFILE | Only given on last day |
| `forex/` | FX_DAILY | Daily interval |
| `indices/` | INDEX_DATA | Daily (SPX, VIX, etc.) |
| `cryptocurrencies/` | DIGITAL_CURRENCY_DAILY | Daily interval |
| `commodities/` | WTI, BRENT, NATURAL_GAS, gold, etc. | Some have only monthly data |
| `economic/` | GDP, CPI, unemployment, fed funds, yields | Some have only monthly data |

**`daily/`** — Daily pulls, organized by date. One folder per trading day (`YYYY-MM-DD/`). Same subfolder structure and file format as `historical/` (compressed `.parquet`). Price endpoints, `insider`, `sentiment`, and `etf_profile` use one file per ticker (e.g., `AAPL.parquet`). Fundamental endpoints (`income_statement`, `balance_sheet`, `cash_flow`, `earnings`, `earnings_estimates`) use two files per ticker: `SYMBOL_annual.parquet` and `SYMBOL_quarterly.parquet`. What each file contains depends on the data type:

- **Time series** (prices, forex, indices, cryptocurrencies, commodities, economic): cut to only that date's data.
- **Fundamentals** (income statement, balance sheet, cash flow, earnings): keep all data up to ~4 years of history.
- **News sentiment, insider transactions**: cut to last retrieved date.

**Key rules:**
- `daily/` is append-only — each day creates a new dated folder. Past days are never modified.
- `historical/` is initially populated from Alpha Vantage. If FirstRate Data is added later, it may modify existing tickers — but only if the overlapping data between Alpha Vantage and FirstRate Data agrees. If the intersecting data conflicts, the ticker is flagged for review rather than silently overwritten.
- The `catalog/` directory is the only mutable area: yield status and ticker metadata are updated as coverage changes.
- Only tickers with positive yield status (known to return data) are pulled daily. Empty/stopped tickers are re-checked weekly.

**Historical price data notes:**
Historical prices are stored in two separate subfolders under `stocks/`: `prices/` holds intraday bars from `TIME_SERIES_INTRADAY` (Open, High, Low, Close, Volume), and `prices_daily/` holds daily bars from `TIME_SERIES_DAILY_ADJUSTED` (Open, High, Low, Close, Volume, DividendAmount, SplitCoefficient). Adjusted close is not calculated or stored in either folder. If supplemented with FirstRate Data, the FirstRate bundle ships three daily variants per symbol (unadjusted, split-adjusted, split+dividend-adjusted) plus unadjusted 1-min bars. `DividendAmount` and `SplitCoefficient` are derived from the three daily variants to match the AV schema. The data source is recorded per ticker so the origin is preserved.

## Estimated costs

| Item | Cost | Notes |
|---|---|---|
| Alpha Vantage (paid plan) | ~$600/yr | Sole required provider — historical setup + ongoing daily data. |
| GCP Cloud Storage | ~$20/yr | Parquet files (historical + daily) |
| GCP Cloud Run | ~$5–20/yr | Daily ingestion container (low usage, mostly free tier) |
| FirstRate Data (optional) | ~$300–400 one-time | Adds 16k+ tickers (7k+ delisted), 26 years of 1-min data. |
| **Total year 1 (AV only)** | **~$625–640** | No FirstRate purchase needed |
| **Total year 1 (with FirstRate)** | **~$925–1,040** | Includes optional one-time FirstRate purchase |
| **Total year 2+** | **~$625–640/yr** | Recurring only |

## Future considerations

- **Add Norgate Data** for historical index constituent data beyond the S&P 500 (Russell 3000, NASDAQ 100, etc.). S&P 500 history is already covered via Wikipedia/FMP, but Norgate provides ready-made constituent history for other indices that would enable reconstitution bias-free backtesting across a wider universe.
- **Add Finnhub** for congressional trading data, insider sentiment, and ESG scores as supplementary alternative data.
- **Add Polygon.io or Databento** if the project evolves toward live trading or HFT requiring real-time streaming or order book depth.
- **EDGAR XBRL ingestion** as a direct SEC filing pipeline to cross-validate Alpha Vantage fundamentals and capture restatements at the source.
- **If institutional access becomes available** (e.g., university affiliation), integrate CRSP/Compustat via WRDS to replace the homegrown PIT layer.
- **Schedule local sync** of the GCS bucket before market open (e.g., cron + `gsutil rsync`) so no manual step is needed.
- **GCS lifecycle rules** to move raw data older than 1 year to Nearline/Coldline storage.

## License

This project is for personal research use. Data from FirstRate Data and Alpha Vantage is subject to their respective terms of service and cannot be redistributed. GCP resources are billed to your own GCP account.

## References

- FirstRate Data: [firstratedata.com](https://firstratedata.com)
- Alpha Vantage: [alphavantage.co](https://www.alphavantage.co)
