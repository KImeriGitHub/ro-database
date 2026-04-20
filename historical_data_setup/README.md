# Historical Data Setup

One-time historical data download from Alpha Vantage. Fetches price, fundamental, FX, index, crypto, commodity, and economic data for all asset types in the catalog, saves as per-symbol `.parquet` files. Resumable -- already-downloaded symbols are skipped on re-run.

## Data folder structure

```
historical/
├── stocks/
│   ├── prices/               # 1-min intraday (TIME_SERIES_INTRADAY)
│   ├── prices_daily/         # daily adjusted (TIME_SERIES_DAILY_ADJUSTED)
│   ├── income_statement/     # SYMBOL_annual.parquet + SYMBOL_quarterly.parquet
│   ├── balance_sheet/        # SYMBOL_annual.parquet + SYMBOL_quarterly.parquet
│   ├── cash_flow/            # SYMBOL_annual.parquet + SYMBOL_quarterly.parquet
│   ├── earnings/             # SYMBOL_annual.parquet + SYMBOL_quarterly.parquet
│   ├── earnings_estimates/   # SYMBOL_annual.parquet + SYMBOL_quarterly.parquet
│   ├── insider/              # SYMBOL.parquet (INSIDER_TRANSACTIONS)
│   └── sentiment/            # NEWS_SENTIMENT (global paginated fetch)
├── etfs/
│   ├── prices/               # 1-min intraday (same as stocks)
│   ├── prices_daily/         # daily adjusted (same as stocks)
│   └── etf_profile/          # ETF_PROFILE (one file per symbol)
├── forex/                    # SYMBOL.parquet (FX_DAILY, ~160 pairs vs USD)
├── indices/                  # SYMBOL.parquet (INDEX_DATA, ~400+ indices)
├── cryptocurrencies/         # SYMBOL.parquet (DIGITAL_CURRENCY_DAILY, ~600 USD pairs)
├── commodities/              # SYMBOL.parquet (13 commodities, mixed daily/monthly)
├── economic/                 # SYMBOL.parquet (15 indicators, mixed intervals)
└── ingestion_report.parquet  # issue tracking table
```

## Usage

```bash
# Create folder structure only
python historical_data_setup/ensure_folders.py

# Full run (all asset types, all implemented endpoints)
python historical_data_setup/setup_historical.py

# Stocks only, daily prices only
python historical_data_setup/setup_historical.py --asset-types stocks --endpoints prices_daily

# ETFs only
python historical_data_setup/setup_historical.py --asset-types etfs

# Forex only
python historical_data_setup/setup_historical.py --asset-types forex

# Cryptocurrencies only
python historical_data_setup/setup_historical.py --asset-types cryptocurrencies

# Commodities only
python historical_data_setup/setup_historical.py --asset-types commodities

# Economic indicators only
python historical_data_setup/setup_historical.py --asset-types economic

# Indices only
python historical_data_setup/setup_historical.py --asset-types indices

# With standard API tier (default is premium)
python historical_data_setup/setup_historical.py --api-tier standard

# Custom paths
python historical_data_setup/setup_historical.py --catalog-dir /path/to/catalog --historical-dir /path/to/historical
```

## Recovery and resume

The setup is resumable by design -- if the process crashes, is killed, or hits an unhandled exception mid-run, just rerun the same command. No ledger, task IDs, or manual cleanup are needed.

### How it works

- **Per-symbol file-level resume.** Each endpoint's fetch function checks whether the destination parquet already exists for a symbol and skips it if so. On restart, only symbols that were never written (or whose writes didn't complete) are re-fetched. This applies uniformly across `prices`, `prices_daily`, fundamentals, `insider`, `etf_profile`, `forex`, `indices`, `cryptocurrencies`, `commodities`, and `economic`.
- **Stable data_complete_date across resumes.** The start time is captured once in the mtime of `historical/.setup_started_at`, created on the first run and preserved through every resume. All rows finalized in `yield_status` therefore share a single `data_complete_date` anchored to the original start, not the last restart.
- **Top-level exception isolation.** Each `(asset_type, endpoint)` task is wrapped so a failure in one endpoint (e.g. a sentiment crash) does not tear down the `asyncio.gather`; other tasks keep running and the failed one is retried on the next run.
- **Append-only writes.** Parquet files are written atomically per symbol, so a crash mid-write leaves either the complete previous file or no file; it never leaves a corrupted partial file that would be treated as "already done".

### Operational notes

- A full rerun of the same command is always the right recovery action.
- `ingestion_report.parquet` is overwritten each run (issues seen during this run, not a cumulative log). To audit what's still missing after a partial run, inspect the parquet tree directly or rerun with the same flags -- only gaps will be touched.
- `historical/.setup_started_at` is only deleted on a successful full-run finalize. Its presence means a setup is in progress or never cleanly finished; leave it alone.
- To force a clean restart (new start date, re-fetch everything), delete the per-symbol parquet trees under `historical/` AND `historical/.setup_started_at`.
- Partial runs with `--asset-types` / `--endpoints` intentionally skip the finalize step, so they never delete the start marker.

## FirstRate Data integration

When `--stocks-dir` and/or `--etfs-dir` are provided, the pipeline loads `prices/` and `prices_daily/` data from FirstRate Data CSVs instead of Alpha Vantage. Symbols not covered by FirstRate Data fall back to AV automatically. The two endpoints are independent -- a symbol can use FRD for one and AV for the other.

### Expected directory structure

Each FRD directory (stocks or ETFs) is a **flat folder** containing per-symbol CSV files. No subdirectories.

### File naming

For each symbol, up to four CSV files may be present:

| File | Used by | Description |
|------|---------|-------------|
| `{SYMBOL}_1min.csv` | `prices/` | 1-minute intraday bars (unadjusted) |
| `{SYMBOL}_1day_unadjusted.csv` | `prices_daily/` | Daily bars, no adjustments |
| `{SYMBOL}_1day_splitadjusted.csv` | `prices_daily/` | Daily bars, split-adjusted |
| `{SYMBOL}_1day_splitdivadjusted.csv` | `prices_daily/` | Daily bars, split- and dividend-adjusted |

For `prices/`, only `{SYMBOL}_1min.csv` is required. For `prices_daily/`, all three daily files (`_1day_unadjusted`, `_1day_splitadjusted`, `_1day_splitdivadjusted`) must be present; if any is missing the symbol falls back to AV.

### CSV header

All files must have the header:

```
timestamp,open,high,low,close,volume
```

### Timestamp formats

- **1-min files:** `YYYY-MM-DD HH:MM:SS` (e.g. `2026-03-16 04:03:00`)
- **Daily files:** `YYYY-MM-DD` (e.g. `2026-03-16`)

### SplitCoefficient and DividendAmount derivation

`SplitCoefficient` and `DividendAmount` are not present in FRD CSVs and are derived from the three daily files:

- **SplitCoefficient**: computed as the ratio of consecutive cumulative split factors (`unadjusted_close / splitadjusted_close`). Equals 1.0 on non-split days and the split ratio (e.g. 4.0 for a 4:1 split) on split days.
- **DividendAmount**: derived from the change in the cumulative dividend factor (`splitdivadjusted_close / splitadjusted_close`) between consecutive days, then un-adjusted by the cumulative split factor to give the actual cash dividend per share (matching AV convention). Equals 0.0 on non-ex-dividend days.

### Usage with FirstRate Data

```bash
# Stocks + ETFs with FRD
python historical_data_setup/setup_historical.py --stocks-dir /path/to/frd/stocks --etfs-dir /path/to/frd/etfs

# Stocks only, prices only, with FRD
python historical_data_setup/setup_historical.py --asset-types stocks --endpoints prices --stocks-dir /path/to/frd/stocks

# FRD for stocks, AV-only for ETFs
python historical_data_setup/setup_historical.py --stocks-dir /path/to/frd/stocks
```

## Stocks & ETFs

### Stocks

Stocks use the `prices`, `prices_daily`, `income_statement`, `balance_sheet`, `cash_flow`, `earnings`, `earnings_estimates`, `insider`, and `sentiment` endpoints. The pipeline reads from `catalog/stocks.parquet` and writes to `historical/stocks/`.

### ETFs

ETFs use the `prices`, `prices_daily`, and `etf_profile` endpoints with `--asset-types etfs`. The pipeline reads from `catalog/etfs.parquet` and writes to `historical/etfs/prices/`, `historical/etfs/prices_daily/`, and `historical/etfs/etf_profile/`.

### prices (TIME_SERIES_INTRADAY)

Per symbol, fetches 1-min bars for every month from `max(ipoDate, 2000-01)` to `min(delistingDate, today)`.

**Output schema** (`historical/stocks/prices/SYMBOL.parquet`):

| Column | Type |
|--------|------|
| Date   | pl.Datetime |
| Open   | pl.Float32 |
| High   | pl.Float32 |
| Low    | pl.Float32 |
| Close  | pl.Float32 |
| Volume | pl.Float32 |

Avg round-trip per call is ~3.5s (large JSON payloads), making this the slowest per-call endpoint. For historical intraday prices, FirstRate Data is used instead, so this endpoint is primarily needed for daily updates of the most recent month.

**Ingestion report issues:**
- `structure_error` -- response missing `"Meta Data"` or `"Time Series (1min)"` key, or fetch failure
- `empty_content` -- empty time series, or empty individual bar
- `cast_failure` -- OHLCV `float()` conversion failure
- `timezone_mismatch` -- timezone is not `"US/Eastern"`
- `av_throttle` -- persistent rate-limit after retries

### prices_daily (TIME_SERIES_DAILY_ADJUSTED)

Per symbol, fetches the full daily price history in a single API call.

**Output schema** (`historical/stocks/prices_daily/SYMBOL.parquet`):

| Column | Type |
|--------|------|
| Date             | pl.Date |
| Open             | pl.Float32 |
| High             | pl.Float32 |
| Low              | pl.Float32 |
| Close            | pl.Float32 |
| Volume           | pl.Float32 |
| DividendAmount   | pl.Float32 |
| SplitCoefficient | pl.Float32 |

Adjusted Close is intentionally omitted (calculated outside of raw data fetching).

**Ingestion report issues:**
- `structure_error` -- response missing `"Meta Data"` or `"Time Series (Daily)"` key, or fetch failure
- `empty_content` -- empty time series, or empty individual bar
- `cast_failure` -- OHLCV/dividend/split `float()` conversion or date parse failure
- `timezone_mismatch` -- timezone is not `"US/Eastern"`
- `av_throttle` -- persistent rate-limit after retries

### income_statement (INCOME_STATEMENT)

Per symbol, fetches the full income statement history in a single API call. The response contains both annual and quarterly reports, saved as separate files.

**Output schema** (`historical/stocks/income_statement/SYMBOL_annual.parquet` and `SYMBOL_quarterly.parquet`):

| Column | Type | Notes |
|--------|------|-------|
| fiscalDateEnding | pl.Date | Sort key |
| reportedCurrency | pl.String | e.g. "USD" |
| (other fields) | pl.Float32 | Non-castable values forced to null with `cast_failure` recorded |

Fields vary across symbols and between annual/quarterly. Null sentinels (`None`, `"None"`, `""`, `"."`) from the API are converted to null. All non-date, non-string columns are cast to Float32; values that cannot be cast are forced to null (never kept as String) and a `cast_failure` is recorded.

**Ingestion report issues:**
- `structure_error` -- fetch failure, missing top-level keys (`symbol`, `annualReports`, `quarterlyReports`), or missing `fiscalDateEnding` column
- `empty_content` -- empty annual or quarterly reports list
- `cast_failure` -- `fiscalDateEnding` or `reportedDate` date parse failure, or non-castable Float32 values (forced to null)
- `av_throttle` -- persistent rate-limit after retries

### balance_sheet (BALANCE_SHEET)

Same structure as income_statement. Per symbol, fetches balance sheet data.

**Output schema** (`historical/stocks/balance_sheet/SYMBOL_annual.parquet` and `SYMBOL_quarterly.parquet`):

| Column | Type | Notes |
|--------|------|-------|
| fiscalDateEnding | pl.Date | Sort key |
| reportedCurrency | pl.String | e.g. "USD" |
| (other fields) | pl.Float32 | Non-castable values forced to null with `cast_failure` recorded |

**Ingestion report issues:**
- `structure_error` -- fetch failure, missing top-level keys (`symbol`, `annualReports`, `quarterlyReports`), or missing `fiscalDateEnding` column
- `empty_content` -- empty annual or quarterly reports list
- `cast_failure` -- `fiscalDateEnding` or `reportedDate` date parse failure, or non-castable Float32 values (forced to null)
- `av_throttle` -- persistent rate-limit after retries

### cash_flow (CASH_FLOW)

Same structure as income_statement. Per symbol, fetches cash flow data.

**Output schema** (`historical/stocks/cash_flow/SYMBOL_annual.parquet` and `SYMBOL_quarterly.parquet`):

| Column | Type | Notes |
|--------|------|-------|
| fiscalDateEnding | pl.Date | Sort key |
| reportedCurrency | pl.String | e.g. "USD" |
| (other fields) | pl.Float32 | Non-castable values forced to null with `cast_failure` recorded |

**Ingestion report issues:**
- `structure_error` -- fetch failure, missing top-level keys (`symbol`, `annualReports`, `quarterlyReports`), or missing `fiscalDateEnding` column
- `empty_content` -- empty annual or quarterly reports list
- `cast_failure` -- `fiscalDateEnding` or `reportedDate` date parse failure, or non-castable Float32 values (forced to null)
- `av_throttle` -- persistent rate-limit after retries

### earnings (EARNINGS)

Per symbol, fetches earnings data. Unlike the other fundamental endpoints, the top-level keys are `annualEarnings` and `quarterlyEarnings`.

**Output schema** (`historical/stocks/earnings/SYMBOL_annual.parquet`):

| Column | Type | Notes |
|--------|------|-------|
| fiscalDateEnding | pl.Date | Sort key |
| reportedEPS | pl.Float32 | |

**Output schema** (`historical/stocks/earnings/SYMBOL_quarterly.parquet`):

| Column | Type | Notes |
|--------|------|-------|
| fiscalDateEnding | pl.Date | Sort key |
| reportedDate | pl.Date | Date the earnings were publicly reported |
| reportedEPS | pl.Float32 | |
| estimatedEPS | pl.Float32 | |
| surprise | pl.Float32 | |
| surprisePercentage | pl.Float32 | |
| reportTime | pl.String | "pre-market" or "post-market" |

**Note:** `reportedDate` and `reportTime` are provided by this endpoint and are critical for point-in-time (PIT) reconstruction -- they indicate exactly when earnings became public knowledge.

**Ingestion report issues:**
- `structure_error` -- fetch failure, missing top-level keys (`symbol`, `annualEarnings`, `quarterlyEarnings`), or missing `fiscalDateEnding` column
- `empty_content` -- empty annual or quarterly earnings list
- `cast_failure` -- `fiscalDateEnding` or `reportedDate` date parse failure, or non-castable Float32 values (forced to null)
- `av_throttle` -- persistent rate-limit after retries

### earnings_estimates (EARNINGS_ESTIMATES)

Per symbol, fetches analyst earnings estimates. The API response returns a flat `"estimates"` list; records are split into annual and quarterly based on the `"horizon"` field (`"fiscal year"` vs `"fiscal quarter"`). The `"date"` field is renamed to `fiscalDateEnding` for consistency with other fundamental endpoints.

**Output schema** (`historical/stocks/earnings_estimates/SYMBOL_annual.parquet` and `SYMBOL_quarterly.parquet`):

| Column | Type | Notes |
|--------|------|-------|
| fiscalDateEnding | pl.Date | Sort key (from `date` in API response) |
| (other fields) | pl.Float32 | Non-castable values forced to null with `cast_failure` recorded |

Typical fields include `eps_estimate_average`, `eps_estimate_high`, `eps_estimate_low`, `number_of_analysts`, revision counts, etc. Fields vary across symbols. The `"horizon"` field is consumed during the split and not stored.

**Ingestion report issues:**
- `structure_error` -- response missing `"symbol"` or `"estimates"` top-level keys
- `empty_content` -- empty estimates list, or no records for a given horizon after split
- `cast_failure` -- `fiscalDateEnding` parse failure, or a numeric-looking column that could not be cast to Float32

### insider (INSIDER_TRANSACTIONS)

Per symbol, fetches insider transaction history for **active stocks only**. The API response contains a flat `"data"` list. The `"transaction_date"` field is renamed to `transactionDate`; the `"ticker"` field is dropped (redundant with the file name). One file per symbol.

**Output schema** (`historical/stocks/insider/SYMBOL.parquet`):

| Column | Type | Notes |
|--------|------|-------|
| transactionDate | pl.Date | Sort key (from `transaction_date` in API response) |
| executive | pl.String | e.g. "COOK, TIMOTHY D" |
| executive_title | pl.String | e.g. "Director, Chief Executive Officer" |
| security_type | pl.String | e.g. "Common Stock" |
| acquisition_or_disposal | pl.String | "A" or "D" |
| shares | pl.Float32 | |
| share_price | pl.Float32 | |

Fields vary across symbols. Known string columns (`executive`, `executive_title`, `security_type`, `acquisition_or_disposal`) stay as String; all other columns are cast to Float32. Non-castable values in Float32 columns are forced to null (never kept as String) and a `cast_failure` is recorded.

**Ingestion report issues:**
- `structure_error` -- response missing or invalid `"data"` key
- `empty_content` -- empty data list
- `cast_failure` -- `transactionDate` parse failure, or non-castable values in a Float32 column (forced to null)

### sentiment (NEWS_SENTIMENT)

Global (not per-ticker) query paginating backward from the current UTC time to 2010-01-01, 1000 articles per call. Each response's oldest `time_published` is used to compute the next `time_to` (ceiling to next minute to avoid gaps). After all pages are fetched, rows are filtered to tickers present in the catalog, deduplicated on `(url, ticker)`, and saved as a single `ALL_MESSAGES.parquet`. Per-symbol files are then split from this master table for every active symbol.

**Output schema** (`historical/stocks/sentiment/ALL_MESSAGES.parquet` and `{SYMBOL}.parquet`):

| Column | Type | Notes |
|--------|------|-------|
| time_published | pl.Datetime | **UTC** -- parsed from `"20260410T153926"` format |
| ticker | pl.String | From `ticker_sentiment` array |
| ticker_relevance_score | pl.Float32 | |
| ticker_sentiment_score | pl.Float32 | |
| ticker_sentiment_label | pl.String | |
| title | pl.String | |
| url | pl.String | |
| authors | pl.String | Multiple authors joined with `";"`, empty list -> `""` |
| summary | pl.String | |
| banner_image | pl.String | |
| source | pl.String | |
| category_within_source | pl.String | |
| source_domain | pl.String | |
| overall_sentiment_score | pl.Float32 | |
| overall_sentiment_label | pl.String | |
| blockchain | pl.Float32 | Topic relevance score (null if topic absent) |
| earnings | pl.Float32 | " |
| ipo | pl.Float32 | " |
| mergers_and_acquisitions | pl.Float32 | " |
| financial_markets | pl.Float32 | " |
| economy_fiscal | pl.Float32 | " |
| economy_monetary | pl.Float32 | " |
| economy_macro | pl.Float32 | " |
| energy_transportation | pl.Float32 | " |
| finance | pl.Float32 | " |
| life_sciences | pl.Float32 | " |
| manufacturing | pl.Float32 | " |
| real_estate | pl.Float32 | " |
| retail_wholesale | pl.Float32 | " |
| technology | pl.Float32 | " |

Each row corresponds to one ticker mentioned in one article. If an article mentions 3 tickers, it produces 3 rows sharing the same article-level fields.

`ALL_MESSAGES.parquet` contains only rows whose `ticker` matches a symbol in the catalog. Per-symbol `{SYMBOL}.parquet` files are created only for active symbols.

**Ingestion report issues:**
- `structure_error` -- response missing `"feed"` key, or fetch failure
- `empty_content` -- no sentiment data fetched at all
- `cast_failure` -- `time_published` parse failure
- `av_throttle` -- persistent rate-limit after retries

**Resource estimate (based on 500-call sample, 2026-04-10):**

| Metric | Observed (500 calls) | Extrapolated (full 2010--now) |
|--------|---------------------|-------------------------------|
| API calls | 500 | ~40,000 |
| Articles | 500,000 | ~40M |
| Ticker rows | 687,590 | ~55M |
| Time span covered | 1.2% (~73 days) | 100% (~16 years) |
| Wall-clock time | 21 min | ~29 hours |
| Avg time per call | 2.56s | -- |
| Avg history per call | 213 min (0.1 days) | -- |
| Est. DataFrame RAM | -- | ~53 GB |

These are upper-bound estimates -- older years have fewer articles per day, so calls cover more time and produce fewer rows. Actual totals will likely be lower.

Sentiment is the heaviest endpoint in the historical setup -- both in RAM (~53 GB in-memory DataFrame) and in per-call wall-clock time (~2.5s/call due to multi-MB JSON payloads with nested text). Other endpoints return compact numeric data and are budget-bound rather than request-bound. Run sentiment on a machine with at least 128 GB RAM. Because sentiment alone consumes only ~24 of the 74 calls/min budget, co-schedule it with fast endpoints in the same `setup_historical.py` run (e.g. `--endpoints sentiment prices_daily income_statement`); the shared sliding-window rate limiter keeps the combined rate within budget.

### etf_profile (ETF_PROFILE)

Per symbol, fetches the ETF profile in a single API call. Only runs when `asset_type="etfs"` (skipped for stocks). The response is a flat object with scalar metadata, a `sectors` list, and a `holdings` list. Sectors are pivoted into fixed snake_case columns; unknown sectors accumulate into `other`. Holdings are stored as a list of `{symbol, weight}` structs; entries with `symbol == "n/a"` are discarded.

**Output schema** (`historical/etfs/etf_profile/SYMBOL.parquet`):

| Column | Type | Notes |
|--------|------|-------|
| date | pl.Date | Date the profile was fetched |
| information_technology | pl.Float32 | Sector weight |
| communication_services | pl.Float32 | |
| consumer_discretionary | pl.Float32 | |
| consumer_staples | pl.Float32 | |
| healthcare | pl.Float32 | |
| industrials | pl.Float32 | |
| utilities | pl.Float32 | |
| materials | pl.Float32 | |
| energy | pl.Float32 | |
| financials | pl.Float32 | |
| real_estate | pl.Float32 | |
| other | pl.Float32 | Sum of unknown sector weights |
| holdings | pl.List(pl.Struct) | `{symbol: Utf8, weight: Float32}` -- `n/a` symbols discarded |
| net_assets | pl.Float32 | |
| net_expense_ratio | pl.Float32 | |
| portfolio_turnover | pl.Float32 | Often null (API returns `"n/a"`) |
| dividend_yield | pl.Float32 | |
| inception_date | pl.String | e.g. `"1999-03-10"` |
| leveraged | pl.String | `"YES"` or `"NO"` |

**Null handling:** `None`, `"None"`, `"n/a"`, `""`, and `"."` are all treated as null. Null values in Float32 columns remain null without triggering a cast failure. Scalar Float32 fields that fail to cast are set to null (not kept as String) and a `cast_failure` is recorded.

**Ingestion report issues:**
- `structure_error` -- response missing required keys, or `sectors`/`holdings` not a list
- `empty_content` -- empty sectors list
- `cast_failure` -- sector weight, holding weight, or scalar field could not be cast to Float32
- `av_throttle` -- persistent rate-limit after retries

### Fundamental endpoints file naming convention

Fundamental endpoints (`income_statement`, `balance_sheet`, `cash_flow`, `earnings`, `earnings_estimates`) store **two files per symbol** -- one for annual data and one for quarterly data:

- **Historical:** `SYMBOL_annual.parquet` and `SYMBOL_quarterly.parquet`
- **Daily:** `SYMBOL_annual.parquet` and `SYMBOL_quarterly.parquet`

The Alpha Vantage API returns both annual and quarterly data in a single response (e.g., `annualReports` + `quarterlyReports` for financial statements, `annualEarnings` + `quarterlyEarnings` for earnings). Each is split and saved as a separate file. For `EARNINGS_ESTIMATES`, the split is based on the `horizon` field in the response.

This does not apply to price endpoints (`prices`, `prices_daily`), `insider`, or `etf_profile`, which use a single file per symbol (`SYMBOL.parquet`). The `sentiment` endpoint uses `ALL_MESSAGES.parquet` (master table) plus per-symbol `{SYMBOL}.parquet` files.

## Forex

### forex (FX_DAILY)

Per currency pair, fetches the full daily FX history in a single API call. The forex catalog contains ~160 currencies all paired against USD (e.g. `EURUSD`, `GBPUSD`). `USDUSD` is skipped. All symbols are processed regardless of `status` (no Delisted/Corrupted filtering). The API returns UTC timestamps; the pipeline validates that the timezone is `"UTC"` and records a `timezone_mismatch` if not.

**Output schema** (`historical/forex/SYMBOL.parquet`):

| Column | Type |
|--------|------|
| Date   | pl.Date |
| Open   | pl.Float32 |
| High   | pl.Float32 |
| Low    | pl.Float32 |
| Close  | pl.Float32 |

No volume data is available for FX pairs. FX_INTRADAY is premium-only and not tracked.

**Ingestion report issues:**
- `structure_error` -- response missing `"Meta Data"` or `"Time Series FX (Daily)"` key, or fetch failure
- `empty_content` -- empty time series, or empty individual bar
- `cast_failure` -- OHLC `float()` conversion or date parse failure
- `timezone_mismatch` -- timezone is not `"UTC"`
- `av_throttle` -- persistent rate-limit after retries

## Cryptocurrencies

### cryptocurrencies (DIGITAL_CURRENCY_DAILY)

Per symbol, fetches the full daily crypto price history in a single API call. The cryptocurrencies catalog contains ~600 USD-paired symbols (filtered to `market=USD` at catalog creation time). All symbols are attempted regardless of `status` -- many will be dead/empty, and the ingestion report captures which ones failed. Volume is in the cryptocurrency's own unit (e.g. BTC), not USD. The `outputsize` parameter is not supported by this endpoint; the API returns all available history by default.

**Output schema** (`historical/cryptocurrencies/SYMBOL.parquet`):

| Column | Type |
|--------|------|
| Date   | pl.Date |
| Open   | pl.Float32 |
| High   | pl.Float32 |
| Low    | pl.Float32 |
| Close  | pl.Float32 |
| Volume | pl.Float32 |

Only OHLCV columns are stored. Additional fields that may appear in some responses (e.g. `market cap (USD)` in older data) are ignored.

**Ingestion report issues:**
- `structure_error` -- response missing `"Meta Data"` or `"Time Series (Digital Currency Daily)"` key, or fetch failure
- `empty_content` -- empty time series, or empty individual bar
- `cast_failure` -- OHLCV `float()` conversion or date parse failure
- `timezone_mismatch` -- timezone is not `"UTC"`
- `av_throttle` -- persistent rate-limit after retries

## Commodities

### commodities

Fetches historical commodity data for all 13 symbols in the commodities catalog. Three groups of symbols use different API endpoints and intervals:

**Group 1 -- Daily standard** (WTI, BRENT, NATURAL_GAS):
```
?function=SYMBOL&interval=daily&apikey=API_KEY
```

**Group 2 -- Monthly standard** (COPPER, ALUMINUM, WHEAT, CORN, COTTON, SUGAR, COFFEE, ALL_COMMODITIES):
```
?function=SYMBOL&interval=monthly&apikey=API_KEY
```
Daily interval is not available for these symbols; monthly is the finest granularity.

**Group 3 -- Gold and Silver** (XAU, XAG):
```
?function=GOLD_SILVER_HISTORY&symbol=GOLD&interval=daily&apikey=API_KEY
?function=GOLD_SILVER_HISTORY&symbol=SILVER&interval=daily&apikey=API_KEY
```
The catalog maps XAU to GOLD and XAG to SILVER. The response uses `price` instead of `value`; this is renamed to `value` for consistency.

**Output schema** (`historical/commodities/SYMBOL.parquet`):

| Column | Type | Notes |
|--------|------|-------|
| Date   | pl.Date | Sort key |
| value  | pl.Float32 | Null sentinels (`None`, `"None"`, `""`, `"."`) treated as null |
| unit   | pl.String | From AV response (e.g. `"dollars per barrel"`); hardcoded to `"dollars per troy ounce"` for XAU/XAG |

One file per symbol. All 13 symbols share the same schema.

**Ingestion report issues:**
- `structure_error` -- response missing `"data"` key, or fetch failure
- `empty_content` -- empty data list
- `cast_failure` -- `value`/`price` could not be cast to Float32, or date parse failure
- `av_throttle` -- persistent rate-limit after retries

## Economic indicators

### economic

Fetches historical data for all 15 economic indicators in the catalog. Each indicator maps to a different AV function (and sometimes extra parameters like `interval` or `maturity`). Only ~15 API calls total.

**Indicator config:**

| Catalog symbol | AV function | Extra params |
|---------------|-------------|--------------|
| REAL_GDP | REAL_GDP | interval=quarterly |
| REAL_GDP_PER_CAPITA | REAL_GDP_PER_CAPITA | (none) |
| TREASURY_YIELD_30Y | TREASURY_YIELD | interval=daily, maturity=30year |
| TREASURY_YIELD_10Y | TREASURY_YIELD | interval=daily, maturity=10year |
| TREASURY_YIELD_7Y | TREASURY_YIELD | interval=daily, maturity=7year |
| TREASURY_YIELD_5Y | TREASURY_YIELD | interval=daily, maturity=5year |
| TREASURY_YIELD_2Y | TREASURY_YIELD | interval=daily, maturity=2year |
| TREASURY_YIELD_3M | TREASURY_YIELD | interval=daily, maturity=3month |
| FEDERAL_FUNDS_RATE | FEDERAL_FUNDS_RATE | interval=daily |
| CPI | CPI | interval=monthly |
| INFLATION | INFLATION | (none, annual only) |
| RETAIL_SALES | RETAIL_SALES | (none, monthly only) |
| DURABLES | DURABLES | (none, monthly only) |
| UNEMPLOYMENT | UNEMPLOYMENT | (none, monthly only) |
| NONFARM_PAYROLL | NONFARM_PAYROLL | (none, monthly only) |

All indicators share the same AV response structure (`name`, `interval`, `unit`, `data`). The `"."` value in the data list is treated as null.

**Output schema** (`historical/economic/SYMBOL.parquet`):

| Column | Type | Notes |
|--------|------|-------|
| Date   | pl.Date | Sort key |
| value  | pl.Float32 | Null sentinels (`None`, `"None"`, `""`, `"."`) treated as null |

One file per indicator. Unit metadata is not stored.

**Ingestion report issues:**
- `structure_error` -- response missing expected top-level keys (`name`, `interval`, `unit`, `data`), unknown indicator symbol, or fetch failure
- `empty_content` -- empty data list
- `cast_failure` -- `value` could not be cast to Float32, or date parse failure
- `av_throttle` -- persistent rate-limit after retries

## Indices

### indices (INDEX_DATA)

Per symbol, fetches the full daily index price history in a single API call. The indices catalog contains ~400+ symbols (e.g. SPX, DJI, IXIC). Many symbols may not have data; the ingestion report captures which ones failed. The `INDEX_DATA` endpoint does not support `outputsize` -- the API returns all available history by default. The response has no `"Meta Data"` key; the top-level structure is `{symbol, name, interval, data}` where `data` is a flat list of OHLC records.

Note: `INDEX_DATA` was introduced in April 2026 and may not work with older premium API keys.

**Output schema** (`historical/indices/SYMBOL.parquet`):

| Column | Type |
|--------|------|
| Date   | pl.Date |
| Open   | pl.Float32 |
| High   | pl.Float32 |
| Low    | pl.Float32 |
| Close  | pl.Float32 |

No volume data is available for indices. One file per symbol. Null sentinels (`None`, `"None"`, `""`, `"."`) are treated as null.

**Ingestion report issues:**
- `structure_error` -- response missing `"data"` key, or fetch failure
- `empty_content` -- empty data list
- `cast_failure` -- OHLC `float()` conversion or date parse failure
- `av_throttle` -- persistent rate-limit after retries

## Ingestion report

Issues encountered during fetching are tracked in `historical/ingestion_report.parquet`:

| Column | Type | Description |
|--------|------|-------------|
| symbol | Utf8 | Ticker symbol |
| asset_type | Utf8 | stocks, etfs, forex, indices, cryptocurrencies, commodities, or economic |
| endpoint | Utf8 | prices, prices_daily, etc. |
| issue_type | Utf8 | structure_error, empty_content, cast_failure, timezone_mismatch, av_throttle |
| detail | Utf8 | Specifics (e.g., month, error message) |
| timestamp | Datetime | When the issue was recorded |

## Finalizing yield_status

At the end of a **full run** (no `--asset-types` and no `--endpoints` passed), `catalog/yield_status.parquet` is completely overwritten. Per (symbol, applicable endpoint column):

| Ingestion-report issue for (symbol, endpoint) | Resulting cell |
|---|---|
| none | True |
| `structure_error` | False |
| `av_throttle` | False |
| `empty_content` (non-fundamental endpoint) | False |
| `empty_content` (fundamental endpoint) | True if `{symbol}_annual.parquet` or `{symbol}_quarterly.parquet` exists, else False |
| `cast_failure` | True (most rows still saved; only malformed entries forced to null) |
| `timezone_mismatch` | True (data still saved) |

Cells for non-applicable (symbol, column) pairs remain null. Ingestion-report endpoints `forex`, `indices`, `cryptocurrencies`, `commodities`, `economic` map to the single `direct` yield column.

The `--asset-types` and `--endpoints` flags are reserved for non-daily activities (targeted backfills, reruns of a single endpoint); using them intentionally skips finalize to avoid flipping columns for asset types that were not part of the run.

### data_complete_date

All rows share the same `date`, chosen as the last fully-traded ET date at the start of the run:

- Weekend -> start date.
- Weekday, time >= 20:00 ET -> start date.
- Weekday, time <  20:00 ET -> start date minus one day.

The start time is recovered via the mtime of `historical/.setup_started_at`. The marker is created on the first run and preserved across resumes, so a crashed-and-restarted setup keeps its original start timestamp. It is deleted after a successful finalize.

## Rate limiting and cross-endpoint execution

### Sliding-window rate limiter

A single `RateLimiter` is shared across every endpoint task in a run. It tracks timestamps of the last N calls in a trailing 60-second window (default `N = 74`, for a 1-call margin against AV's 75/min cap). `wait()` is async: it returns immediately if the window has room, otherwise it sleeps until the oldest timestamp falls out. An `asyncio.Lock` inside `wait()` serializes concurrent registrations so the budget is enforced globally, even when many coroutines hit it simultaneously.

AV throttle responses (`"Note"` / `"Information"` keys) trigger a 60s retry, up to 3 attempts. Persistent throttle failures are recorded as `av_throttle` issues in the ingestion report.

### Cross-endpoint concurrency

`setup_historical.py` builds one asyncio task per applicable `(asset_type, endpoint)` pair and runs them concurrently with `asyncio.gather` against a single `aiohttp.ClientSession`. Each endpoint function is `async` and processes its symbols sequentially **within the task**, but multiple tasks make HTTP calls in parallel under the shared rate limiter.

This matters for slow endpoints. Intraday `prices` (~1.6 s/call) and `sentiment` (~2.5 s/call) cannot saturate the 75/min budget on their own. Running them alongside fast endpoints (`prices_daily`, `insider`, fundamentals, `forex`, `commodities`) keeps the API budget full without breaching the limit.

Good pairings for a single run:
- `--endpoints sentiment prices_daily income_statement balance_sheet cash_flow` (sentiment is slow + writes `ALL_MESSAGES.parquet` first; fast fundamentals fill idle budget)
- `--endpoints prices forex` (both slow-ish; each drives partial budget, together approach the limit)

The concurrency design is asyncio-only: no threads, no multiprocessing. All coroutines share a single event loop on a single CPU.

## Round-trip times per endpoint (measured 2026-04-11)

Per-endpoint round-trip averages from catalog-sample speedtests, with full-catalog extrapolations at the 74.9 calls/min rate limit.

| Endpoint | Avg round-trip | Sample size | Full-catalog calls | Est. total time | Notes |
|----------|---------------|-------------|-------------------|-----------------|-------|
| commodities | 0.65s | 13 | 13 | ~10s | 13 symbols total; negligible in budget |
| economic | 0.75s | 15 | 15 | ~12s | One call per indicator |
| forex | 1.70s | 40 | 156 | ~4 min | Larger payloads (~3.2k rows/call) |
| cryptocurrencies | 0.81s | 100 | 352 | ~5 min | USD pairs; ~915 rows/call |
| etf_profile | 0.40s | 10 | 6,527 | ~1.4 h | One call per ETF |
| insider | 0.70s | 10 | 8,649 | ~1.9 h | Active stocks only; ~1,150 rows/call |
| prices_daily | 0.70s | 30 | 22,623 | ~5.5 h | Stocks + ETFs combined; ~1,740 rows/call |
| Fundamentals (5 endpoints) | 0.5s | 30 | 80,480 | ~12.4 h | 16,096 stocks x 5 endpoints |
| sentiment | ~2.5s | 500 | ~40,000 | ~29 h | Global paginated fetch to 2010; multi-MB payloads |
| prices (intraday) | ~1.6s | 20 | 2,223,345 | ~600 h | One call per symbol-month. Historical intraday uses FRD instead |

**Commodities per-group breakdown (13-call sample):**

| Group | Avg round-trip | Calls |
|-------|---------------|-------|
| daily (WTI, BRENT, NATURAL_GAS) | 0.70 | 3 |
| gold_silver (XAU, XAG) | 0.70 | 2 |
| monthly (COPPER, ALUMINUM, ...) | 0.40 | 8 |

**Fundamentals per-endpoint breakdown (30-call sample, 6 calls each):**

| Endpoint | Avg round-trip |
|----------|---------------|
| INCOME_STATEMENT | 0.40s |
| BALANCE_SHEET | 0.59s |
| CASH_FLOW | 0.38s |
| EARNINGS | 0.34s |
| EARNINGS_ESTIMATES | 0.52s |

Fundamentals and similar lightweight endpoints are rate-limit-bound: the ~0.8s/call budget ceiling dominates their own round-trip. Intraday prices and sentiment return large payloads where the HTTP request itself takes longer than one budget slot, so run alone they leave most of the 74/min budget unused. With cross-endpoint execution (see "Rate limiting and cross-endpoint execution" above), a slow endpoint can run alongside fast ones in the same run, sharing one rate limiter and one `aiohttp` session, so the combined call rate approaches 74/min without any single endpoint exceeding it. The full historical setup (all endpoints, no FRD) is dominated by intraday prices (~556 h); substituting FRD for `prices` drops the critical path to fundamentals (~18.4 h) plus sentiment (~29 h), which can now overlap in a single concurrent run.

## Null handling

Alpha Vantage encodes missing values in several ways: the JSON literal `null` (Python `None`), the string `"None"`, the empty string `""`, and the string `"."`. All four are treated as **null sentinels** and converted to Polars null during ingestion.

- **Fundamental endpoints** (income_statement, balance_sheet, cash_flow, earnings, earnings_estimates): `_build_fundamental_df` in `_common.py` replaces all null sentinels (`None`, `"None"`, `""`, `"."`) with Python `None` before constructing the DataFrame with `infer_schema_length=0` (all columns start as Utf8). Polars null values in a Utf8 column cast to Float32 as null -- no error raised. Columns that fail to cast to Float32 after sentinel cleanup are force-cast with `strict=False` (remaining non-castable values become null) and a `cast_failure` is recorded. Columns are **never** left as String when Float32 is expected.
- **Insider endpoint**: Same null sentinel replacement before DataFrame construction with `infer_schema_length=0`. Known string columns (`executive`, `executive_title`, `security_type`, `acquisition_or_disposal`) are kept as String; all others must be Float32. Non-castable values in Float32 columns are force-cast to null with a `cast_failure` recorded.
- **ETF profile endpoint**: Null sentinels include `"n/a"` in addition to the standard four. Scalar Float32 fields that fail to cast are set to null (not kept as String) with a `cast_failure` recorded.
- **Price endpoints** (prices, prices_daily): Values are converted via `float()` before DataFrame construction. Any null sentinel or non-numeric value raises `ValueError`/`TypeError`; both are caught, logged as `cast_failure`, and the individual bar is skipped without crashing the run.
- **Commodities and economic endpoints**: All null sentinels (`None`, `"None"`, `""`, `"."`) produce null values in the output. Non-castable values are logged as `cast_failure` and the row is skipped.
- **Sentiment endpoint**: Null sentinels in float and string fields are converted to null via `_safe_float` and `_safe_str` helpers.


## Module structure

```
historical_data_setup/
├── __init__.py
├── ensure_folders.py          # directory tree creation
├── _common.py                 # sliding-window RateLimiter, async fetch_av_json (aiohttp), generate_months, IssueTracker, read_catalog_symbols
├── setup_historical.py        # async CLI orchestrator (asyncio.gather across endpoints)
└── endpoints/
    ├── __init__.py
    ├── prices.py              
    ├── prices_daily.py        
    ├── income_statement.py    
    ├── balance_sheet.py       
    ├── cash_flow.py           
    ├── earnings.py            
    ├── earnings_estimates.py  
    ├── insider.py             
    ├── sentiment.py           
    ├── etf_profile.py         
    ├── forex.py               
    ├── indices.py             
    ├── cryptocurrencies.py    
    ├── commodities.py         
    └── economic.py            
```

Tests live in `tests/historical_data_setup/` (see [tests/README.md](../tests/README.md)).
