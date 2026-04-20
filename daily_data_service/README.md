# Daily Data Service

Daily incremental pull from Alpha Vantage. Same endpoint coverage as [`historical_data_setup/`](../historical_data_setup/README.md), but every endpoint is truncated to a narrow recent window and written to a new date-stamped folder. FirstRate Data is **not** used. Resumable on crash via a top-level start marker.

## Relationship to historical_data_setup

The daily service mirrors historical's orchestration (async, cross-endpoint concurrency, shared sliding-window rate limiter, issue tracker, resume-by-file-existence) and reuses its primitives directly from [`historical_data_setup/_common.py`](../historical_data_setup/_common.py):

| Reused 1-to-1 | Rewritten for daily |
|---|---|
| `RateLimiter` | all files under `endpoints/` (different output dir, truncation, no FRD) |
| `fetch_av_json`, `AVResponseError` | `setup_daily.py` (orchestrator) |
| `IssueTracker` | `ensure_folders.py` (date-stamped tree) |
| `read_catalog_symbols`, `validate_meta_data` | `_common.py` (folder-date + previous-date resolution) |
| `_build_fundamental_df` | |

FRD helpers (`frd_csv_path`), `generate_months`, and `fetch_fundamental_endpoint` are historical-only.

## Data folder structure

```
daily/
├── .setup_started_at               # mtime = start datetime; drives folder-date across resumes
└── YYYY-MM-DD/                     # folder-date (see "Date resolution" below)
    ├── stocks/
    │   ├── prices/                 # SYMBOL.parquet (1-min, truncated)
    │   ├── prices_daily/           # SYMBOL.parquet (daily, truncated)
    │   ├── income_statement/       # SYMBOL_annual.parquet + SYMBOL_quarterly.parquet (last 5y)
    │   ├── balance_sheet/
    │   ├── cash_flow/
    │   ├── earnings/
    │   ├── earnings_estimates/
    │   ├── insider/                # SYMBOL.parquet (last year on transactionDate)
    │   └── sentiment/              # ALL_MESSAGES.parquet + {SYMBOL}.parquet
    ├── etfs/
    │   ├── prices/
    │   ├── prices_daily/
    │   └── etf_profile/            # SYMBOL.parquet with date = folder-date
    ├── forex/
    ├── indices/
    ├── cryptocurrencies/
    ├── commodities/
    ├── economic/
    └── ingestion_report.parquet
```

## Date resolution

Two dates drive every endpoint's truncation: **folder-date** (the anchor of the day's pull) and **previous-date** (the anchor of the most recent successful pull).

### folder-date

Computed from the execution start time (ET):

- Weekend -> start date.
- Weekday, time >= 20:00 ET -> start date.
- Weekday, time <  20:00 ET -> start date minus one day.

On every run, `setup_daily.py` checks for `daily/.setup_started_at`. If present, the start time is recovered from its mtime and the folder-date is recomputed from that (resume path). Otherwise a new marker is touched and the folder-date comes from the current time. The marker is deleted only on a successful full-run finalize.

This keeps folder-date stable across crashes and across calendar-day rollovers during a long resumed run.

### previous-date

Read from `catalog/yield_status.parquet`'s `date` column (all rows share the same value; read any one). This is the folder-date of the prior successful full run.

The truncation windows use **`(previous-date, folder-date]`** -- previous-date excluded, folder-date included.

### Same-day no-op

If `previous-date == folder-date`, the day's pull has already been finalized; the run logs a no-op message and exits without touching any endpoint, the ingestion report, or the marker. Rerun only after the next fully-traded ET day closes.

## Truncation rules

| Endpoint | Asset types | AV call | Truncation |
|---|---|---|---|
| `prices` | stocks, etfs | `TIME_SERIES_INTRADAY`, `interval=1min`, `adjusted=false`, `outputsize=full` (no `month` param) | `Date` in `(previous-date, folder-date]` |
| `prices_daily` | stocks, etfs | `TIME_SERIES_DAILY_ADJUSTED`, **`outputsize=compact`** | `Date` in `(previous-date, folder-date]` |
| `income_statement` | stocks | `INCOME_STATEMENT` | `fiscalDateEnding >= folder-date - 5 years` |
| `balance_sheet` | stocks | `BALANCE_SHEET` | `fiscalDateEnding >= folder-date - 5 years` |
| `cash_flow` | stocks | `CASH_FLOW` | `fiscalDateEnding >= folder-date - 5 years` |
| `earnings` | stocks | `EARNINGS` | `fiscalDateEnding >= folder-date - 5 years` |
| `earnings_estimates` | stocks | `EARNINGS_ESTIMATES` | `fiscalDateEnding >= folder-date - 5 years` |
| `insider` | stocks | `INSIDER_TRANSACTIONS` | `transactionDate >= folder-date - 1 year` |
| `sentiment` | stocks | `NEWS_SENTIMENT`, backward pagination | `time_from = previous-date 00:00 UTC` (INCLUDING) to current UTC time |
| `etf_profile` | etfs | `ETF_PROFILE` | no truncation; `date` column set to folder-date |
| `forex` | forex | `FX_DAILY`, **`outputsize=compact`** | `Date` in `(previous-date, folder-date]` |
| `cryptocurrencies` | cryptocurrencies | `DIGITAL_CURRENCY_DAILY` | `Date` in `(previous-date, folder-date]` |
| `commodities` (daily group: WTI, BRENT, NATURAL_GAS, XAU, XAG) | commodities | `interval=daily` or `GOLD_SILVER_HISTORY` | `Date` in `(previous-date, folder-date]` |
| `commodities` (monthly group: COPPER, ALUMINUM, WHEAT, CORN, COTTON, SUGAR, COFFEE, ALL_COMMODITIES) | commodities | `interval=monthly` | `Date >= folder-date - 1 year` |
| `economic` (daily indicators: TREASURY_YIELD_*, FEDERAL_FUNDS_RATE) | economic | `interval=daily` | `Date` in `(previous-date, folder-date]` |
| `economic` (non-daily indicators: REAL_GDP, CPI, ...) | economic | (default interval per indicator) | `Date >= folder-date - 1 year` |
| `indices` | indices | `INDEX_DATA`, `interval=daily` | `Date` in `(previous-date, folder-date]` |

### Notes on specific endpoints

- **prices (intraday)**: the `month` parameter is intentionally **omitted**. With `outputsize=full`, Alpha Vantage returns the trailing 30 days of 1-min bars regardless of month boundary, which cleanly covers any reasonable `(previous-date, folder-date]` gap including cross-month rollovers.
- **prices_daily / forex `outputsize=compact`**: returns the trailing ~100 data points, which covers any reasonable previous-date-to-folder-date gap and saves payload.
- **sentiment**: `ALL_MESSAGES.parquet` is built first (paginated backward from current UTC to the start of previous-date, inclusive), then filtered to catalog symbols, deduplicated on `(url, ticker)`, and split into per-symbol `{SYMBOL}.parquet` files for active symbols (same logic as historical).
- **etf_profile**: one row per ETF; the `date` field is the folder-date (not the run-time date).
- **commodities / economic**: the "daily group" vs "other" split mirrors the per-symbol interval choice already baked into the historical endpoints; the monthly / non-daily rows get a 1-year window because `(previous-date, folder-date]` would usually be empty.

## Output schemas

Every parquet file is schema-identical to its historical counterpart (same column names and dtypes, just fewer rows). See [`historical_data_setup/README.md`](../historical_data_setup/README.md#stocks--etfs) for per-endpoint schemas.

## Usage

```bash
# Create today's folder structure only
python daily_data_service/ensure_folders.py

# Full run (all applicable asset-type x endpoint pairs)
python daily_data_service/setup_daily.py

# Subsetting (partial run; skips yield_status finalize)
python daily_data_service/setup_daily.py --asset-types stocks --endpoints prices_daily
python daily_data_service/setup_daily.py --endpoints sentiment

# Custom paths
python daily_data_service/setup_daily.py --catalog-dir /path/to/catalog --daily-dir /path/to/daily

# Standard-tier API key (default is premium)
python daily_data_service/setup_daily.py --api-tier standard
```

## Recovery and resume

Resumable by design. Just rerun the same command on crash; already-written per-symbol parquet files are skipped.

- **Per-symbol file-level resume.** Each endpoint checks whether the destination parquet already exists under the current folder-date and skips it if so.
- **Stable folder-date across resumes.** Captured once via the mtime of `daily/.setup_started_at`. On every startup, the service reads the marker's mtime and recomputes the folder-date from that same start time, so a restarted run keeps its original folder-date even when the restart happens on a later calendar day. The marker is deleted only on a successful full-run finalize.
- **Top-level exception isolation.** Each `(asset_type, endpoint)` task is wrapped so one endpoint crashing does not tear down the `asyncio.gather`; other tasks keep running and the failed one is retried on the next run.
- **Append-only writes.** Files are written atomically per symbol.

Operational notes:
- A full rerun of the same command is always the right recovery action.
- `ingestion_report.parquet` is overwritten within its folder-date each run; it reflects issues seen during the run that produced it.
- `daily/.setup_started_at`'s presence means a daily run is in progress or never cleanly finished; leave it alone.
- To force a clean restart of the current day, delete the `daily/YYYY-MM-DD/` tree AND `daily/.setup_started_at`.
- Partial runs with `--asset-types` / `--endpoints` intentionally skip the finalize step and do not delete the marker.

## Cross-endpoint concurrency and rate limiting

Identical to historical: one `asyncio` task per `(asset_type, endpoint)` pair, all sharing a single `aiohttp.ClientSession`, a single `RateLimiter` (sliding 60s window, 74 calls/min), and a single `IssueTracker`. See [`historical_data_setup/README.md`](../historical_data_setup/README.md#rate-limiting-and-cross-endpoint-execution).

In practice, the daily call volume is a tiny fraction of a full historical pull (most endpoints do one call per symbol at most, and truncation is applied client-side after the fetch), so the budget is never the bottleneck -- wall-clock is dominated by intraday `prices` and `sentiment` paging.

## Ingestion report

Same schema as historical's `ingestion_report.parquet`. Saved at `daily/YYYY-MM-DD/ingestion_report.parquet`:

| Column | Type | Description |
|--------|------|-------------|
| symbol | Utf8 | Ticker symbol |
| asset_type | Utf8 | stocks, etfs, forex, indices, cryptocurrencies, commodities, or economic |
| endpoint | Utf8 | prices, prices_daily, etc. |
| issue_type | Utf8 | structure_error, empty_content, cast_failure, timezone_mismatch, av_throttle |
| detail | Utf8 | Specifics |
| timestamp | Datetime | When the issue was recorded |

## Finalizing yield_status

At the end of a **full run**, `catalog/yield_status.parquet` is overwritten with the same logic as historical (see [historical README: Finalizing yield_status](../historical_data_setup/README.md#finalizing-yield_status)). The only difference is `data_complete_date = folder-date`.

Cells resolved per the same rules:

| Ingestion-report issue for (symbol, endpoint) | Resulting cell |
|---|---|
| none | True |
| `structure_error` | False |
| `av_throttle` | False |
| `empty_content` (non-fundamental endpoint) | False |
| `empty_content` (fundamental endpoint) | True if `{symbol}_annual.parquet` or `{symbol}_quarterly.parquet` exists, else False |
| `cast_failure` | True |
| `timezone_mismatch` | True |

Partial runs (`--asset-types` / `--endpoints`) skip finalize to avoid flipping columns for endpoints that were not part of the run.

## Module structure

```
daily_data_service/
├── __init__.py
├── README.md
├── ensure_folders.py       # create daily/YYYY-MM-DD/ subtree for a given folder-date
├── _common.py              # compute_folder_date, read_previous_date, date-window helpers
├── setup_daily.py          # async CLI orchestrator (analogous to setup_historical.py)
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

Each endpoint imports shared primitives from `historical_data_setup._common` and service-specific helpers from `daily_data_service._common`.
