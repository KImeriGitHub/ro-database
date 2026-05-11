# Asset Catalog Service

Manages all catalog parquet files that track the universe of tradeable assets and their lifecycle status. Two entry points: `init_catalog.py` for first-time setup (optionally enhanced with FirstRate Data) and `update_catalog.py` for daily maintenance.

## When to run

1. **Initial setup** (`init_catalog.py`) - before historical data download. Creates all catalog parquet files from scratch. Optionally incorporates FirstRate Data catalogs for survivorship bias-free coverage.
2. **Daily** (`update_catalog.py`) - before the daily data fetching pipeline. Updates existing catalogs with changes (new listings, delistings, status changes). Does not use FirstRate Data.

## Usage

```bash
# Initial setup (AV only, ~10k OVERVIEW queries for stock sectors, ~3 hours at AV_RATE_LIMIT_PER_MIN=70)
python asset_catalog_service/init_catalog.py

# Initial setup with FirstRate Data
python asset_catalog_service/init_catalog.py \
    --stocks-dir /path/to/firstrate/stocks \
    --etfs-dir /path/to/firstrate/etfs

# Custom output directory
python asset_catalog_service/init_catalog.py --catalog-dir /path/to/catalog

# Daily update
python asset_catalog_service/update_catalog.py

# Custom output directory
python asset_catalog_service/update_catalog.py --catalog-dir /path/to/catalog
```

Or import directly:

```python
from asset_catalog_service.init_catalog import init_all
init_all()

from asset_catalog_service.update_catalog import update_all
update_all()
```

## Catalog files

### stocks.parquet

**Schema:** `symbol (Utf8), name (Utf8), sector (Utf8), ipoDate (Date), delistingDate (Date), status (Utf8)`.

**Source:** Alpha Vantage `LISTING_STATUS` (active + delisted) + `OVERVIEW` (sector). Optionally enhanced with FirstRate Data `catalog_stocks.csv`.

**Init (`init_catalog.py`):**
1. If FirstRate stock directory provided: load `catalog_stocks.csv`. Validate that the file exists and contains all required headers (`Ticker`, `Company Name`, `Sector`, `IPO Date`, `Status`). Abort if validation fails. Normalize sector values (see Sector Normalization).
2. Query AV `LISTING_STATUS` (active + delisted), filter to stocks by `assetType`.
3. Merge both sources. FirstRate data takes precedence for overlapping symbols.
   - Log symbols present in FirstRate CSV but not in AV.
   - Log symbols present in AV but not in FirstRate CSV.
   - Log status disagreements for symbols present in both.
4. For stock symbols still missing a sector after the merge: query `OVERVIEW` per symbol to get the sector. Normalize the sector value.
5. If no FirstRate data provided: query AV `LISTING_STATUS` (2 calls) + `OVERVIEW` for every stock symbol (~10k+ API calls, ~3 hours at `AV_RATE_LIMIT_PER_MIN` (currently 70 in [config/settings.py](../config/settings.py))).

**Update (`update_catalog.py`):**
- No FirstRate Data incorporation.
- New symbols: appended. `OVERVIEW` queried for the new symbol's sector. Logged with full row details.
- Missing keys (in catalog but gone from API):
  - If `delistingDate` is null: set `delistingDate` to today, `status` to `Corrupted`.
  - If `delistingDate` is already set and older than 30 days: `status` promoted to `Delisted`.
- `ipoDate` moved earlier for an existing symbol: `ipoDate` updated to the earlier value and `status` set to `Corrupted` (data integrity concern). A later fresh `ipoDate` is ignored so the earliest known data window is preserved and the same row is not re-flagged on every run.
- `delistingDate` changed: updated to the new value and logged.

### etfs.parquet

**Schema:** `symbol (Utf8), name (Utf8), ipoDate (Date), delistingDate (Date), status (Utf8)`.

**Source:** Alpha Vantage `LISTING_STATUS` (active + delisted). Optionally enhanced with FirstRate Data `catalog_etfs.csv`.

**Init (`init_catalog.py`):**
1. If FirstRate ETF directory provided: load `catalog_etfs.csv`. Validate that the file exists and contains all required headers (`Ticker`, `Name`, `IPO Date`, `Status`). Abort if validation fails.
2. Query AV `LISTING_STATUS` (active + delisted), filter to ETFs by `assetType`.
3. Merge both sources. FirstRate data takes precedence for overlapping symbols.
   - Log symbols present in FirstRate CSV but not in AV.
   - Log symbols present in AV but not in FirstRate CSV.
   - Log status disagreements for symbols present in both.

**Update (`update_catalog.py`):**
- Same update logic as stocks (new symbols, vanished symbols, date changes) but no `OVERVIEW` query needed (ETFs have no sector).

**API calls (stocks + ETFs combined):**

| Scenario | Calls |
|---|---|
| Init with FirstRate Data (sectors covered by CSV) | 2 (`LISTING_STATUS`) + OVERVIEW only for symbols without sector from CSV |
| Init without FirstRate Data | 2 (`LISTING_STATUS`) + ~10k (`OVERVIEW`) |
| Daily update | 2 (`LISTING_STATUS`) + 1 `OVERVIEW` per new stock symbol |

### indices.parquet

**Source:** Alpha Vantage `INDEX_CATALOG` (JSON, key = symbol, value = name).

**Schema:** `symbol, name, ipoDate (Date), delistingDate (Date), status (Utf8)`.

**Init:** All entries added with `ipoDate`, `delistingDate`, and `status` set to null.

**Update logic:**
- New keys: added with null dates/status, logged.
- Missing keys (in catalog but gone from API):
  - If `delistingDate` is null: set `delistingDate` to today, `status` to `Corrupted`.
  - If `delistingDate` is already set and older than 30 days: `status` promoted to `Delisted`.

**API calls:** 1.

### forex.parquet

**Source:** `https://www.alphavantage.co/physical_currency_list/` (CSV: `currency code, currency name`).

**Schema:** `symbol, name, ipoDate (Date), delistingDate (Date), status (Utf8)`.

Symbols are constructed as `{currency_code}USD` (e.g., `AEDUSD`, `JPYUSD`).

**Update logic:** Same as indices.

**API calls:** 0 (static list, not counted against rate limit).

### cryptocurrencies.parquet

**Source:** `https://www.alphavantage.co/cryptocurrency_list/` (CSV: `from_currency, to_currency`).

**Schema:** `symbol, name, ipoDate (Date), delistingDate (Date), status (Utf8)`.

Only rows where `to_currency == "USD"` are kept. Symbol is the `from_currency` value. Name follows the format `Cryptocurrency {symbol} for Market {market}`.

**Update logic:** Same as indices.

**API calls:** 0 (static list).

### commodities.parquet

**Source:** Hard-coded list of 13 commodity symbols (XAU, XAG, WTI, BRENT, NATURAL_GAS, COPPER, ALUMINUM, WHEAT, CORN, COTTON, SUGAR, COFFEE, ALL_COMMODITIES).

**Schema:** `symbol, name, status`.

**Behaviour:** Created once with all statuses set to `Active`. Never modified by subsequent runs.

**API calls:** 0.

### economic.parquet

**Source:** Hard-coded list of 15 economic indicator symbols (REAL_GDP, REAL_GDP_PER_CAPITA, TREASURY_YIELD_30Y, TREASURY_YIELD_10Y, TREASURY_YIELD_7Y, TREASURY_YIELD_5Y, TREASURY_YIELD_2Y, TREASURY_YIELD_3M, FEDERAL_FUNDS_RATE, CPI, INFLATION, RETAIL_SALES, DURABLES, UNEMPLOYMENT, NONFARM_PAYROLL).

**Schema:** `symbol, name, status`.

**Behaviour:** Same as commodities - created once, never modified.

**API calls:** 0.

### yield_status.parquet

**Source:** Derived from all asset catalog parquet files (stocks, etfs, forex, indices, cryptocurrencies, commodities, economic).

**Schema:** `symbol (Utf8), prices (Boolean), prices_daily (Boolean), income_statement (Boolean), balance_sheet (Boolean), cash_flow (Boolean), earnings (Boolean), earnings_estimates (Boolean), insider (Boolean), sentiment (Boolean), etf_profile (Boolean), direct (Boolean), date (Date)`.

Columns correspond to data endpoints. Stock-specific columns (`prices`, `prices_daily`, `income_statement`, `balance_sheet`, `cash_flow`, `earnings`, `earnings_estimates`, `insider`, `sentiment`) apply to stocks. `etf_profile` applies to ETFs. `direct` applies to forex, indices, cryptocurrencies, commodities, and economic indicators. If a symbol and column do not match, the cell is left null and ignored.

**Init:** All yield columns set to null, `date` set to the current date.

**Finalize (end of full historical setup run):** `yield_status.parquet` is completely overwritten. Applicable (symbol, column) pairs default to True and are flipped to False per the ingestion report:

- `structure_error` or `av_throttle` -> False (no usable data).
- `empty_content` -> False, except for fundamental endpoints (`income_statement`, `balance_sheet`, `cash_flow`, `earnings`, `earnings_estimates`) where True is preserved if at least one of the annual/quarterly files was saved (partial save is acceptable).
- `cast_failure` / `timezone_mismatch` -> True (data was saved; only malformed entries became null).

Ingestion-report endpoints `forex`, `indices`, `cryptocurrencies`, `commodities`, `economic` map to the single `direct` yield column. Non-applicable (symbol, column) pairs stay null.

All rows share the same `date`, set to the last fully-traded ET date at the start of the run (weekend -> start date; weekday before 20:00 ET -> start date minus one day; otherwise start date). The start time is recovered from the mtime of `historical/.setup_started_at`, which is preserved across resumes and deleted after a successful finalize.

Finalize runs only when no `--asset-types` / `--endpoints` subset flags were passed to `setup_historical.py`; those flags target non-daily backfills and intentionally skip finalize.

**Update:** Yield status is also updated through `ingestion_report.parquet` at the end of the daily data pipeline.

**API calls:** 0.

> **`earnings_calendar.parquet` moved.** This file used to live in `catalog/`
> and was rebuilt by `init_catalog`/`update_catalog`. It now belongs to the
> data-pull folders -- `historical/earnings_calendar.parquet` and
> `daily/<YYYY-MM-DD>/earnings_calendar.parquet` -- and is fetched by
> `historical_data_setup` / `daily_data_service`. The catalog flow no
> longer touches it. See [historical_data_setup/SPEC.md](../historical_data_setup/SPEC.md#earnings_calendar)
> and [daily_data_service/SPEC.md](../daily_data_service/SPEC.md#earnings_calendar).

## FirstRate Data requirements

FirstRate Data catalogs are optional CSV files that provide survivorship bias-free coverage (including delisted securities). If provided to `init_catalog.py`, both the file and its required headers are validated before any processing begins. If either validation fails, the process aborts.

### Stock catalog (`catalog_stocks.csv`)

Must be located in the directory passed via `--stocks-dir`.

**Required headers:** `Ticker`, `Company Name`, `Sector`, `IPO Date`, `Status`

**Optional headers:** `Delisting Date` (and possibly others, which are ignored).

Sector values are normalized to the standard sector list (see Sector Normalization). Empty or unrecognized values are mapped to `Other`.

### ETF catalog (`catalog_etfs.csv`)

Must be located in the directory passed via `--etfs-dir`.

**Required headers:** `Ticker`, `Name`, `IPO Date`, `Status`

**Optional headers:** `Delisting Date` (and possibly others, which are ignored).

### Validation rules

- If `--stocks-dir` is provided, `catalog_stocks.csv` must exist in that directory and contain all required headers.
- If `--etfs-dir` is provided, `catalog_etfs.csv` must exist in that directory and contain all required headers.
- If both directories are provided, both CSVs are validated before any processing starts. If either fails, the entire process aborts.
- Neither directory is required. If neither is provided, `init_catalog.py` uses AV data only.

## Sector normalization

Both `init_catalog.py` and `update_catalog.py` normalize sector values to a canonical set. Sectors from different sources (FirstRate CSV, AV `OVERVIEW` response) use different casing and naming conventions.

**Canonical sectors:**

| Canonical | FirstRate CSV values | AV OVERVIEW values |
|---|---|---|
| Basic Materials | `Basic Materials` | `BASIC MATERIALS` |
| Communication Services | `Communication Services` | `COMMUNICATION SERVICES` |
| Consumer Cyclical | `Consumer Cyclical` | `CONSUMER CYCLICAL` |
| Consumer Defensive | `Consumer Defensive` | `CONSUMER DEFENSIVE`, `CONSUMER STAPLES` |
| Energy | `Energy` | `ENERGY` |
| Financial Services | `Financial Services` | `FINANCIAL SERVICES`, `FINANCIALS` |
| Healthcare | `Healthcare` | `HEALTHCARE` |
| Industrials | `Industrials` | `INDUSTRIALS` |
| Real Estate | `Real Estate` | `REAL ESTATE` |
| Technology | `Technology` | `TECHNOLOGY` |
| Utilities | `Utilities` | `UTILITIES` |
| Other | empty, unrecognized | `null`, `NONE`, `OTHER`, unrecognized |

## Total API calls per run

### init_catalog.py

| Endpoint | Calls |
|---|---|
| LISTING_STATUS (active) | 1 |
| LISTING_STATUS (delisted) | 1 |
| OVERVIEW (per stock without sector) | variable (0 if FirstRate covers all, ~10k+ if no FirstRate) |
| INDEX_CATALOG | 1 |
| **Total /query calls** | **3 + OVERVIEW calls** |
| physical_currency_list (static) | 1 |
| cryptocurrency_list (static) | 1 |

### update_catalog.py

| Endpoint | Calls |
|---|---|
| LISTING_STATUS (active) | 1 |
| LISTING_STATUS (delisted) | 1 |
| OVERVIEW (per new stock symbol) | variable (typically 0-few) |
| INDEX_CATALOG | 1 |
| **Total /query calls** | **3 + OVERVIEW calls** |
| physical_currency_list (static) | 1 |
| cryptocurrency_list (static) | 1 |

## Error handling

Each catalog update runs independently. If one step fails (network error, API rate limit, malformed response), the error is logged and the remaining catalogs still update. The `fetch_text` helper rejects responses that return JSON instead of CSV (common AV error pattern for rate-limited or invalid requests).

**FirstRate validation errors are fatal.** If a provided directory is missing its expected CSV or required headers, the entire `init_catalog.py` process aborts before any API calls or writes.

## Design considerations

- **Two scripts, two modes:** `init_catalog.py` is idempotent but expensive (especially without FirstRate Data). `update_catalog.py` is cheap and incremental. Separating them makes the cost explicit and prevents accidental re-initialization.
- **FirstRate precedence:** When both sources provide data for the same symbol, FirstRate wins. This reflects that FirstRate Data is a curated, purchased dataset with better delisting coverage. Disagreements are logged for review.
- **Sector via OVERVIEW:** The `LISTING_STATUS` endpoint does not return sector information. Sectors require a per-symbol `OVERVIEW` query. FirstRate Data provides sectors in bulk, avoiding thousands of API calls.
- **Corrupted vs Delisted:** A missing symbol is first marked `Corrupted` (with today's date). Only after 30+ days of continuous absence does it become `Delisted`. This two-stage approach avoids prematurely marking symbols as delisted due to transient API issues.
- **ipoDate as integrity signal:** If Alpha Vantage moves a stock's IPO date earlier, the symbol is marked `Corrupted` for manual review and the new earlier date is written to the catalog so the change is not re-detected on every subsequent run. A fresh ipoDate that is *later* than the stored value is ignored: under the min-ipoDate fetch rule (below) any persisted earlier date is the truthful boundary, and a later AV value typically means a delisted-row contribution dropped out of the response.
- **Re-issued tickers (active wins, earliest ipoDate kept):** AV's `LISTING_STATUS` can return the same `symbol` string twice when a ticker was reused: once in `state=delisted` for the original company (with its old `ipoDate` and a `delistingDate`) and once in `state=active` for the new company that now trades under it (with a newer `ipoDate` and null `delistingDate`). `_fetch_av_listings` deduplicates by symbol with the active row winning for `name`/`status`/`delistingDate`, but `ipoDate` is set to the minimum across the active and delisted rows. This keeps the catalog pointing at the currently-trading entity (so the yield-aware pipeline does not skip a live ticker) while preserving the earliest date for which any data may exist under the ticker, e.g. for survivorship-bias-aware queries against `historical/`. Note: `symbol` is treated as a primary key throughout the catalog, so representing both issuers as separate rows would require a richer key (e.g. `symbol + ipoDate`) and is out of scope.
- **Static catalogs are immutable:** Commodities and economic indicators are fixed lists defined in code. They are created once and never touched again by the catalog script.
- **Yield status init only:** This script only initialises `yield_status.parquet`. The actual yield tracking (marking which symbols return data for which endpoints) is updated through `ingestion_report.parquet` at the end of the daily or historical data pipeline.
- **Date columns as pl.Date for stocks/etfs:** Date strings from the LISTING_STATUS CSV are cast to `pl.Date` on ingestion. This enables the same 30-day delistingDate arithmetic used by indices/forex/crypto.
- **Execution order matters:** `update_yield_status` depends on all asset catalog parquet files existing, so it runs last in the catalog pipeline.
- **Validate before work:** When FirstRate directories are provided, both CSVs are fully validated (existence + headers) before any API calls are made or data is written. This prevents partial state from a late validation failure.

## Folder structure

```
asset_catalog_service/
├── __init__.py
├── init_catalog.py                # Initial setup orchestrator (FirstRate + AV)
├── update_catalog.py              # Daily update orchestrator
├── README.md
└── updates/
    ├── __init__.py                # Re-exports all update functions
    ├── _common.py                 # Shared constants, HTTP helpers, sector normalization
    ├── stocks_etfs.py             # stocks.parquet + etfs.parquet (init + update)
    ├── indices.py                 # indices.parquet
    ├── forex.py                   # forex.parquet
    ├── cryptocurrencies.py        # cryptocurrencies.parquet
    ├── commodities.py             # commodities.parquet (static)
    ├── economic.py                # economic.parquet (static)
    └── yield_status.py            # yield_status.parquet (init only)
```

Tests live in `tests/asset_catalog_service/` (see [tests/README.md](../tests/README.md)).

## Dependencies

- `polars` - DataFrame operations and parquet I/O
- `requests` - HTTP calls to Alpha Vantage
- `maintainance_scripts.get_api_key` - API key loading
