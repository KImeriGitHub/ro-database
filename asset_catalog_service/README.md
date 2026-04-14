# Asset Catalog Service

Manages all catalog parquet files that track the universe of tradeable assets and their lifecycle status. The script is the single entry point for both initial setup and daily maintenance of the catalog.

## When to run

1. **Initial setup** - before historical data download. Creates all catalog parquet files from scratch.
2. **Daily** - before the daily data fetching pipeline. Updates existing catalogs with changes (new listings, delistings, status changes).

The script detects which mode to use based on whether the parquet files already exist.

## Usage

```bash
# Default: writes to <project_root>/catalog/
python asset_catalog_service/update_catalog.py

# Custom output directory
python asset_catalog_service/update_catalog.py --catalog-dir /path/to/catalog
```

Or import directly:

```python
from asset_catalog_service.update_catalog import update_all
update_all()
```

## Catalog files

### stocks.parquet / etfs.parquet

**Source:** Alpha Vantage `LISTING_STATUS` (active + delisted).

**Schema:** `symbol (Utf8), name (Utf8), exchange (Utf8), ipoDate (Date), delistingDate (Date), status (Utf8)`.

**Init:** Query both `state=active` and `state=delisted`, combine, split by `assetType` into stocks and ETFs.

**Update logic:**
- New symbols: appended, logged with full row details.
- Vanished symbols (in catalog but not in fresh data):
  - If `delistingDate` is null: set `delistingDate` to today, `status` to `Corrupted`.
  - If `delistingDate` is already set and older than 30 days: `status` promoted to `Delisted`.
- `ipoDate` changed for an existing symbol: `status` set to `Corrupted` (data integrity concern).
- `delistingDate` changed: updated to the new value and logged.

**API calls:** 2 (active + delisted listing).

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

**Update:** Yield status is updated through `ingestion_report.parquet` at the end of the daily or historical data pipeline.

**API calls:** 0.

### earnings_calendar.parquet

**Source:** Alpha Vantage `EARNINGS_CALENDAR` with `horizon=6month`.

**Schema:** `symbol (Utf8), name (Utf8), reportDate (Date), fiscalDateEnding (Date), estimate (Float32), currency (Utf8), timeOfTheDay (Utf8), cast_issues (Utf8)`.

**Behaviour:** Always fetched and overwritten, regardless of whether the file exists. The `cast_issues` column records which fields (if any) failed type casting for each row (e.g., `"reportDate,estimate"`). Rows where a cast failed have null in the affected typed column and the original value is not preserved.

**Logging:** Three checkpoints are logged:
1. Whether the CSV was fetched successfully.
2. Whether any rows had cast issues (count reported).
3. Whether the save completed.

**API calls:** 1.

## Total API calls per run

| Endpoint | Calls |
|---|---|
| LISTING_STATUS (active) | 1 |
| LISTING_STATUS (delisted) | 1 |
| INDEX_CATALOG | 1 |
| EARNINGS_CALENDAR | 1 |
| **Total /query calls** | **4** |
| physical_currency_list (static) | 1 |
| cryptocurrency_list (static) | 1 |

The 4 `/query` calls count against the Alpha Vantage rate limit (~75 calls/min). The two static list URLs do not.

## Error handling

Each catalog update runs independently. If one step fails (network error, API rate limit, malformed response), the error is logged and the remaining catalogs still update. The `_fetch_text` helper rejects responses that return JSON instead of CSV (common AV error pattern for rate-limited or invalid requests).

## Design considerations

- **Corrupted vs Delisted:** A missing symbol is first marked `Corrupted` (with today's date). Only after 30+ days of continuous absence does it become `Delisted`. This two-stage approach avoids prematurely marking symbols as delisted due to transient API issues.
- **ipoDate as integrity signal:** If Alpha Vantage changes a stock's IPO date, this is a data integrity red flag. The symbol is marked `Corrupted` for manual review rather than silently accepting the change.
- **Static catalogs are immutable:** Commodities and economic indicators are fixed lists defined in code. They are created once and never touched again by the catalog script.
- **Yield status init only:** This script only initialises `yield_status.parquet`. The actual yield tracking (marking which symbols return data for which endpoints) is updated through `ingestion_report.parquet` at the end of the daily or historical data pipeline.
- **Date columns as pl.Date for stocks/etfs:** Date strings from the LISTING_STATUS CSV are cast to `pl.Date` on ingestion. This enables the same 30-day delistingDate arithmetic used by indices/forex/crypto.
- **Execution order matters:** `update_yield_status` depends on all asset catalog parquet files existing, so it runs last.

## Folder structure

```
asset_catalog_service/
├── __init__.py
├── update_catalog.py              # Entry point / orchestrator
├── README.md
└── updates/
    ├── __init__.py                # Re-exports all update functions
    ├── _common.py                 # Shared constants, HTTP helpers, update_simple_catalog
    ├── stocks_etfs.py             # stocks.parquet + etfs.parquet
    ├── indices.py                 # indices.parquet
    ├── forex.py                   # forex.parquet
    ├── cryptocurrencies.py        # cryptocurrencies.parquet
    ├── commodities.py             # commodities.parquet (static)
    ├── economic.py                # economic.parquet (static)
    ├── yield_status.py            # yield_status.parquet (init only)
    └── earnings_calendar.py       # earnings_calendar.parquet (always overwrite)
```

Tests live in `tests/asset_catalog_service/` (see [tests/README.md](../tests/README.md)).

## Dependencies

- `polars` - DataFrame operations and parquet I/O
- `requests` - HTTP calls to Alpha Vantage
- `maintainance_scripts.get_api_key` - API key loading
