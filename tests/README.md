# Tests

Unified test directory for the ro-database project.

## Structure

```
tests/
├── asset_catalog_service/      # Tests for asset_catalog_service
│   ├── mock_catalog/           # Temp catalog dir used by tests (created/cleaned per test)
│   ├── test_init.py            # Tests initial catalog creation (no parquets exist)
│   └── test_daily.py           # Tests daily update logic (parquets already exist)
├── daily_data_service/         # Tests for daily_data_service
│   ├── test_adjust_weekly.py   # Weekend retry pass: dates, retry plan,
│   │                           # sentiment rename, ingestion-report merge,
│   │                           # stubbed end-to-end orchestrator
│   └── test_common.py          # compute_folder_date thresholds, .setup_started_at
│                               # marker resume, read_previous_date / read_yield_skip_set,
│                               # window_expr / since_expr / years_before, ensure_daily_folders
├── maintainance_scripts/       # Tests for maintainance_scripts
│   ├── test_get_api_key.py     # Local file + Secret Manager fallback resolution
│   ├── test_logging_setup.py   # CloudLoggingJsonFormatter + configure_logging idempotency
│   └── test_paths.py           # Local <-> GCS blob round-trip across catalog/historical/daily
├── historical_data_setup/      # Tests for historical_data_setup
│   ├── test_rate_limiter.py    # Sliding-window RateLimiter behavior
│   ├── test_cross_endpoint.py  # Cross-endpoint concurrency + shared rate limit
│   └── test_common.py          # _common helpers (generate_months, IssueTracker,
│                               # symbol_parquet_name, frd_csv_path,
│                               # validate_meta_data, fetch_av_json throttle/retry,
│                               # AV call counter, ensure_historical_folders)
├── monitoring_service/         # Tests for monitoring_service
│   ├── test_analyze_catalog.py    # catalog/*.parquet rollups
│   ├── test_analyze_ingestion.py  # ingestion_report.parquet rollups
│   ├── test_analyze_coverage.py   # SPY/MDY/EWJ/EWU/DIA/QQQ + QQQ-holdings probes
│   └── test_diff.py               # signed deltas vs previous monitoring_report.json
├── data_transformation/            # Tests for data_transformation
│   ├── test_asset_data_service.py  # AssetData dataclasses round-trip
│   ├── test_common.py              # source enumeration, sector lookup, schema cast, report
│   ├── test_dedup.py               # shared dedup-with-discrepancy-log helper
│   ├── test_overview.py            # Phase 1: assets_overview.parquet
│   ├── test_price_daily_simple.py  # Phase 2: forex/indices/crypto/commodities/economic
│   ├── test_shareprice_daily.py    # Phase 3: stocks/etfs daily + AdjClose/AdjVolume math
│   ├── test_shareprice_intraday.py # Phase 4: intraday + factor-frame join + tz strip
│   ├── test_etf_profile.py         # Phase 5: etf_profile + holdings List(Struct) round-trip
│   └── test_transform_cli.py       # End-to-end CLI via subprocess.run
├── integration_tests/               # End-to-end smoke tests against a real, persistent database/
│   ├── _helpers.py                  # MANDATORY_STOCKS/ETFS, reduce_catalogs, shared paths
│   ├── int_test_init_catalog.py     # init_catalog -> analyze_catalog -> reduce_catalogs
│   ├── int_test_update_catalog.py   # update_catalog with before/after symbol diff
│   ├── int_test_historical_setup.py # setup_historical (FRD-backed prices) + monitor checks
│   ├── int_test_run_daily.py        # setup_daily + monitor + prior-folder integrity check
│   ├── int_test_adjust_weekly.py    # adjust_weekly + monitor (weekend mode)
│   ├── int_test_transform.py        # transform.py + per-symbol output presence check
│   ├── int_helper_reduce_catalog.py # standalone re-trim of database/catalog/
│   ├── database/                    # populated by the scripts; persisted across runs
│   ├── frd_dir/                     # FRD CSVs (pre-populated for FRD-covered subset)
│   └── transformation/              # transform.py output
├── call_speedtests/                       # Scripts that measure real API call performance
│   ├── estimate_sentiment_calls.py        # NEWS_SENTIMENT backward pagination cost
│   ├── estimate_prices_calls.py           # TIME_SERIES_INTRADAY monthly pagination
│   ├── estimate_prices_daily_calls.py     # TIME_SERIES_DAILY_ADJUSTED cost
│   ├── estimate_fundamentals_calls.py     # INCOME_STATEMENT/BALANCE_SHEET/CASH_FLOW
│   ├── estimate_insider_calls.py          # INSIDER_TRANSACTIONS cost
│   ├── estimate_etf_profile_calls.py      # ETF_PROFILE cost
│   ├── estimate_forex_calls.py            # FX_DAILY cost
│   ├── estimate_cryptocurrencies_calls.py # DIGITAL_CURRENCY_DAILY cost
│   ├── estimate_commodities_calls.py      # WTI/BRENT/etc. cost
│   ├── estimate_indices_calls.py          # INDEX_DATA cost
│   └── estimate_economic_calls.py         # GDP/CPI/etc. cost
└── README.md
```

## Running tests

```bash
# All tests
pytest tests/

# Asset catalog service tests only
pytest tests/asset_catalog_service/

# A single test file
pytest tests/asset_catalog_service/test_init.py
```

## Subfolders

### asset_catalog_service

Unit tests for initial catalog creation and daily update logic. Uses mocked Alpha Vantage API responses (no real API calls). The `mock_catalog/` directory is created and cleaned up automatically by each test via a pytest fixture.

### daily_data_service

Unit tests for the daily incremental pull. No real network or catalog -- every test builds the inputs in `tmp_path`.

- `test_adjust_weekly.py` -- weekend retry orchestrator end-to-end with stubbed endpoint coroutines. Covers `resolve_dates` (full-week, fallback, non-date entries, empty), `_load_retry_plan` (per-(asset, endpoint) grouping including the `GLOBAL` sentinel), `_rename_sentiment_files`, `_merge_report` drop-and-append semantics, and four orchestrator scenarios (retry success / failure, sentiment full-rerun, fundamentals with `skip_empty_yield=False`, missing-report no-op).
- `test_common.py` -- `compute_folder_date` weekday/weekend cutoffs at 20:00 ET, `.setup_started_at` marker resume from mtime, `read_previous_date` and `read_yield_skip_set` over `yield_status.parquet` (only explicit `False` cells skipped, nulls stay queryable), `window_expr` strict `(prev, folder]` truncation on both Date and Datetime columns, `since_expr` inclusivity, `years_before` Feb-29 clamping, and `ensure_daily_folders` idempotency.

### maintainance_scripts

Unit tests for the shared utility modules. No real GCP, no real network -- Secret Manager and the GCS client are stubbed, and `K_SERVICE` is monkeypatched to flip the Cloud Run detection.

- `test_get_api_key.py` -- local file vs. Secret Manager fallback across all combinations of file-missing / tier-missing / placeholder values and the `USE_SECRET_MANAGER_FOR_AV_KEYS` flag.
- `test_paths.py` -- local-path helpers, GCS prefix helpers, and the `to_gcs_blob_name` <-> `to_local_path` round-trip across `catalog/`, `historical/`, `daily/<date>/` (including the Windows-backslash-to-POSIX-slash contract).
- `test_logging_setup.py` -- `detect_cloud_run`, the `CloudLoggingJsonFormatter` (severity / source location / extras / exception serialisation / underscore-prefix filtering), and `configure_logging` idempotency with text and JSON output modes.

### historical_data_setup

Unit tests covering the sliding-window rate limiter and cross-endpoint concurrency used by the historical setup pipeline. Pure asyncio tests -- no real network, no pytest-asyncio dependency (each test wraps its body with `asyncio.run`).

- `test_rate_limiter.py` -- verifies `RateLimiter` respects `calls_per_minute`, `window`, and `min_gap`; that concurrent waiters share the budget; and that the window slides forward as timestamps age out.
- `test_cross_endpoint.py` -- uses a hand-rolled mock `aiohttp.ClientSession` to confirm two endpoint coroutines interleave, never exceed the shared rate limit, and that a slow endpoint does not starve a fast one.
- `test_common.py` -- pure-helper coverage in `_common`: `generate_months` clamping/year-rollover, `IssueTracker` parquet round-trip and append-on-rerun, `symbol_parquet_name` Windows reserved-name protection, `frd_csv_path` lookup, `validate_meta_data` timezone branches, the `fetch_av_json` throttle-and-retry path (`Note` / `Information` keys, exhaustion -> `AVResponseError`), `get_av_call_count`/`reset_av_call_count`, and `ensure_historical_folders` idempotency.

### monitoring_service

Unit tests for the end-of-run monitoring report. Each analyzer is exercised
against a fresh temporary directory of synthetic parquet files (no real
catalog or ingestion report needed). Tests cover:

- `test_analyze_catalog.py` -- per-file status bucketing (Active / Delisted /
  Corrupted, case-insensitive), missing-file fallback, yield_status True /
  False / Null counts plus ratios, earnings_calendar averages.
- `test_analyze_ingestion.py` -- flat counts for `timezone_mismatch` and
  `av_throttle`, per-(asset_type, endpoint) breakdowns for the other issue
  types, missing-file fallback.
- `test_analyze_coverage.py` -- SPY/MDY/EWJ/EWU/DIA/QQQ probes including
  intraday row-count and per-OHLCV-column null-ratio thresholds, daily
  exact-row-count check, and QQQ-holdings extension when the profile is
  present.
- `test_diff.py` -- signed deltas vs a previous monitoring_report.json,
  including malformed/missing previous reports.

### data_transformation

Unit tests for the per-symbol transformation pipeline that builds
`AssetData` instances from raw `historical/` and `daily/` parquets.
Each test builds synthetic source files in `tmp_path` -- no real
catalog or daily folder is touched. Phase 6 (insider, sentiment,
financials) is not yet implemented; its test plan lives in
[../data_transformation/_tests_prompt.md](../data_transformation/_tests_prompt.md).

### integration_tests

Standalone scripts (not pytest) that exercise each major pipeline against a real, persistent `database/` folder under `tests/integration_tests/database/`. They make real Alpha Vantage calls and use the local `frd_dir/` for FirstRate-covered stock and ETF prices. The catalog is trimmed to a small fixed subset of symbols after init so subsequent runs stay cheap.

**Symbol subset.** Mandatory stocks: `AAPL, MSFT, GOOGL, AMZN, META, TSLA, NVDA, JPM, GS, BRK-B, IBM, T, NEE, SPG, O, TSM, F` plus 10 extras picked deterministically from the active stock catalog by SHA-256 ranking with a fixed seed (the ranking is stable across re-inits unless one of the picks disappears from AV's `LISTING_STATUS`). Mandatory ETFs: `QQQ, SPY, GLD, MDY, EWJ, EWU, DIA`. Trimming logic lives in `_helpers.reduce_catalogs`, which also propagates the trim to `yield_status.parquet` and `earnings_calendar.parquet`.

**Persistence.** None of the scripts wipe `database/` between runs. They are designed for chained execution (`init -> historical -> daily -> weekly -> transform`) and for manual inspection of intermediate state. Pass `--wipe` to `int_test_init_catalog.py` to start the catalog from scratch.

**Opting out of catalog reduction.** `int_test_init_catalog.py`, `int_test_update_catalog.py`, `int_test_run_daily.py`, and `int_test_adjust_weekly.py` each accept `--no-reduce` to skip the post-run trim. To trim a catalog later (e.g. after a `--no-reduce` run, or after a daily/weekly finalize that appended new symbols), run `int_helper_reduce_catalog.py`, which only calls `_helpers.reduce_catalogs` against `database/catalog/`.

**Suggested run order**

```bash
# 1. Build catalog from FRD CSVs + AV, then reduce
python tests/integration_tests/int_test_init_catalog.py [--wipe]

# 2. Pull historical (uses frd_dir for stocks/etfs prices, AV for everything else)
python tests/integration_tests/int_test_historical_setup.py

# 3. Daily incremental pull (also writes a monitoring report)
python tests/integration_tests/int_test_run_daily.py

# 4. Weekend retry pass (usually a no-op on the small int-test catalog)
python tests/integration_tests/int_test_adjust_weekly.py [--look-back-days 7]

# 5. Transform raw parquets into AssetData per-symbol folders
python tests/integration_tests/int_test_transform.py
```

**Per-script checks**

- `int_test_init_catalog.py` -- runs `asset_catalog_service.init_catalog.init_all`, verifies all expected catalog parquets exist, calls `monitoring_service.analyze_catalog` and asserts non-trivial counts (`stocks >= 1000`, `etfs >= 100`, etc.), then trims via `reduce_catalogs`.
- `int_test_update_catalog.py` -- runs `asset_catalog_service.update_catalog.update_all` against an already-initialised catalog. Snapshots per-file symbol sets before and after, logs added/removed symbols (first 10 of each), then runs the same `analyze_catalog` count assertions as the init test. Trims at the end.
- `int_test_historical_setup.py` -- runs `historical_data_setup.setup_historical.run_historical_setup` with `frd_dir` for stock/ETF prices and `run_monitor=True`. Verifies every `historical/<subfolder>/` exists and that the ingestion + monitoring reports were written.
- `int_test_run_daily.py` -- snapshots every pre-existing `daily/<date>/` file, runs `run_daily_pull`, then asserts (a) a new `daily/<folder-date>/` was created with the full subtree and at least one parquet per leaf, (b) the pre-existing folders are byte-for-byte unchanged, (c) the monitoring report was written for the new folder. The script writes the monitoring report itself; `setup_daily` does not. Re-reduces the catalog at the end.
- `int_test_adjust_weekly.py` -- runs `adjust_weekly` against the most recent date folder, asserts the folder and its `ingestion_report.parquet` survived the run, writes a `weekend`-mode monitoring report, and re-reduces the catalog.
- `int_test_transform.py` -- runs `data_transformation.transform.main` and asserts that every kept stock and ETF has a `data_<SYM>/` directory with at least one non-empty parquet, plus that the flat asset-type roots (`forex`, `indices`, `cryptocurrencies`, `commodities`, `economic`) each have at least one populated symbol folder.

**Known caveat.** Running `int_test_init_catalog.py` again after the catalog has been reduced will rebuild it back to full size from AV `LISTING_STATUS` (init_catalog is idempotent on existing data but writes the full universe); the script trims again at the end of every run.

### call_speedtests

Scripts that make real API calls to measure per-endpoint cost and performance characteristics. These are not pytest tests -- they are standalone scripts run manually and require a valid Alpha Vantage API key (read via `maintainance_scripts.get_api_key`).

Each script estimates the number of API calls a typical full historical pull needs for a given endpoint, which feeds into API-budget planning for the setup run.
