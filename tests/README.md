# Tests

Unified test directory for the ro-database project.

## Structure

```
tests/
├── asset_catalog_service/      # Tests for asset_catalog_service
│   ├── mock_catalog/           # Temp catalog dir used by tests (created/cleaned per test)
│   ├── test_init.py            # Tests initial catalog creation (no parquets exist)
│   └── test_daily.py           # Tests daily update logic (parquets already exist)
├── historical_data_setup/      # Tests for historical_data_setup
│   ├── test_rate_limiter.py    # Sliding-window RateLimiter behavior
│   └── test_cross_endpoint.py  # Cross-endpoint concurrency + shared rate limit
├── monitoring_service/         # Tests for monitoring_service
│   ├── test_analyze_catalog.py    # catalog/*.parquet rollups
│   ├── test_analyze_ingestion.py  # ingestion_report.parquet rollups
│   ├── test_analyze_coverage.py   # SPY/MDY/EWJ/EWU/DIA/QQQ + QQQ-holdings probes
│   └── test_diff.py               # signed deltas vs previous monitoring_report.json
├── data_transformation/        # Tests for data_transformation
│   ├── test_asset_data_service.py  # AssetData dataclasses round-trip
│   ├── test_common.py              # source enumeration, sector lookup, schema cast, report
│   ├── test_dedup.py               # shared dedup-with-discrepancy-log helper
│   ├── test_overview.py            # Phase 1: assets_overview.parquet
│   ├── test_price_daily_simple.py  # Phase 2: forex/indices/crypto/commodities/economic
│   ├── test_shareprice_daily.py    # Phase 3: stocks/etfs daily + AdjClose/AdjVolume math
│   ├── test_shareprice_intraday.py # Phase 4: intraday + factor-frame join + tz strip
│   ├── test_etf_profile.py         # Phase 5: etf_profile + holdings List(Struct) round-trip
│   └── test_transform_cli.py       # End-to-end CLI via subprocess.run
├── call_speedtests/            # Scripts that measure real API call performance
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

### historical_data_setup

Unit tests covering the sliding-window rate limiter and cross-endpoint concurrency used by the historical setup pipeline. Pure asyncio tests -- no real network, no pytest-asyncio dependency (each test wraps its body with `asyncio.run`).

- `test_rate_limiter.py` -- verifies `RateLimiter` respects `calls_per_minute`, `window`, and `min_gap`; that concurrent waiters share the budget; and that the window slides forward as timestamps age out.
- `test_cross_endpoint.py` -- uses a hand-rolled mock `aiohttp.ClientSession` to confirm two endpoint coroutines interleave, never exceed the shared rate limit, and that a slow endpoint does not starve a fast one.

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

### call_speedtests

Scripts that make real API calls to measure per-endpoint cost and performance characteristics. These are not pytest tests -- they are standalone scripts run manually and require a valid Alpha Vantage API key (read via `maintainance_scripts.get_api_key`).

Each script estimates the number of API calls a typical full historical pull needs for a given endpoint, which feeds into API-budget planning for the setup run.
