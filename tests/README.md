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

### call_speedtests

Scripts that make real API calls to measure per-endpoint cost and performance characteristics. These are not pytest tests -- they are standalone scripts run manually and require a valid Alpha Vantage API key (read via `maintainance_scripts.get_api_key`).

Each script estimates the number of API calls a typical full historical pull needs for a given endpoint, which feeds into API-budget planning for the setup run.
