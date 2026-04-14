# Tests

Unified test directory for the ro-database project.

## Structure

```
tests/
├── asset_catalog_service/     # Tests for asset_catalog_service
│   ├── mock_catalog/          # Temp catalog dir used by tests (created/cleaned per test)
│   ├── test_init.py           # Tests initial catalog creation (no parquets exist)
│   └── test_daily.py          # Tests daily update logic (parquets already exist)
├── historical_data_setup/     # Tests for historical_data_setup (placeholder)
├── call_speedtests/           # Scripts that measure real API call performance
│   └── estimate_sentiment_calls.py  # Estimates NEWS_SENTIMENT backward pagination cost
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

Placeholder for future tests covering the historical data download pipeline.

### call_speedtests

Scripts that make real API calls to measure performance characteristics. These are not pytest tests -- they are standalone scripts run manually.

- `estimate_sentiment_calls.py` -- estimates how many API calls the NEWS_SENTIMENT backward pagination needs to reach 2010-01-01. Requires a valid Alpha Vantage API key.
