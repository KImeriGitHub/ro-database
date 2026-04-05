# response_schemas

Infers, stores, and validates the JSON structure of Alpha Vantage API responses. Schemas are generated from live API calls and used to catch structural changes (added/removed/renamed fields, type changes) before bad data enters the pipeline.

## Folder structure

```
response_schemas/
├── schema_inferrer.py          # Infer a schema from one or more JSON responses
├── schema_validator.py         # Validate a JSON response against a saved schema
├── __init__.py                 # Re-exports: infer_schema, validate_response, etc.
│
├── schemas/                    # Saved schema files (one .json per AV endpoint)
│   ├── TIME_SERIES_DAILY_ADJUSTED.json
│   ├── INCOME_STATEMENT.json
│   └── ...
│
├── scripts/                    # CLI scripts for bulk inference and validation
│   ├── endpoint_definitions.py # All 47 AV endpoints with params and categories
│   ├── infer_all_schemas.py    # Calls every endpoint, infers schema, saves to schemas/
│   └── validate_all_schemas.py # Calls endpoints with alt symbols, validates against saved schemas
│
└── tests/                      # Unit tests
    ├── test_schema_inferrer.py
    └── test_schema_validator.py
```

## How to use

### 1. Generate schemas from live API

Calls every endpoint, infers the response structure, and saves a `.json` file per endpoint into `schemas/`.

```bash
# All endpoints
python -m response_schemas.scripts.infer_all_schemas

# Only specific categories
python -m response_schemas.scripts.infer_all_schemas --category fundamental --category economic

# Custom delay between calls (default: 2s)
python -m response_schemas.scripts.infer_all_schemas --delay 5
```

### 2. Validate schemas with different symbols

Calls each endpoint with a different ticker/symbol/currency pair (defined as `alt_params` in `endpoint_definitions.py`) and validates the response against the saved schema. This confirms the schema holds across different assets.

```bash
python -m response_schemas.scripts.validate_all_schemas

# Same filtering options as inference
python -m response_schemas.scripts.validate_all_schemas --category fundamental
```

### 3. Use in code

```python
from response_schemas import infer_schema, save_schema, load_schema, validate_response

# Infer and save (one-time bootstrap)
schema = infer_schema(api_response)
save_schema(schema, "INCOME_STATEMENT")

# Validate (in the daily pipeline)
schema = load_schema("INCOME_STATEMENT")
violations = validate_response(new_response, schema)
if violations:
    for v in violations:
        print(v)
```

### 4. Multi-sample inference

When you have responses from multiple tickers for the same endpoint, merge them to detect optional fields:

```python
from response_schemas import infer_schema_from_samples

schema = infer_schema_from_samples([response_ibm, response_aapl, response_msft])
# Fields present in some but not all samples are marked _optional
```

## Schema format

Schemas are recursive dicts describing the JSON structure:

```json
{
  "_type": "dict",
  "children": {
    "Meta Data": {
      "_type": "dict",
      "children": {
        "1. Information": {"_type": "str"},
        "2. Symbol": {"_type": "str"}
      }
    },
    "Time Series (Daily)": {
      "_type": "dict",
      "_dynamic_keys": true,
      "children": {
        "*": {
          "_type": "dict",
          "children": {
            "1. open": {"_type": "str"},
            "2. high": {"_type": "str"}
          }
        }
      }
    }
  }
}
```

| Field            | Meaning                                                              |
|------------------|----------------------------------------------------------------------|
| `_type`          | Expected Python type: `str`, `int`, `float`, `bool`, `null`, `dict`, `list`, `mixed`, `unknown` |
| `children`       | Child key schemas (for `dict` type)                                  |
| `element`        | Element schema (for `list` type)                                     |
| `_dynamic_keys`  | `true` when dict keys are variable (dates, tickers, IDs). Children are collapsed into a `*` wildcard |
| `_optional`      | `true` when the field may be absent or null                          |
| `_types`         | List of observed types (only present when `_type` is `mixed`)        |

Dynamic key detection uses two heuristics:
1. More than half the keys match date or numeric patterns
2. All children are dicts/lists with identical structure

## Available categories

Both scripts accept `--category` to filter endpoints:

- `alpha_intelligence` -- NEWS_SENTIMENT, EARNINGS_CALL_TRANSCRIPT, INSIDER_TRANSACTIONS, INSTITUTIONAL_HOLDINGS
- `commodities` -- WTI, BRENT, NATURAL_GAS, COPPER, ALUMINUM, WHEAT, CORN, COTTON, SUGAR, COFFEE, ALL_COMMODITIES, GOLD_SILVER_SPOT, GOLD_SILVER_HISTORY
- `crypto` -- DIGITAL_CURRENCY_DAILY
- `economic` -- REAL_GDP, REAL_GDP_PER_CAPITA, TREASURY_YIELD, FEDERAL_FUNDS_RATE, CPI, INFLATION, RETAIL_SALES, DURABLES, UNEMPLOYMENT, NONFARM_PAYROLL
- `forex` -- CURRENCY_EXCHANGE_RATE, FX_INTRADAY, FX_DAILY
- `fundamental` -- OVERVIEW, ETF_PROFILE, INCOME_STATEMENT, BALANCE_SHEET, CASH_FLOW, SHARES_OUTSTANDING, EARNINGS, EARNINGS_ESTIMATES, LISTING_STATUS, EARNINGS_CALENDAR
- `index` -- INDEX_CATALOG, INDEX_DATA
- `stock_time_series` -- TIME_SERIES_INTRADAY, TIME_SERIES_DAILY_ADJUSTED, SYMBOL_SEARCH, MARKET_STATUS

## Notes

- Both scripts use the **premium** API key from `secrets/alpha_vantage_keys` (via `maintainance_scripts/get_api_key.py`).
- Default delay between calls is **2 seconds** (safe for premium plans at 75 req/min).
- **CSV-only endpoints** (LISTING_STATUS, EARNINGS_CALENDAR) are automatically skipped since schemas operate on JSON.
- **INDEX_DATA** requires a higher-tier premium subscription and may fail with a standard premium key.
- GOLD_SILVER_SPOT and GOLD_SILVER_HISTORY require a `symbol` param (`GOLD`, `XAU`, `SILVER`, or `XAG`).
- Validation flags both **missing required keys** and **unexpected keys**, which catches API additions/removals.
- AV error responses (`Error Message`, `Note`, `Information`) are detected and reported without crashing the run.
- Schemas are committed to git. Re-run inference periodically to check for API changes.
