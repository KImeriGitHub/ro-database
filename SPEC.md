# Algo Trading Database — Implementation Specification

Implementation specification for the algo trading database. See [README.md](README.md) for project motivation, provider rationale, costs, and design decisions.

## Design tenets

The implementation must preserve these properties. See [README.md](README.md) for the motivation behind each.

- **Bias avoidance.** Minimize survivorship bias (delisted tickers covered via the optional FirstRate Data add-on) and look-ahead bias in fundamentals (daily PIT snapshot pipeline; previous values are never overwritten).
- **Alpha Vantage is the sole required provider for ongoing daily updates.** Historical setup is also AV-only by default.
- **Fundamentals PIT pipeline.** Daily run pulls every covered ticker, stores each API return in a compact cleaned form, and never overwrites previous values. The weekend pass may extend the most-recent `daily/<date>/` folder — retrying flagged cells, adding files that were missing or empty on the daily run, and refreshing `ingestion_report.parquet` and the monitoring report — but does not rewrite older dated folders.
- **FirstRate Data is an optional historical add-on.** When FRD directories are provided to `setup_historical.py`, FRD CSVs populate `prices/` and `prices_daily/` per symbol; Alpha Vantage fills every symbol/endpoint FRD does not cover. The two endpoints are independent (a symbol can use FRD for one and AV for the other). No FRD-vs-AV overlap comparison.

## Data pipeline architecture

Daily data fetching runs in a **GCP Cloud container** (e.g., Cloud Run), not on the local machine. The container executes the ingestion scripts on a schedule and writes to a single GCS bucket (whose name is taken from the `GCS_BUCKET` env var; see [GCP configuration](#gcp-configuration)), following the same folder structure described in [Data storage structure](#data-storage-structure). Raw files are append-only by default, with one narrow exception: the weekend pass may extend or rewrite content in the most-recent `daily/<date>/` folder (retried cells, refreshed ingestion report, added monitoring report). This is the permanent record.

The one-time historical setup runs **locally** (it is a multi-hour job that benefits from local disk and easy restarts), and the resulting `historical/` and `catalog/` trees are pushed to the same GCS bucket once the setup finishes. After that initial upload, the container takes over for all ongoing daily work.

A local sync script downloads data from the GCS bucket to a local mirror. This local data is then transformed into `AssetData` instances (a standardized schema defining what information each asset should contain) and processed into features for strategy research.


### API call management

**Rate limit:** Alpha Vantage's premium plan caps usage at `AV_HARD_CAP_PER_MIN` calls per minute (75). The pipeline's sliding-window limiter is configured at `AV_RATE_LIMIT_PER_MIN` (currently 70 in [config/settings.py](config/settings.py)), leaving 5 calls as a safety margin for retries and catalog-side sweeps running in parallel. Some of this budget may need to be reserved for live trading hours (8:00 AM – 5:00 PM ET), so daily batch ingestion should be scheduled outside this window when possible.

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

### Run sequence

The pipeline has a one-time bootstrap and two recurring runs:

**One-time setup (local):**

1. `python -m asset_catalog_service.init_catalog [--stocks-dir ... --etfs-dir ...]`
   builds `catalog/` from scratch (FirstRate-aware if the dirs are provided).
2. `python -m historical_data_setup.setup_historical` downloads full Alpha
   Vantage history into `historical/`. Resumable; finalises `yield_status`
   only on a clean full run.
3. Push `catalog/` and `historical/` to GCS.

**Recurring runs (Cloud Run):**

- **Daily** (`scheduled_scripts/run_daily.py`) — pulls `catalog/` from GCS,
  runs `update_catalog_all` to refresh metadata and yield_status, runs
  `run_daily_pull` with `skip_empty_yield=True`, builds the monitoring
  report, and pushes the new `daily/<date>/` folder and updated `catalog/`
  back to GCS.
- **Weekend** (`scheduled_scripts/run_weekend.py`, Saturday) — pulls
  `catalog/` and the latest `daily/<date>/` folder, runs `adjust_weekly`
  (retries the cells flagged in that folder's `ingestion_report.parquet`
  with `look_back_days` widening the truncation window for fundamentals,
  then rewrites the ingestion report and refreshes `yield_status`), builds
  the monitoring report, and pushes the extended folder and `catalog/`
  back. **Does not call `update_catalog_all`** — catalog refresh is the
  daily run's responsibility.

### Recovery and resume

The historical setup is a long-running job (tens of hours for a full Alpha Vantage pull) and is designed to be crash-tolerant. If the process dies for any reason -- OOM, network blip, unhandled exception, manual kill -- just rerun the same command.

- **File-level resume.** Each endpoint skips symbols that already have a parquet file on disk, so a restart only re-fetches what is missing. No task ledger or manual cleanup is required.
- **Stable start date across resumes.** The original run's start timestamp is preserved in `historical/.setup_started_at`, so the `data_complete_date` written to `yield_status` reflects when the setup actually began, not when it was last restarted.
- **Per-task isolation.** A failure in one `(asset_type, endpoint)` task does not abort the rest; other tasks keep running under the shared rate limiter and the failed one retries on the next run.
- **Finalize only on clean full runs.** `yield_status` is only finalized when the full setup completes with no subsetting flags, so a partial or failed run never corrupts the catalog.

See [historical_data_setup/SPEC.md](historical_data_setup/SPEC.md) for the full recovery behavior, including how to force a clean restart.

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

### GCP configuration

[`config/gcp.py`](config/gcp.py) reads every deployment-specific identifier (project id, region, bucket name, Cloud Run job name, Secret Manager secret names) from environment variables first, then falls back to a matching snake_case key in `secrets/gcs_credentials.json` -- so local dev can keep the values in one file instead of exporting them in every shell, while Cloud Run keeps using its env-var spec (the secrets file is not shipped in the container). There are intentionally **no** hard-coded defaults: the repo is public and a default would either leak a real deployment's identifiers or paper over a misconfigured one. Missing values surface as `None` at import time, and the first GCP client call that needs them fails loudly. The expected env vars (and their JSON keys) are: `GCP_PROJECT_ID` / `project_id`, `GCP_REGION` / `gcp_region`, `GCS_BUCKET` / `gcs_bucket`, `CLOUD_RUN_JOB_NAME` / `cloud_run_job_name`, `SECRET_AV_KEY_STANDARD` / `secret_av_key_standard`, `SECRET_AV_KEY_PREMIUM` / `secret_av_key_premium`, `USE_SECRET_MANAGER_FOR_AV_KEYS` / `use_secret_manager_for_av_keys`. Only the boolean `USE_SECRET_MANAGER_FOR_AV_KEYS` carries a default (`false`), so local runs without GCP configured fail loudly instead of silently reaching for a Secret Manager that does not exist. Bucket name, Cloud Run job name, and secret names are chosen by the operator; there is no built-in convention for any of them.

Authentication is **Application Default Credentials (ADC)** on every host. On Cloud Run the platform injects ADC via the bound service account. Locally, run `gcloud auth application-default login` once; the SDK writes credentials to the standard ADC location and the Google client libraries pick them up automatically (`GOOGLE_APPLICATION_CREDENTIALS` still overrides that path if set). `secrets/gcs_credentials.json` holds configuration only -- no service-account key lives on disk.

### API key resolution

`maintainance_scripts.get_api_key.get_alpha_vantage_key(tier)` tries the local `secrets/alpha_vantage_keys` file first. If the file is missing, the tier entry is absent, or the value is still a placeholder, it falls back to GCP Secret Manager **only when** `USE_SECRET_MANAGER_FOR_AV_KEYS=true`. The secret names it reads come from `SECRET_AV_KEY_STANDARD` and `SECRET_AV_KEY_PREMIUM` (see "GCP configuration" above). The container runs with the flag on; local dev keeps the default of off so runs fail loudly when the local file is misconfigured.

### Local path configuration

By default the local database trees (`catalog/`, `historical/`, `daily/`) and the transformation output live under the repo root. To put either somewhere else (e.g. a fast SSD outside the checkout), create `secrets/dir_location.txt` with one or both keys:

```
database_dir=/path/to/local/database
transformation_dir=/path/to/local/transformation
```

Same parser as `alpha_vantage_keys`: one `key=value` per line, `#` comments and blank lines ignored. `maintainance_scripts.paths.configured_database_dir()` / `configured_transformed_dir()` consume this file and are the default for `--root` in [data_transformation/transform.py](data_transformation/transform.py) and for `--local-root` in [scheduled_scripts/push_historical_to_gcs.py](scheduled_scripts/push_historical_to_gcs.py) / [scheduled_scripts/sync_gcs_to_local.py](scheduled_scripts/sync_gcs_to_local.py). A missing file or missing key falls back to `PROJECT_ROOT` (database) and `<PROJECT_ROOT>/transformed/` (transformation), so existing checkouts keep working with no extra setup.

Both `gcs_client.download_tree` and `gcs_client.upload_tree` run their per-blob work in a `ThreadPoolExecutor` so per-request latency is amortised across many small parquet files. Concurrency is controlled by `--workers` on the user-facing entrypoints ([scheduled_scripts/sync_gcs_to_local.py](scheduled_scripts/sync_gcs_to_local.py), [scheduled_scripts/push_historical_to_gcs.py](scheduled_scripts/push_historical_to_gcs.py), [scheduled_scripts/run_daily.py](scheduled_scripts/run_daily.py), [scheduled_scripts/run_weekend.py](scheduled_scripts/run_weekend.py)). Listing remains single-threaded — only the per-blob MD5 check + download and the per-file upload run in parallel.

### Health check

After a fresh deploy (new container, new project, new local machine), run `python -m maintainance_scripts.gcp_ping_test`. The script takes no flags: bucket and project id are resolved through the shared GCS client (env vars first, then `secrets/gcs_credentials.json`). It exercises the GCS bucket end-to-end (list / write / read-back / delete a throwaway blob under `_health/`) and, when secret names are configured, fetches each AV-key secret to confirm Secret Manager access. Each failure mode (missing creds, IAM denial, wrong bucket/project, secret missing or unversioned, network egress blocked) maps to a distinct error message. Logs land in `logs/<UTC>_gcp_ping_test.log` and on stdout. Enable the Cloud Scheduler triggers only after the ping logs `PING OK`.


## Folder structure

```
secrets/                      # NOT IN GIT - optional locally; container pulls from Secret Manager
├── alpha_vantage_keys        # Alpha Vantage API keys (standard= / premium=)
├── gcs_credentials.json      # GCP config: project id, bucket, secret names (local dev only; container uses env). NOT a service-account key - auth is ADC on every host.
└── dir_location.txt          # Optional local-path overrides (database_dir= / transformation_dir=)

config/
├── settings.py               # Local paths, AV rate-limit constants
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

data_transformation/          # Transforms raw data into AssetData instances

scheduled_scripts/            # Orchestration scripts for download runs and API budget tracking
maintainance_scripts/         # common py files used throughout the repo

monitoring_service/           # End-of-run summary of database state and changes
                              # (catalog, ingestion report, coverage probes, file counts,
                              # storage size, AV calls used, diff vs previous report).
                              # Auto-runs after daily / weekend / historical pulls; also
                              # invocable via `python -m monitoring_service.run_monitor`.

tests/                        # Unified test directory (one subdirectory per service plus call_speedtests/ and integration_tests/)
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
└── yield_status.parquet

historical/
├── .setup_started_at            # mtime = original start time; preserved across resumes
├── ingestion_report.parquet     # per-run issue log (overwritten each run)
├── earnings_calendar.parquet    # EARNINGS_CALENDAR (6-month horizon, one global file)
├── monitoring_report.json       # end-of-run database snapshot (machine-readable)
├── monitoring_report.md         # end-of-run database snapshot (human-readable)
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
    ├── earnings_calendar.parquet # EARNINGS_CALENDAR (6-month horizon, one global file)
    ├── monitoring_report.json   # end-of-run database snapshot (machine-readable)
    ├── monitoring_report.md     # end-of-run database snapshot (human-readable)
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
- `daily/` is append-only at the dated-folder level — each run creates or extends a `YYYY-MM-DD` folder. Only the most-recent dated folder may be extended (by the weekend pass, which can retry flagged cells, rewrite that folder's `ingestion_report.parquet`, and add the monitoring report); folders older than the most-recent are never modified.
- `historical/` is populated from Alpha Vantage by default. If FirstRate Data directories are provided to `setup_historical.py`, FRD CSVs are loaded for `prices/` and `prices_daily/` per symbol; symbols (or endpoints) not covered by FRD fall back to Alpha Vantage. The two endpoints are independent, so a symbol can use FRD for one and AV for the other. There is no FRD-vs-AV overlap comparison.
- The `catalog/` directory is the only mutable area: yield status and ticker metadata are updated as coverage changes.
- Only tickers with positive yield status (known to return data) are pulled daily. Empty/stopped tickers are re-checked weekly.

**Historical price data notes:**
Historical prices are stored in two separate subfolders under `stocks/`: `prices/` holds intraday bars from `TIME_SERIES_INTRADAY` (Open, High, Low, Close, Volume), and `prices_daily/` holds daily bars from `TIME_SERIES_DAILY_ADJUSTED` (Open, High, Low, Close, Volume, DividendAmount, SplitCoefficient). Adjusted close is not calculated or stored in either folder. If supplemented with FirstRate Data, the FirstRate bundle ships three daily variants per symbol (unadjusted, split-adjusted, split+dividend-adjusted) plus unadjusted 1-min bars. `DividendAmount` and `SplitCoefficient` are derived from the three daily variants to match the AV schema. The data source is recorded per ticker so the origin is preserved.

**Per-symbol parquet filenames:**
Per-symbol files are prefixed with their asset type so Windows reserved names (CON, PRN, AUX, NUL, COM0-9, LPT0-9) cannot collide with real tickers. The mapping is `stocks` -> `stocks_`, `etfs` -> `etfs_`, `forex` -> `forex_`, `indices` -> `indices_`, `cryptocurrencies` -> `cryptocurrencies_`, `commodities` -> `commodities_`, `economic` -> `economic_`. So `historical/etfs/prices/SPY.parquet` is actually written as `historical/etfs/prices/etfs_SPY.parquet`, fundamentals become `stocks_AAPL_annual.parquet` / `stocks_AAPL_quarterly.parquet`, and the helper `historical_data_setup._common.symbol_parquet_name(asset_type, symbol, suffix="")` is the single source of truth. The `sentiment/ALL_MESSAGES.parquet` master table and the asset-class catalog files (`stocks.parquet`, `etfs.parquet`, ...) keep their existing names; only the per-symbol files are prefixed.

## Monitoring

Every daily, weekend, and historical run ends with a monitoring pass that
snapshots the state of the database and records regressions both to disk and
to Cloud Logging. See [monitoring_service/SPEC.md](monitoring_service/SPEC.md)
for the full breakdown.

- **When it runs.** Automatically at the end of `scheduled_scripts/run_daily.py`,
  `scheduled_scripts/run_weekend.py`, and (by default) the full-run path of
  `historical_data_setup/setup_historical.py`. Can also be invoked manually:
  `python -m monitoring_service.run_monitor [--mode {daily,weekend,historical}]
  [--folder-date YYYY-MM-DD]`. The CLI defaults to `--mode daily` and the
  latest `YYYY-MM-DD` folder under `daily/`. Failures inside the monitor
  never fail the underlying run; they are logged and skipped.

- **What it checks.**
  1. **Catalog health.** Per-file symbol counts. For `stocks`, `etfs`,
     `indices`, `forex`, `cryptocurrencies`: breakdown by status (Active /
     Delisted / Corrupted). For `commodities`, `economic`: row count. For
     `yield_status`: per-endpoint True / False / Null counts and the True /
     False ratios over True+False. For `earnings_calendar` (read from the
     run's `historical/` or `daily/<date>/` folder, not from `catalog/`):
     row count, cast issue count, average days until the next reportedDate.
  2. **Ingestion report.** From the run's `ingestion_report.parquet`: total
     `timezone_mismatch` and `av_throttle` (ideally zero, warning if not);
     `structure_error`, `empty_content`, `cast_failure` totals plus a
     per-`(asset_type, endpoint)` breakdown.
  3. **Coverage probes.** Confirms SPY, MDY, EWJ, EWU, DIA, QQQ each have
     intraday and daily price parquets with the expected shape (intraday
     >= 390 rows of 1-min bars, per-OHLCV-column null ratio < 1%; daily =
     exactly one row). Reads today's QQQ ETF profile and extends the probe
     to every constituent it lists. If the QQQ profile file is not in this
     run's folder, the holdings probe is logged "skipped" and the six ETFs
     are still checked.
  4. **File counts.** Per `(asset_type, endpoint)`, parquet files written
     vs the catalog size narrowed by `yield_status` (True cells only).
     Quickly exposes a silently broken endpoint task.
  5. **Storage size.** Total bytes and file count under the analysed folder.
  6. **AV calls used.** A counter inside `fetch_av_json` reports how many
     HTTP requests this run actually issued, for trend-tracking against
     the `AV_RATE_LIMIT_PER_MIN` budget (currently 70/min). CLI invocations show `null` because a fresh
     process always sees zero.
  7. **Delta vs previous report.** Signed deltas of catalog status counts,
     yield True/False counts, ingestion issue totals, and coverage totals
     against the previous `monitoring_report.json` (downloaded from GCS).
     `catalog/` and `yield_status.parquet` are overwritten in place each
     run, so the JSON snapshot is the only durable prior state available.

- **Output.** `monitoring_report.json` (machine-readable) and
  `monitoring_report.md` (human summary) land alongside `ingestion_report.parquet`
  (in `daily/<YYYY-MM-DD>/` for daily/weekend runs, `historical/` for setup)
  and are uploaded to GCS by the same push step that ships the ingestion
  report. The summary is also written to stdout, so it shows up verbatim
  in Cloud Logging.

- **Dashboards and alerts.** Headline counts are emitted as structured log
  fields (`jsonPayload.monitor.catalog.stocks.active`, `monitor.ingestion.av_throttle`,
  `monitor.coverage.intraday_ok`, `monitor.api_calls.total`, etc.), so Cloud
  Logging log-based metrics or Cloud Monitoring custom metrics can chart
  them and alert on thresholds. Google Analytics is **not** used here: GA
  is a web/app analytics product and does not fit pipeline telemetry. The
  right GCP fit is Cloud Logging plus optional Cloud Monitoring custom
  metrics (or log-based metrics) on the structured fields above.

