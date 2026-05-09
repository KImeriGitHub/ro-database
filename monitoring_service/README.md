# Monitoring Service

End-of-run summary of database state and changes. Runs automatically at the
end of every daily, weekend, and historical pull, and can also be invoked on
demand from the CLI. Produces both a human-readable Markdown summary and a
machine-readable JSON file alongside the run's `ingestion_report.parquet`.

## When it runs

| Trigger | Folder it analyses | Previous report it diffs against |
|---|---|---|
| `scheduled_scripts/run_daily.py` | `daily/<folder_date>/` | `daily/<previous folder_date>/monitoring_report.json` (pulled from GCS) |
| `scheduled_scripts/run_weekend.py` | `daily/<folder_date>/` | the daily monitoring_report from the same folder, downloaded as `monitoring_report.previous.json` before `adjust_weekly` runs |
| `historical_data_setup/setup_historical.py` (full run, default) | `historical/` | (none) |
| `python -m monitoring_service.run_monitor` | configurable, defaults to the latest `daily/<date>/` | `--previous-report PATH` if supplied |

Failures inside the monitor never abort the run; they are logged with
`logger.exception` and the orchestration step that triggered them carries on.

## What it checks

### Catalog (`monitoring_service/analyze_catalog.py`)

Per file under `catalog/`:

- `stocks.parquet`, `etfs.parquet`, `indices.parquet`, `forex.parquet`,
  `cryptocurrencies.parquet`: total row count plus the number with status
  `Active`, `Delisted`, and `Corrupted` (case-insensitive). Any row whose
  status doesn't fall in those buckets is reported as `other_status`.
- `commodities.parquet`, `economic.parquet`: total row count.
- `yield_status.parquet`: per-endpoint True / False / Null counts plus the
  ratios `true / (true + false)` and `false / (true + false)`. Null cells
  (the inapplicable pairs) are reported but excluded from the ratios.
- `earnings_calendar.parquet`: total row count, `cast_issues` row count, and
  the average days between `today` and the next `reportedDate`. Read from
  the run's own `folder_dir` (`historical/` or `daily/<date>/`), not from
  `catalog/` -- the file moved with the historical/daily pull. When the
  monitoring caller passes no `folder_dir`, this entry is reported missing.

### Ingestion report (`monitoring_service/analyze_ingestion.py`)

From the run's `ingestion_report.parquet`:

- `timezone_mismatch`, `av_throttle`: flat totals, ideally zero (warning
  log emitted otherwise).
- `structure_error`, `empty_content`, `cast_failure`: total plus a
  per-`(asset_type, endpoint)` breakdown so a single broken endpoint stands
  out.

### Coverage probes (`monitoring_service/analyze_coverage.py`)

For SPY, MDY, EWJ, EWU, DIA, QQQ and (when the QQQ ETF profile is present
in the run's folder) every constituent listed in its `holdings`:

- Intraday parquet must exist with at least
  `INTRADAY_MIN_ROWS = 390` rows (a full regular-session day of 1-min bars)
  and a per-OHLCV-column null ratio strictly below 1%.
- Daily parquet must exist with exactly one row.
- The QQQ profile is consulted only at this run's path; if missing the
  probe records `qqq_profile_status="missing"` and continues with the
  six ETFs only.

The maximum `Date` of each parquet is recorded so a frozen feed (file with
the right shape but stale data) shows up in the report.

### File-count sanity (`monitoring_service/analyze_files.py`)

Per `(asset_type, endpoint)` in the run's folder, count the parquet files
written. Expected count is the catalog size, narrowed by `yield_status`
(True cells only) for endpoints with a per-symbol yield column. The ratio
`written / expected` makes a silently broken endpoint task obvious.
Fundamental endpoints (two files per symbol) count distinct symbols.

### Storage size (`monitoring_service/analyze_files.py:analyze_storage`)

Total bytes and file count under the analysed folder. Useful as a smoke
signal that something was written and as a baseline for GCS cost tracking.

### API call counter

`historical_data_setup/_common.py` keeps a module-level counter that
increments inside `fetch_av_json` once per HTTP request issued (including
retries). Orchestrators reset it at the start of a run and pass the final
value to the monitor. Reported as `api_calls.total_calls_made` and useful
for trend-tracking against the `AV_RATE_LIMIT_PER_MIN` budget (currently
70/min, see [config/settings.py](../config/settings.py)). CLI invocations
show `null` because a fresh process always sees zero.

### Delta vs previous report (`monitoring_service/diff.py`)

The previous monitoring report (when the orchestrator hands one over)
contributes signed deltas for catalog status counts, yield True/False
counts, ingestion issue totals, and coverage ok/total. We do not retain
the previous `catalog/` or `yield_status.parquet` directly: those files are
overwritten in place each run, so the JSON snapshot is the only durable
prior state we have to compare against.

## Output

Two files are written into the analysed folder:

- `monitoring_report.json`: full machine-readable structure.
- `monitoring_report.md`: condensed human summary (rendered from the JSON).

Both are also uploaded to GCS by the existing `_push_*` helpers in
`scheduled_scripts/run_daily.py` and `scheduled_scripts/run_weekend.py`,
landing alongside the day's `ingestion_report.parquet`. The same summary is
printed via `logger.info` so it appears in Cloud Logging on the container,
with headline numbers attached as `extra={...}` fields (queryable in the
Logs Explorer as `jsonPayload.monitor.<field>`).

Why not Google Analytics: GA is for web/app user-behaviour tracking and is
not appropriate for pipeline telemetry. The right GCP fit is Cloud Logging
plus optional Cloud Monitoring custom metrics (or log-based metrics) on
those structured fields.

## CLI

```bash
# Default: mode=daily, folder_date=latest under <project>/daily/
python -m monitoring_service.run_monitor

# Specific date
python -m monitoring_service.run_monitor --folder-date 2026-04-23

# Weekend mode against the same daily folder
python -m monitoring_service.run_monitor --mode weekend --folder-date 2026-04-23

# Historical post-hoc (writes monitoring_report.json into historical/)
python -m monitoring_service.run_monitor --mode historical

# Diff against a known-good previous report
python -m monitoring_service.run_monitor \
    --previous-report ./daily/2026-04-22/monitoring_report.json
```

## Module structure

```
monitoring_service/
├── __init__.py           # public entry points
├── analyze_catalog.py    # catalog/*.parquet rollups
├── analyze_ingestion.py  # ingestion_report.parquet rollups
├── analyze_coverage.py   # SPY/MDY/EWJ/EWU/DIA/QQQ + QQQ-holdings probes
├── analyze_files.py      # file-count sanity + storage size
├── diff.py               # signed deltas vs previous monitoring_report.json
├── report.py             # assembler, Markdown renderer, log + write helpers
└── run_monitor.py        # CLI wrapper
```

Tests live in `tests/monitoring_service/`.
