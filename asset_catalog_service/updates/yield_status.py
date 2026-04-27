"""yield_status.parquet lifecycle.

- ``update_yield_status``: init-only (no-op if file exists). Populates all
  yield columns with null.
- ``finalize_yield_status``: overwrite after a full historical setup run.
  Applicable cells become True by default and are flipped to False based
  on the ingestion report.
"""

import logging
from datetime import date, datetime, time, timedelta
from pathlib import Path

import polars as pl

from asset_catalog_service.updates._common import YIELD_ENDPOINTS
from historical_data_setup._common import symbol_parquet_name

logger = logging.getLogger(__name__)

CATALOG_FILES = [
    "stocks.parquet",
    "etfs.parquet",
    "forex.parquet",
    "indices.parquet",
    "cryptocurrencies.parquet",
    "commodities.parquet",
    "economic.parquet",
]

ASSET_TYPE_COLUMNS: dict[str, tuple[str, ...]] = {
    "stocks": (
        "prices", "prices_daily", "income_statement", "balance_sheet",
        "cash_flow", "earnings", "earnings_estimates", "insider", "sentiment",
    ),
    "etfs": ("prices", "prices_daily", "etf_profile"),
    "forex": ("direct",),
    "indices": ("direct",),
    "cryptocurrencies": ("direct",),
    "commodities": ("direct",),
    "economic": ("direct",),
}

# Fundamental endpoints split into annual/quarterly files; partial save is OK.
_FUNDAMENTAL_ENDPOINTS = {
    "income_statement", "balance_sheet", "cash_flow",
    "earnings", "earnings_estimates",
}

# Issue types that unambiguously mean no usable data was saved.
_HARD_FAIL_ISSUES = {"structure_error", "av_throttle"}


def update_yield_status(catalog_dir: Path) -> None:
    path = catalog_dir / "yield_status.parquet"
    if path.exists():
        logger.info("yield_status.parquet exists, no changes needed")
        return

    all_symbols = []
    for fname in CATALOG_FILES:
        fpath = catalog_dir / fname
        if not fpath.exists():
            logger.warning(f"Cannot include {fname} in yield_status: file not found")
            continue
        cat = pl.read_parquet(fpath)
        all_symbols.extend(cat["symbol"].to_list())

    if not all_symbols:
        logger.warning("Cannot init yield_status: no catalog files found")
        return

    today = date.today()

    data: dict = {"symbol": all_symbols}
    for ep in YIELD_ENDPOINTS:
        data[ep] = [None] * len(all_symbols)
    data["date"] = [today] * len(all_symbols)

    schema: dict = {"symbol": pl.Utf8}
    for ep in YIELD_ENDPOINTS:
        schema[ep] = pl.Boolean
    schema["date"] = pl.Date

    df = pl.DataFrame(data, schema=schema)
    df.write_parquet(path, compression="zstd")
    logger.info(
        f"Established yield_status.parquet "
        f"({df.height} rows, {len(YIELD_ENDPOINTS)} endpoints)"
    )


def _data_complete_date(started_at_et: datetime) -> date:
    """Last fully-traded ET date at *started_at_et*.

    Weekend -> start date (no trading expected).
    Weekday and time >= 20:00 ET -> start date (after-hours done).
    Weekday and time < 20:00 ET -> start date minus one day.
    """
    start_date = started_at_et.date()
    if started_at_et.weekday() >= 5:
        return start_date
    if started_at_et.time() >= time(20, 0):
        return start_date
    return start_date - timedelta(days=1)


def _collect_catalog(catalog_dir: Path) -> list[tuple[str, str]]:
    """Return a list of (symbol, asset_type) across all catalog files."""
    pairs: list[tuple[str, str]] = []
    for fname in CATALOG_FILES:
        fpath = catalog_dir / fname
        if not fpath.exists():
            logger.warning(f"Cannot include {fname} in yield_status: file not found")
            continue
        asset_type = fpath.stem
        symbols = pl.read_parquet(fpath)["symbol"].to_list()
        pairs.extend((s, asset_type) for s in symbols)
    return pairs


def _load_issue_index(
    historical_dir: Path,
) -> dict[tuple[str, str], set[str]]:
    """Return {(symbol, report_endpoint): {issue_types}} from ingestion_report."""
    path = historical_dir / "ingestion_report.parquet"
    if not path.exists():
        logger.info("No ingestion_report.parquet found; all applicable yields default True")
        return {}

    report = pl.read_parquet(path)
    index: dict[tuple[str, str], set[str]] = {}
    for row in report.iter_rows(named=True):
        key = (row["symbol"], row["endpoint"])
        index.setdefault(key, set()).add(row["issue_type"])
    return index


def _fundamental_files_exist(
    historical_dir: Path, symbol: str, endpoint: str
) -> bool:
    ep_dir = historical_dir / "stocks" / endpoint
    return (
        (ep_dir / symbol_parquet_name("stocks", symbol, "_annual")).exists()
        or (ep_dir / symbol_parquet_name("stocks", symbol, "_quarterly")).exists()
    )


def _resolve_cell(
    symbol: str,
    column: str,
    asset_type: str,
    issue_index: dict[tuple[str, str], set[str]],
    historical_dir: Path,
) -> bool:
    report_ep = asset_type if column == "direct" else column
    issues = issue_index.get((symbol, report_ep), set())

    if issues & _HARD_FAIL_ISSUES:
        return False
    if "empty_content" in issues:
        if column in _FUNDAMENTAL_ENDPOINTS:
            return _fundamental_files_exist(historical_dir, symbol, column)
        return False
    return True


def finalize_yield_status(
    catalog_dir: Path,
    historical_dir: Path,
    started_at: datetime,
) -> None:
    """Overwrite ``catalog_dir/yield_status.parquet`` from the ingestion report.

    For each (symbol, applicable column), True unless the ingestion report
    shows ``structure_error``, ``av_throttle``, or ``empty_content`` (the
    latter with a partial-save exemption for fundamental endpoints).
    """
    pairs = _collect_catalog(catalog_dir)
    if not pairs:
        logger.warning("Cannot finalize yield_status: no catalog files found")
        return

    issue_index = _load_issue_index(historical_dir)
    data_date = _data_complete_date(started_at)

    columns: dict[str, list] = {"symbol": []}
    for ep in YIELD_ENDPOINTS:
        columns[ep] = []
    columns["date"] = []

    for symbol, asset_type in pairs:
        applicable = ASSET_TYPE_COLUMNS.get(asset_type, ())
        columns["symbol"].append(symbol)
        for ep in YIELD_ENDPOINTS:
            if ep in applicable:
                columns[ep].append(_resolve_cell(
                    symbol, ep, asset_type, issue_index, historical_dir,
                ))
            else:
                columns[ep].append(None)
        columns["date"].append(data_date)

    schema: dict = {"symbol": pl.Utf8}
    for ep in YIELD_ENDPOINTS:
        schema[ep] = pl.Boolean
    schema["date"] = pl.Date

    df = pl.DataFrame(columns, schema=schema)
    out_path = catalog_dir / "yield_status.parquet"
    df.write_parquet(out_path, compression="zstd")
    logger.info(
        f"Finalized yield_status.parquet at {out_path} "
        f"({df.height} rows, data_complete_date={data_date})"
    )
