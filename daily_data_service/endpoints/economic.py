"""Daily pull of economic indicator data.

Daily-interval indicators (TREASURY_YIELD_*, FEDERAL_FUNDS_RATE) truncate to
``(previous_date, folder_date]``. All others truncate to
``Date >= folder_date - 1 year``.
"""

import logging
from datetime import date
from pathlib import Path
from urllib.parse import urlencode

import aiohttp
import polars as pl

from historical_data_setup._common import (
    AV_BASE,
    AVResponseError,
    IssueTracker,
    RateLimiter,
    fetch_av_json,
    read_catalog_symbols,
    symbol_parquet_name,
)
from daily_data_service._common import since_expr, window_expr, years_before

logger = logging.getLogger(__name__)

_NULL_SENTINELS = {None, "None", "", "."}
_ASSET_TYPE = "economic"
_ENDPOINT = "economic"

_INDICATOR_CONFIG: dict[str, dict] = {
    "REAL_GDP":            {"function": "REAL_GDP",          "params": {"interval": "quarterly"}},
    "REAL_GDP_PER_CAPITA": {"function": "REAL_GDP_PER_CAPITA", "params": {}},
    "TREASURY_YIELD_30Y":  {"function": "TREASURY_YIELD",   "params": {"interval": "daily", "maturity": "30year"}},
    "TREASURY_YIELD_10Y":  {"function": "TREASURY_YIELD",   "params": {"interval": "daily", "maturity": "10year"}},
    "TREASURY_YIELD_7Y":   {"function": "TREASURY_YIELD",   "params": {"interval": "daily", "maturity": "7year"}},
    "TREASURY_YIELD_5Y":   {"function": "TREASURY_YIELD",   "params": {"interval": "daily", "maturity": "5year"}},
    "TREASURY_YIELD_2Y":   {"function": "TREASURY_YIELD",   "params": {"interval": "daily", "maturity": "2year"}},
    "TREASURY_YIELD_3M":   {"function": "TREASURY_YIELD",   "params": {"interval": "daily", "maturity": "3month"}},
    "FEDERAL_FUNDS_RATE":  {"function": "FEDERAL_FUNDS_RATE", "params": {"interval": "daily"}},
    "CPI":                 {"function": "CPI",              "params": {"interval": "monthly"}},
    "INFLATION":           {"function": "INFLATION",        "params": {}},
    "RETAIL_SALES":        {"function": "RETAIL_SALES",     "params": {}},
    "DURABLES":            {"function": "DURABLES",         "params": {}},
    "UNEMPLOYMENT":        {"function": "UNEMPLOYMENT",     "params": {}},
    "NONFARM_PAYROLL":     {"function": "NONFARM_PAYROLL",  "params": {}},
}

_EXPECTED_KEYS = {"name", "interval", "unit", "data"}


def _is_daily_interval(symbol: str) -> bool:
    cfg = _INDICATOR_CONFIG.get(symbol, {})
    return cfg.get("params", {}).get("interval") == "daily"


async def fetch_economic(
    catalog_dir: Path,
    daily_dir: Path,
    api_key: str,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    issue_tracker: IssueTracker,
    asset_type: str,
    folder_date: date,
    previous_date: date,
    symbols_filter: set[str] | None = None,
) -> None:
    catalog = read_catalog_symbols(catalog_dir, _ASSET_TYPE)
    if symbols_filter is not None:
        catalog = catalog.filter(pl.col("symbol").is_in(list(symbols_filter)))
    output_dir = daily_dir / _ASSET_TYPE
    output_dir.mkdir(parents=True, exist_ok=True)

    daily_window = window_expr("Date", previous_date, folder_date)
    one_year_cutoff = years_before(folder_date, 1)
    yearly_since = since_expr("Date", one_year_cutoff)

    total = catalog.height
    logger.info(f"{_ENDPOINT}: {total} indicators to process")

    for idx, row in enumerate(catalog.iter_rows(named=True), 1):
        symbol = row["symbol"]
        out_path = output_dir / symbol_parquet_name(_ASSET_TYPE, symbol)

        if out_path.exists():
            continue

        config = _INDICATOR_CONFIG.get(symbol)
        if config is None:
            issue_tracker.record(
                symbol, _ASSET_TYPE, _ENDPOINT,
                "structure_error", f"unknown economic indicator: {symbol}",
            )
            continue

        query = {"function": config["function"], "apikey": api_key}
        query.update(config["params"])
        url = f"{AV_BASE}/query?{urlencode(query)}"

        try:
            data = await fetch_av_json(url, session, rate_limiter)
        except AVResponseError as e:
            issue_tracker.record(symbol, _ASSET_TYPE, _ENDPOINT, "av_throttle", str(e))
            continue
        except Exception as e:
            issue_tracker.record(
                symbol, _ASSET_TYPE, _ENDPOINT,
                "structure_error", f"fetch failed: {e}",
            )
            continue

        missing = _EXPECTED_KEYS - data.keys()
        if missing:
            issue_tracker.record(
                symbol, _ASSET_TYPE, _ENDPOINT,
                "structure_error", f"missing top-level keys: {missing}",
            )
            del data
            continue

        records = data["data"]
        del data

        if not records:
            issue_tracker.record(
                symbol, _ASSET_TYPE, _ENDPOINT,
                "empty_content", "empty data list",
            )
            continue

        rows: list[dict] = []
        for entry in records:
            raw_val = entry.get("value")
            try:
                val = None if raw_val in _NULL_SENTINELS else float(raw_val)
                rows.append({
                    "Date": entry["date"],
                    "value": val,
                })
            except (ValueError, TypeError) as e:
                issue_tracker.record(
                    symbol, _ASSET_TYPE, _ENDPOINT,
                    "cast_failure", f"date={entry.get('date')}: {e}",
                )

        del records

        if not rows:
            continue

        if _is_daily_interval(symbol):
            filter_expr = daily_window
            label = f"in ({previous_date}, {folder_date}]"
        else:
            filter_expr = yearly_since
            label = f">= {one_year_cutoff}"

        df = (
            pl.DataFrame(rows)
            .with_columns(pl.col("Date").str.to_date("%Y-%m-%d"))
            .cast({"value": pl.Float32})
            .filter(filter_expr)
            .sort("Date")
        )
        del rows

        df.write_parquet(out_path, compression="zstd")
        if df.height == 0:
            logger.info(f"  {_ENDPOINT}: {symbol} saved empty frame (no rows {label})")
        else:
            logger.info(f"  {_ENDPOINT}: {symbol} saved {df.height} rows")
        del df
