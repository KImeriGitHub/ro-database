"""Daily pull of commodities data.

Daily-interval group (WTI, BRENT, NATURAL_GAS, XAU, XAG) truncates to
``(previous_date, folder_date]``. Monthly-interval group (COPPER, ALUMINUM,
WHEAT, CORN, COTTON, SUGAR, COFFEE, ALL_COMMODITIES) truncates to
``Date >= folder_date - 1 year``.
"""

import logging
from datetime import date
from pathlib import Path

import aiohttp
import polars as pl

from historical_data_setup._common import (
    AV_BASE,
    AVResponseError,
    IssueTracker,
    RateLimiter,
    fetch_av_json,
    read_catalog_symbols,
)
from daily_data_service._common import since_expr, window_expr, years_before

logger = logging.getLogger(__name__)

_NULL_SENTINELS = {None, "None", "", "."}
_ASSET_TYPE = "commodities"
_ENDPOINT = "commodities"

_DAILY_SYMBOLS = {"WTI", "BRENT", "NATURAL_GAS"}

_MONTHLY_SYMBOLS = {
    "COPPER", "ALUMINUM", "WHEAT", "CORN",
    "COTTON", "SUGAR", "COFFEE", "ALL_COMMODITIES",
}

_GOLD_SILVER_MAP = {
    "XAU": "GOLD",
    "XAG": "SILVER",
}


def _finalize(
    rows: list[dict],
    out_path: Path,
    symbol: str,
    filter_expr,
    issue_tracker: IssueTracker,
    label: str,
) -> None:
    if not rows:
        return
    df = (
        pl.DataFrame(rows)
        .with_columns(pl.col("Date").str.to_date("%Y-%m-%d"))
        .cast({"value": pl.Float32})
        .filter(filter_expr)
        .sort("Date")
    )
    if df.height == 0:
        issue_tracker.record(
            symbol, _ASSET_TYPE, _ENDPOINT,
            "empty_content", f"no rows {label} after truncation",
        )
        del df
        return
    df.write_parquet(out_path, compression="zstd")
    logger.info(f"  {symbol}: saved {df.height} rows")
    del df


async def _fetch_standard(
    symbol: str,
    interval: str,
    api_key: str,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    issue_tracker: IssueTracker,
    out_path: Path,
    filter_expr,
    label: str,
) -> None:
    url = (
        f"{AV_BASE}/query?function={symbol}"
        f"&interval={interval}&apikey={api_key}"
    )

    try:
        data = await fetch_av_json(url, session, rate_limiter)
    except AVResponseError as e:
        issue_tracker.record(symbol, _ASSET_TYPE, _ENDPOINT, "av_throttle", str(e))
        return
    except Exception as e:
        issue_tracker.record(
            symbol, _ASSET_TYPE, _ENDPOINT,
            "structure_error", f"fetch failed: {e}",
        )
        return

    if "data" not in data:
        issue_tracker.record(
            symbol, _ASSET_TYPE, _ENDPOINT,
            "structure_error", "missing 'data' key",
        )
        del data
        return

    records = data["data"]
    unit = data.get("unit", "")

    if not records:
        issue_tracker.record(
            symbol, _ASSET_TYPE, _ENDPOINT,
            "empty_content", "empty data list",
        )
        del data
        return

    rows: list[dict] = []
    for entry in records:
        raw_val = entry.get("value")
        try:
            val = None if raw_val in _NULL_SENTINELS else float(raw_val)
            rows.append({
                "Date": entry["date"],
                "value": val,
                "unit": unit,
            })
        except (ValueError, TypeError) as e:
            issue_tracker.record(
                symbol, _ASSET_TYPE, _ENDPOINT,
                "cast_failure", f"date={entry.get('date')}: {e}",
            )

    del data, records
    _finalize(rows, out_path, symbol, filter_expr, issue_tracker, label)


async def _fetch_gold_silver(
    symbol: str,
    av_symbol: str,
    api_key: str,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    issue_tracker: IssueTracker,
    out_path: Path,
    filter_expr,
    label: str,
) -> None:
    url = (
        f"{AV_BASE}/query?function=GOLD_SILVER_HISTORY"
        f"&symbol={av_symbol}&interval=daily&apikey={api_key}"
    )

    try:
        data = await fetch_av_json(url, session, rate_limiter)
    except AVResponseError as e:
        issue_tracker.record(symbol, _ASSET_TYPE, _ENDPOINT, "av_throttle", str(e))
        return
    except Exception as e:
        issue_tracker.record(
            symbol, _ASSET_TYPE, _ENDPOINT,
            "structure_error", f"fetch failed: {e}",
        )
        return

    if "data" not in data:
        issue_tracker.record(
            symbol, _ASSET_TYPE, _ENDPOINT,
            "structure_error", "missing 'data' key",
        )
        del data
        return

    records = data["data"]

    if not records:
        issue_tracker.record(
            symbol, _ASSET_TYPE, _ENDPOINT,
            "empty_content", "empty data list",
        )
        del data
        return

    rows: list[dict] = []
    for entry in records:
        raw_val = entry.get("price")
        try:
            val = None if raw_val in _NULL_SENTINELS else float(raw_val)
            rows.append({
                "Date": entry["date"],
                "value": val,
                "unit": "dollars per troy ounce",
            })
        except (ValueError, TypeError) as e:
            issue_tracker.record(
                symbol, _ASSET_TYPE, _ENDPOINT,
                "cast_failure", f"date={entry.get('date')}: {e}",
            )

    del data, records
    _finalize(rows, out_path, symbol, filter_expr, issue_tracker, label)


async def fetch_commodities(
    catalog_dir: Path,
    daily_dir: Path,
    api_key: str,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    issue_tracker: IssueTracker,
    asset_type: str,
    folder_date: date,
    previous_date: date,
) -> None:
    catalog = read_catalog_symbols(catalog_dir, _ASSET_TYPE)
    output_dir = daily_dir / _ASSET_TYPE
    output_dir.mkdir(parents=True, exist_ok=True)

    daily_window = window_expr("Date", previous_date, folder_date)
    monthly_since = since_expr("Date", years_before(folder_date, 1))

    total = catalog.height
    logger.info(f"{_ENDPOINT}: {total} symbols to process")

    for idx, row in enumerate(catalog.iter_rows(named=True), 1):
        symbol = row["symbol"]
        out_path = output_dir / f"{symbol}.parquet"

        if out_path.exists():
            continue

        logger.info(f"[{idx}/{total}] {symbol}")

        if symbol in _GOLD_SILVER_MAP:
            await _fetch_gold_silver(
                symbol, _GOLD_SILVER_MAP[symbol],
                api_key, session, rate_limiter, issue_tracker,
                out_path, daily_window,
                f"in ({previous_date}, {folder_date}]",
            )
        elif symbol in _DAILY_SYMBOLS:
            await _fetch_standard(
                symbol, "daily",
                api_key, session, rate_limiter, issue_tracker,
                out_path, daily_window,
                f"in ({previous_date}, {folder_date}]",
            )
        elif symbol in _MONTHLY_SYMBOLS:
            await _fetch_standard(
                symbol, "monthly",
                api_key, session, rate_limiter, issue_tracker,
                out_path, monthly_since,
                f">= {years_before(folder_date, 1)}",
            )
        else:
            issue_tracker.record(
                symbol, _ASSET_TYPE, _ENDPOINT,
                "structure_error", f"unknown commodity symbol: {symbol}",
            )
