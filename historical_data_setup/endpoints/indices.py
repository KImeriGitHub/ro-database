"""Download historical index data (INDEX_DATA) for all indices in the catalog."""

import logging
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
    symbol_parquet_name,
)

logger = logging.getLogger(__name__)

_NULL_SENTINELS = {None, "None", "", "."}
_ASSET_TYPE = "indices"
_ENDPOINT = "indices"


async def fetch_indices(
    catalog_dir: Path,
    historical_dir: Path,
    api_key: str,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    issue_tracker: IssueTracker,
    asset_type: str = "indices",
) -> None:
    """Download daily index prices for every symbol in the indices catalog."""
    catalog = read_catalog_symbols(catalog_dir, _ASSET_TYPE)
    output_dir = historical_dir / _ASSET_TYPE
    output_dir.mkdir(parents=True, exist_ok=True)

    total = catalog.height
    logger.info(f"{_ENDPOINT}: {total} symbols to process")

    for idx, row in enumerate(catalog.iter_rows(named=True), 1):
        symbol = row["symbol"]
        out_path = output_dir / symbol_parquet_name(_ASSET_TYPE, symbol)

        if out_path.exists():
            continue

        url = (
            f"{AV_BASE}/query?function=INDEX_DATA"
            f"&symbol={symbol}&interval=daily&apikey={api_key}"
        )

        try:
            data = await fetch_av_json(url, session, rate_limiter)
        except AVResponseError as e:
            issue_tracker.record(
                symbol, _ASSET_TYPE, _ENDPOINT,
                "av_throttle", str(e),
            )
            continue
        except Exception as e:
            issue_tracker.record(
                symbol, _ASSET_TYPE, _ENDPOINT,
                "structure_error", f"fetch failed: {e}",
            )
            continue

        if "data" not in data:
            issue_tracker.record(
                symbol, _ASSET_TYPE, _ENDPOINT,
                "structure_error", "missing 'data' key",
            )
            del data
            continue

        records = data["data"]

        if not records:
            issue_tracker.record(
                symbol, _ASSET_TYPE, _ENDPOINT,
                "empty_content", "empty data list",
            )
            del data
            continue

        rows: list[dict] = []
        for entry in records:
            try:
                raw_o = entry.get("open")
                raw_h = entry.get("high")
                raw_l = entry.get("low")
                raw_c = entry.get("close")
                rows.append({
                    "Date": entry["date"],
                    "Open": None if raw_o in _NULL_SENTINELS else float(raw_o),
                    "High": None if raw_h in _NULL_SENTINELS else float(raw_h),
                    "Low": None if raw_l in _NULL_SENTINELS else float(raw_l),
                    "Close": None if raw_c in _NULL_SENTINELS else float(raw_c),
                })
            except (KeyError, ValueError, TypeError) as e:
                issue_tracker.record(
                    symbol, _ASSET_TYPE, _ENDPOINT,
                    "cast_failure", f"date={entry.get('date')}: {e}",
                )

        del data, records

        if rows:
            df = (
                pl.DataFrame(rows)
                .with_columns(
                    pl.col("Date").str.to_date("%Y-%m-%d"),
                )
                .cast({
                    "Open": pl.Float32,
                    "High": pl.Float32,
                    "Low": pl.Float32,
                    "Close": pl.Float32,
                })
                .sort("Date")
            )
            df.write_parquet(out_path, compression="zstd")
            logger.info(f"  {_ENDPOINT}: {symbol} saved {df.height} rows")
            del df

        del rows
