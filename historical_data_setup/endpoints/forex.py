"""Download historical forex daily prices (FX_DAILY) for all currency pairs."""

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
    validate_meta_data,
)

logger = logging.getLogger(__name__)

_ASSET_TYPE = "forex"
_ENDPOINT = "forex"
_TS_KEY = "Time Series FX (Daily)"


async def fetch_forex(
    catalog_dir: Path,
    historical_dir: Path,
    api_key: str,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    issue_tracker: IssueTracker,
    asset_type: str = "forex",
) -> None:
    """Download daily FX prices for every pair in the forex catalog."""
    catalog = read_catalog_symbols(catalog_dir, _ASSET_TYPE)
    output_dir = historical_dir / _ASSET_TYPE
    output_dir.mkdir(parents=True, exist_ok=True)

    total = catalog.height
    logger.info(f"{_ENDPOINT}: {total} symbols to process")

    for idx, row in enumerate(catalog.iter_rows(named=True), 1):
        symbol = row["symbol"]  # e.g. "EURUSD"

        # Skip USDUSD (USD paired against itself)
        if symbol == "USDUSD":
            continue

        out_path = output_dir / symbol_parquet_name(_ASSET_TYPE, symbol)

        if out_path.exists():
            continue

        from_symbol = symbol[:3]
        to_symbol = symbol[3:]

        url = (
            f"{AV_BASE}/query?function=FX_DAILY"
            f"&from_symbol={from_symbol}&to_symbol={to_symbol}"
            f"&outputsize=full&apikey={api_key}"
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

        validate_meta_data(
            data, symbol, _ASSET_TYPE, _ENDPOINT, issue_tracker,
            expected_tz="UTC",
        )

        ts = data.get(_TS_KEY)
        if ts is None:
            issue_tracker.record(
                symbol, _ASSET_TYPE, _ENDPOINT,
                "structure_error", f"missing '{_TS_KEY}'",
            )
            del data
            continue

        if not ts:
            issue_tracker.record(
                symbol, _ASSET_TYPE, _ENDPOINT,
                "empty_content", "empty time series",
            )
            del data
            continue

        rows: list[dict] = []
        for date_str, ohlc in ts.items():
            if not ohlc:
                issue_tracker.record(
                    symbol, _ASSET_TYPE, _ENDPOINT,
                    "empty_content", f"empty bar at {date_str}",
                )
                continue
            try:
                rows.append({
                    "Date": date_str,
                    "Open": float(ohlc["1. open"]),
                    "High": float(ohlc["2. high"]),
                    "Low": float(ohlc["3. low"]),
                    "Close": float(ohlc["4. close"]),
                })
            except (KeyError, ValueError, TypeError) as e:
                issue_tracker.record(
                    symbol, _ASSET_TYPE, _ENDPOINT,
                    "cast_failure", f"date={date_str}: {e}",
                )

        del data, ts

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
