"""Daily pull of intraday 1-min prices (TIME_SERIES_INTRADAY) for stocks/ETFs.

Fetches a single response with no ``month`` parameter (AV returns the trailing
30 days when ``outputsize=full`` and no month is specified), then truncates
client-side to ``(previous_date, folder_date]``.
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
    validate_meta_data,
)

logger = logging.getLogger(__name__)

_TS_KEY = "Time Series (1min)"


async def fetch_intraday_prices(
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
    """Download truncated intraday prices for all symbols of the given asset type."""
    catalog = read_catalog_symbols(catalog_dir, asset_type)
    output_dir = daily_dir / asset_type / "prices"
    output_dir.mkdir(parents=True, exist_ok=True)

    total = catalog.height
    logger.info(f"prices ({asset_type}): {total} symbols to process")

    for idx, row in enumerate(catalog.iter_rows(named=True), 1):
        symbol = row["symbol"]
        out_path = output_dir / f"{symbol}.parquet"

        if out_path.exists():
            continue

        logger.info(f"[{idx}/{total}] {symbol}")

        url = (
            f"{AV_BASE}/query?function=TIME_SERIES_INTRADAY"
            f"&symbol={symbol}&interval=1min"
            f"&adjusted=false&outputsize=full&apikey={api_key}"
        )

        try:
            data = await fetch_av_json(url, session, rate_limiter)
        except AVResponseError as e:
            issue_tracker.record(symbol, asset_type, "prices", "av_throttle", str(e))
            continue
        except Exception as e:
            issue_tracker.record(
                symbol, asset_type, "prices",
                "structure_error", f"fetch failed: {e}",
            )
            continue

        validate_meta_data(data, symbol, asset_type, "prices", issue_tracker)

        ts = data.get(_TS_KEY)
        if ts is None:
            issue_tracker.record(
                symbol, asset_type, "prices",
                "structure_error", f"missing '{_TS_KEY}'",
            )
            del data
            continue

        if not ts:
            issue_tracker.record(
                symbol, asset_type, "prices",
                "empty_content", "empty time series",
            )
            del data
            continue

        rows: list[dict] = []
        for dt_str, ohlcv in ts.items():
            if not ohlcv:
                issue_tracker.record(
                    symbol, asset_type, "prices",
                    "empty_content", f"empty bar at {dt_str}",
                )
                continue
            try:
                rows.append({
                    "Date": dt_str,
                    "Open": float(ohlcv["1. open"]),
                    "High": float(ohlcv["2. high"]),
                    "Low": float(ohlcv["3. low"]),
                    "Close": float(ohlcv["4. close"]),
                    "Volume": float(ohlcv["5. volume"]),
                })
            except (KeyError, ValueError, TypeError) as e:
                issue_tracker.record(
                    symbol, asset_type, "prices",
                    "cast_failure", f"dt={dt_str}: {e}",
                )

        del data, ts

        if not rows:
            continue

        df = (
            pl.DataFrame(rows)
            .with_columns(
                pl.col("Date").str.to_datetime("%Y-%m-%d %H:%M:%S"),
            )
            .cast({
                "Open": pl.Float32,
                "High": pl.Float32,
                "Low": pl.Float32,
                "Close": pl.Float32,
                "Volume": pl.Float32,
            })
            .filter(
                (pl.col("Date").dt.date() > previous_date)
                & (pl.col("Date").dt.date() <= folder_date)
            )
            .sort("Date")
        )
        del rows

        if df.height == 0:
            issue_tracker.record(
                symbol, asset_type, "prices",
                "empty_content",
                f"no bars in ({previous_date}, {folder_date}] after truncation",
            )
            del df
            continue

        df.write_parquet(out_path, compression="zstd")
        logger.info(f"  {symbol}: saved {df.height} rows")
        del df
