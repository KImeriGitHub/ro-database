"""Daily pull of cryptocurrency daily prices (DIGITAL_CURRENCY_DAILY).

The endpoint does not support ``outputsize``, so full history is fetched and
then truncated client-side to ``(previous_date, folder_date]``.
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
from daily_data_service._common import window_expr

logger = logging.getLogger(__name__)

_ASSET_TYPE = "cryptocurrencies"
_ENDPOINT = "cryptocurrencies"
_TS_KEY = "Time Series (Digital Currency Daily)"


async def fetch_cryptocurrencies(
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

    total = catalog.height
    logger.info(f"{_ENDPOINT}: {total} symbols to process")

    for idx, row in enumerate(catalog.iter_rows(named=True), 1):
        symbol = row["symbol"]
        out_path = output_dir / f"{symbol}.parquet"

        if out_path.exists():
            continue

        logger.info(f"[{idx}/{total}] {symbol}")

        url = (
            f"{AV_BASE}/query?function=DIGITAL_CURRENCY_DAILY"
            f"&symbol={symbol}&market=USD&apikey={api_key}"
        )

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
        for date_str, ohlcv in ts.items():
            if not ohlcv:
                issue_tracker.record(
                    symbol, _ASSET_TYPE, _ENDPOINT,
                    "empty_content", f"empty bar at {date_str}",
                )
                continue
            try:
                rows.append({
                    "Date": date_str,
                    "Open": float(ohlcv["1. open"]),
                    "High": float(ohlcv["2. high"]),
                    "Low": float(ohlcv["3. low"]),
                    "Close": float(ohlcv["4. close"]),
                    "Volume": float(ohlcv["5. volume"]),
                })
            except (KeyError, ValueError, TypeError) as e:
                issue_tracker.record(
                    symbol, _ASSET_TYPE, _ENDPOINT,
                    "cast_failure", f"date={date_str}: {e}",
                )

        del data, ts

        if not rows:
            continue

        df = (
            pl.DataFrame(rows)
            .with_columns(pl.col("Date").str.to_date("%Y-%m-%d"))
            .cast({
                "Open": pl.Float32,
                "High": pl.Float32,
                "Low": pl.Float32,
                "Close": pl.Float32,
                "Volume": pl.Float32,
            })
            .filter(window_expr("Date", previous_date, folder_date))
            .sort("Date")
        )
        del rows

        df.write_parquet(out_path, compression="zstd")
        if df.height == 0:
            logger.info(
                f"  {symbol}: saved empty frame "
                f"(no bars in ({previous_date}, {folder_date}])"
            )
        else:
            logger.info(f"  {symbol}: saved {df.height} rows")
        del df
