"""Download historical cryptocurrency daily prices (DIGITAL_CURRENCY_DAILY)."""

import logging
from pathlib import Path

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

_ASSET_TYPE = "cryptocurrencies"
_ENDPOINT = "cryptocurrencies"
_TS_KEY = "Time Series (Digital Currency Daily)"


def fetch_cryptocurrencies(
    catalog_dir: Path,
    historical_dir: Path,
    api_key: str,
    rate_limiter: RateLimiter,
    issue_tracker: IssueTracker,
    asset_type: str = "cryptocurrencies",
) -> None:
    """Download daily OHLCV data for every symbol in the cryptocurrencies catalog."""
    catalog = read_catalog_symbols(catalog_dir, _ASSET_TYPE)
    output_dir = historical_dir / _ASSET_TYPE
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
            data = fetch_av_json(url, rate_limiter)
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
                    "Volume": pl.Float32,
                })
                .sort("Date")
            )
            df.write_parquet(out_path, compression="zstd")
            logger.info(f"  {symbol}: saved {df.height} rows")
            del df

        del rows
