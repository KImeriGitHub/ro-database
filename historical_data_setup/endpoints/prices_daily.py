"""Download daily adjusted price history (TIME_SERIES_DAILY_ADJUSTED) for stocks/ETFs."""

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

_TS_KEY = "Time Series (Daily)"


def fetch_daily_prices(
    catalog_dir: Path,
    historical_dir: Path,
    api_key: str,
    rate_limiter: RateLimiter,
    issue_tracker: IssueTracker,
    asset_type: str = "stocks",
) -> None:
    """Download daily adjusted price data for all symbols of the given asset type."""
    catalog = read_catalog_symbols(catalog_dir, asset_type)
    output_dir = historical_dir / asset_type / "prices_daily"
    output_dir.mkdir(parents=True, exist_ok=True)

    total = catalog.height
    logger.info(f"prices_daily ({asset_type}): {total} symbols to process")

    for idx, row in enumerate(catalog.iter_rows(named=True), 1):
        symbol = row["symbol"]
        out_path = output_dir / f"{symbol}.parquet"

        if out_path.exists():
            continue

        logger.info(f"[{idx}/{total}] {symbol}")

        url = (
            f"{AV_BASE}/query?function=TIME_SERIES_DAILY_ADJUSTED"
            f"&symbol={symbol}&outputsize=full&apikey={api_key}"
        )

        try:
            data = fetch_av_json(url, rate_limiter)
        except AVResponseError as e:
            issue_tracker.record(
                symbol, asset_type, "prices_daily",
                "av_throttle", str(e)
            )
            continue
        except Exception as e:
            issue_tracker.record(
                symbol, asset_type, "prices_daily",
                "structure_error", f"fetch failed: {e}"
            )
            continue

        validate_meta_data(data, symbol, asset_type, "prices_daily", issue_tracker)

        ts = data.get(_TS_KEY)
        if ts is None:
            issue_tracker.record(
                symbol, asset_type, "prices_daily",
                "structure_error", f"missing '{_TS_KEY}'"
            )
            del data
            continue

        if not ts:
            issue_tracker.record(
                symbol, asset_type, "prices_daily",
                "empty_content", "empty time series"
            )
            del data
            continue

        rows: list[dict] = []
        for date_str, ohlcv in ts.items():
            if not ohlcv:
                issue_tracker.record(
                    symbol, asset_type, "prices_daily",
                    "empty_content", f"empty bar at {date_str}"
                )
                continue
            try:
                rows.append({
                    "Date": date_str,
                    "Open": float(ohlcv["1. open"]),
                    "High": float(ohlcv["2. high"]),
                    "Low": float(ohlcv["3. low"]),
                    "Close": float(ohlcv["4. close"]),
                    # "5. adjusted close" intentionally skipped
                    "Volume": float(ohlcv["6. volume"]),
                    "DividendAmount": float(ohlcv["7. dividend amount"]),
                    "SplitCoefficient": float(ohlcv["8. split coefficient"]),
                })
            except (KeyError, ValueError, TypeError) as e:
                issue_tracker.record(
                    symbol, asset_type, "prices_daily",
                    "cast_failure", f"date={date_str}: {e}"
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
                    "DividendAmount": pl.Float32,
                    "SplitCoefficient": pl.Float32,
                })
                .sort("Date")
            )
            df.write_parquet(out_path, compression="zstd")
            logger.info(f"  {symbol}: saved {df.height} rows")
            del df

        del rows
