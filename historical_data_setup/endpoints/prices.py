"""Download intraday 1-min price history (TIME_SERIES_INTRADAY) for stocks/ETFs."""

import logging
from pathlib import Path

import polars as pl

from historical_data_setup._common import (
    AV_BASE,
    AVResponseError,
    IssueTracker,
    RateLimiter,
    fetch_av_json,
    generate_months,
    read_catalog_symbols,
    validate_meta_data,
)

logger = logging.getLogger(__name__)

_TS_KEY = "Time Series (1min)"


def fetch_intraday_prices(
    catalog_dir: Path,
    historical_dir: Path,
    api_key: str,
    rate_limiter: RateLimiter,
    issue_tracker: IssueTracker,
    asset_type: str = "stocks",
) -> None:
    """Download intraday 1-min price data for all symbols of the given asset type."""
    catalog = read_catalog_symbols(catalog_dir, asset_type)
    output_dir = historical_dir / asset_type / "prices"
    output_dir.mkdir(parents=True, exist_ok=True)

    total = catalog.height
    logger.info(f"prices ({asset_type}): {total} symbols to process")

    for idx, row in enumerate(catalog.iter_rows(named=True), 1):
        symbol = row["symbol"]
        out_path = output_dir / f"{symbol}.parquet"

        if out_path.exists():
            continue

        months = generate_months(row["ipoDate"], row["delistingDate"])
        if not months:
            logger.info(f"[{idx}/{total}] {symbol}: no months to fetch, skipping")
            continue

        logger.info(f"[{idx}/{total}] {symbol}: fetching {len(months)} months")

        all_rows: list[dict] = []

        for month in months:
            url = (
                f"{AV_BASE}/query?function=TIME_SERIES_INTRADAY"
                f"&symbol={symbol}&interval=1min&month={month}"
                f"&adjusted=false&outputsize=full&apikey={api_key}"
            )

            try:
                data = fetch_av_json(url, rate_limiter)
            except AVResponseError as e:
                issue_tracker.record(
                    symbol, asset_type, "prices",
                    "av_throttle", f"month={month}: {e}"
                )
                continue
            except Exception as e:
                issue_tracker.record(
                    symbol, asset_type, "prices",
                    "structure_error", f"month={month} fetch failed: {e}"
                )
                continue

            validate_meta_data(data, symbol, asset_type, "prices", issue_tracker)

            ts = data.get(_TS_KEY)
            if ts is None:
                issue_tracker.record(
                    symbol, asset_type, "prices",
                    "structure_error", f"month={month} missing '{_TS_KEY}'"
                )
                del data
                continue

            if not ts:
                issue_tracker.record(
                    symbol, asset_type, "prices",
                    "empty_content", f"month={month} empty time series"
                )
                del data
                continue

            for dt_str, ohlcv in ts.items():
                if not ohlcv:
                    issue_tracker.record(
                        symbol, asset_type, "prices",
                        "empty_content", f"month={month} empty bar at {dt_str}"
                    )
                    continue
                try:
                    all_rows.append({
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
                        "cast_failure", f"month={month} dt={dt_str}: {e}"
                    )

            del data, ts

        if all_rows:
            df = (
                pl.DataFrame(all_rows)
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
                .sort("Date")
            )
            df.write_parquet(out_path, compression="zstd")
            logger.info(f"  {symbol}: saved {df.height} rows")
            del df

        del all_rows
