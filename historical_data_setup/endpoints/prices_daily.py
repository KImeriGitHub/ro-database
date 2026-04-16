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
    frd_csv_path,
    read_catalog_symbols,
    validate_meta_data,
)

logger = logging.getLogger(__name__)

_TS_KEY = "Time Series (Daily)"


def _read_frd_daily_csv(path: Path) -> pl.DataFrame:
    """Read a FRD daily CSV into a DataFrame with Date and Float64 OHLCV."""
    raw = pl.read_csv(path, infer_schema_length=0)
    raw = raw.rename({c: c.strip() for c in raw.columns})
    return (
        raw.rename({
            "timestamp": "Date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        })
        .select("Date", "open", "high", "low", "close", "volume")
        .with_columns(pl.col("Date").str.to_date("%Y-%m-%d"))
        .cast({
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Float64,
        }, strict=False)
    )


def fetch_daily_prices(
    catalog_dir: Path,
    historical_dir: Path,
    api_key: str,
    rate_limiter: RateLimiter,
    issue_tracker: IssueTracker,
    asset_type: str = "stocks",
    frd_dir: Path | None = None,
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

        # --- FirstRate Data path ---
        unadj_path = frd_csv_path(frd_dir, symbol, "1day_unadjusted")
        sa_path = frd_csv_path(frd_dir, symbol, "1day_splitadjusted")
        sda_path = frd_csv_path(frd_dir, symbol, "1day_splitdivadjusted")

        if unadj_path and sa_path and sda_path:
            logger.info(f"[{idx}/{total}] {symbol}: loading from FRD")
            try:
                unadj = _read_frd_daily_csv(unadj_path)
                sa = _read_frd_daily_csv(sa_path)
                sda = _read_frd_daily_csv(sda_path)

                # Join on Date (inner) -- only keep rows present in all 3
                combined = (
                    unadj.select("Date", "open", "high", "low", "close", "volume")
                    .rename({
                        "open": "Open", "high": "High", "low": "Low",
                        "close": "Close", "volume": "Volume",
                    })
                    .join(
                        sa.select("Date", pl.col("close").alias("sa_close")),
                        on="Date", how="inner",
                    )
                    .join(
                        sda.select("Date", pl.col("close").alias("sda_close")),
                        on="Date", how="inner",
                    )
                    .sort("Date")
                )
                del unadj, sa, sda

                # Derive SplitCoefficient:
                #   cumul_split(t) = Close(t) / sa_close(t)
                #   split_coeff(t) = cumul_split(t-1) / cumul_split(t)
                combined = combined.with_columns(
                    (pl.col("Close") / pl.col("sa_close")).alias("_cumul_split"),
                )
                combined = combined.with_columns(
                    (pl.col("_cumul_split").shift(1) / pl.col("_cumul_split"))
                    .fill_null(1.0)
                    .alias("SplitCoefficient"),
                )

                # Derive DividendAmount (actual cash, matching AV):
                #   ratio(t) = sda_close(t) / sa_close(t)
                #   div_factor_change(t) = ratio(t-1) / ratio(t)
                #   derived_div(t) = sa_close(t-1) * (1 - div_factor_change(t))
                #   DividendAmount(t) = derived_div(t) * cumul_split(t-1)
                combined = combined.with_columns(
                    (pl.col("sda_close") / pl.col("sa_close")).alias("_ratio"),
                )
                combined = combined.with_columns(
                    (pl.col("_ratio").shift(1) / pl.col("_ratio")).alias("_div_factor"),
                )
                combined = combined.with_columns(
                    (
                        pl.col("sa_close").shift(1)
                        * (1.0 - pl.col("_div_factor"))
                        * pl.col("_cumul_split").shift(1)
                    )
                    .fill_null(0.0)
                    .alias("DividendAmount"),
                )

                # Count cast failures (nulls in source OHLCV columns)
                cast_failures = 0
                for col in ("Open", "High", "Low", "Close", "Volume"):
                    cast_failures += combined[col].null_count()

                if cast_failures > 0:
                    issue_tracker.record(
                        symbol, asset_type, "prices_daily",
                        "cast_failure",
                        f"FRD: {cast_failures} null values across OHLCV after casting",
                    )

                df = (
                    combined.select(
                        "Date", "Open", "High", "Low", "Close", "Volume",
                        "DividendAmount", "SplitCoefficient",
                    )
                    .cast({
                        "Open": pl.Float32,
                        "High": pl.Float32,
                        "Low": pl.Float32,
                        "Close": pl.Float32,
                        "Volume": pl.Float32,
                        "DividendAmount": pl.Float32,
                        "SplitCoefficient": pl.Float32,
                    }, strict=False)
                )
                df.write_parquet(out_path, compression="zstd")
                logger.info(
                    f"  {symbol}: saved {df.height} rows from FRD"
                    f" ({cast_failures} cast failures)"
                )
                del combined, df
            except Exception as e:
                issue_tracker.record(
                    symbol, asset_type, "prices_daily",
                    "structure_error", f"FRD load failed: {e}",
                )
            continue

        # --- Alpha Vantage path ---
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
