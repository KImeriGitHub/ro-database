"""Download intraday 1-min price history (TIME_SERIES_INTRADAY) for stocks/ETFs."""

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
    frd_csv_path,
    generate_months,
    read_catalog_symbols,
    symbol_parquet_name,
    validate_meta_data,
)

logger = logging.getLogger(__name__)

_TS_KEY = "Time Series (1min)"


class _MonthStructureBuffer:
    """Buffers per-month ``structure_error`` records for the intraday price loop.

    AV occasionally returns a malformed response for a single month (missing
    'Meta Data' or 'Time Series (1min)') while neighbouring months for the
    same symbol come back fine. Recording those one-off month errors when the
    symbol overall succeeds adds noise to the ingestion report. Other issue
    types pass through to the inner tracker untouched. Caller invokes
    ``discard()`` when at least one month produced rows, or ``flush()`` when
    no months produced rows so the underlying problem is still reported.
    """

    def __init__(self, inner: IssueTracker):
        self._inner = inner
        self._buffered: list[tuple[str, str, str, str, str]] = []

    def record(
        self,
        symbol: str,
        asset_type: str,
        endpoint: str,
        issue_type: str,
        detail: str,
    ) -> None:
        if issue_type == "structure_error":
            self._buffered.append((symbol, asset_type, endpoint, issue_type, detail))
        else:
            self._inner.record(symbol, asset_type, endpoint, issue_type, detail)

    def flush(self) -> None:
        for args in self._buffered:
            self._inner.record(*args)
        self._buffered.clear()

    def discard(self) -> None:
        self._buffered.clear()


async def fetch_intraday_prices(
    catalog_dir: Path,
    historical_dir: Path,
    api_key: str,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    issue_tracker: IssueTracker,
    asset_type: str = "stocks",
    frd_dir: Path | None = None,
) -> None:
    """Download intraday 1-min price data for all symbols of the given asset type."""
    catalog = read_catalog_symbols(catalog_dir, asset_type)
    output_dir = historical_dir / asset_type / "prices"
    output_dir.mkdir(parents=True, exist_ok=True)

    total = catalog.height
    logger.info(f"prices ({asset_type}): {total} symbols to process")

    for idx, row in enumerate(catalog.iter_rows(named=True), 1):
        symbol = row["symbol"]
        out_path = output_dir / symbol_parquet_name(asset_type, symbol)

        if out_path.exists():
            continue

        # --- FirstRate Data path ---
        frd_path = frd_csv_path(frd_dir, symbol, "1min")
        if frd_path is not None:
            try:
                raw = pl.read_csv(frd_path, infer_schema_length=0)
                raw = raw.rename({c: c.strip() for c in raw.columns})
                raw = raw.rename({
                    "timestamp": "Date",
                    "open": "Open",
                    "high": "High",
                    "low": "Low",
                    "close": "Close",
                    "volume": "Volume",
                })
                raw = raw.select("Date", "Open", "High", "Low", "Close", "Volume")

                raw = raw.with_columns(
                    pl.col("Date").str.to_datetime("%Y-%m-%d %H:%M:%S"),
                )
                df = raw.cast({
                    "Open": pl.Float32,
                    "High": pl.Float32,
                    "Low": pl.Float32,
                    "Close": pl.Float32,
                    "Volume": pl.Float32,
                }, strict=False).sort("Date")

                # Count nulls introduced by casting (excluding Date)
                cast_failures = 0
                for col in ("Open", "High", "Low", "Close", "Volume"):
                    cast_failures += df[col].null_count()

                if cast_failures > 0:
                    issue_tracker.record(
                        symbol, asset_type, "prices",
                        "cast_failure",
                        f"FRD: {cast_failures} null values across OHLCV after casting",
                    )

                df.write_parquet(out_path, compression="zstd")
                logger.info(
                    f"  prices ({asset_type}): {symbol} saved {df.height} rows from FRD"
                    f" ({cast_failures} cast failures)"
                )
                del raw, df
            except Exception as e:
                issue_tracker.record(
                    symbol, asset_type, "prices",
                    "structure_error", f"FRD load failed: {e}",
                )
            continue

        # --- Alpha Vantage path ---
        try:
            months = generate_months(row["ipoDate"], row["delistingDate"])
        except ValueError as e:
            issue_tracker.record(
                symbol, asset_type, "prices",
                "structure_error", f"date coercion failed: {e}",
            )
            continue
        if not months:
            logger.info(f"[{idx}/{total}] {symbol}: no months to fetch, skipping")
            continue

        logger.info(f"  prices ({asset_type}): [{idx}/{total}] {symbol} fetching {len(months)} months")

        all_rows: list[dict] = []
        month_tracker = _MonthStructureBuffer(issue_tracker)

        for month in months:
            url = (
                f"{AV_BASE}/query?function=TIME_SERIES_INTRADAY"
                f"&symbol={symbol}&interval=1min&month={month}"
                f"&adjusted=false&outputsize=full&apikey={api_key}"
            )

            try:
                data = await fetch_av_json(url, session, rate_limiter)
            except AVResponseError as e:
                month_tracker.record(
                    symbol, asset_type, "prices",
                    "av_throttle", f"month={month}: {e}"
                )
                continue
            except Exception as e:
                month_tracker.record(
                    symbol, asset_type, "prices",
                    "structure_error", f"month={month} fetch failed: {e}"
                )
                continue

            validate_meta_data(data, symbol, asset_type, "prices", month_tracker)

            ts = data.get(_TS_KEY)
            if ts is None:
                month_tracker.record(
                    symbol, asset_type, "prices",
                    "structure_error", f"month={month} missing '{_TS_KEY}'"
                )
                del data
                continue

            if not ts:
                month_tracker.record(
                    symbol, asset_type, "prices",
                    "empty_content", f"month={month} empty time series"
                )
                del data
                continue

            for dt_str, ohlcv in ts.items():
                if not ohlcv:
                    month_tracker.record(
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
                    month_tracker.record(
                        symbol, asset_type, "prices",
                        "cast_failure", f"month={month} dt={dt_str}: {e}"
                    )

            del data, ts

        if all_rows:
            month_tracker.discard()
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
            logger.info(f"  prices ({asset_type}): {symbol} saved {df.height} rows")
            del df
        else:
            month_tracker.flush()

        del all_rows
