"""Shared fundamental-endpoint flow for the daily pull.

Analogous to ``historical_data_setup._common.fetch_fundamental_endpoint`` but
truncates each frame to ``fiscalDateEnding >= folder_date - 5 years`` before
writing.
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
    _build_fundamental_df,
    fetch_av_json,
    read_catalog_symbols,
)
from daily_data_service._common import read_yield_skip_set, since_expr, years_before

logger = logging.getLogger(__name__)


def _write_truncated(
    df: pl.DataFrame,
    out_path: Path,
    symbol: str,
    report_label: str,
    cutoff: date,
) -> None:
    """Filter to ``fiscalDateEnding >= cutoff`` and write. Schema is
    preserved by polars' filter even when the result is zero rows, so an
    empty frame is written with its columns and dtypes intact."""
    truncated = df.filter(since_expr("fiscalDateEnding", cutoff))
    truncated.write_parquet(out_path, compression="zstd")
    if truncated.height == 0:
        logger.info(
            f"  {symbol}: saved empty {report_label} frame (no rows >= {cutoff})"
        )
    else:
        logger.info(f"  {symbol}: saved {truncated.height} {report_label} rows")


async def fetch_fundamental_endpoint_daily(
    catalog_dir: Path,
    daily_dir: Path,
    api_key: str,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    issue_tracker: IssueTracker,
    asset_type: str,
    av_function: str,
    endpoint: str,
    annual_key: str,
    quarterly_key: str,
    folder_date: date,
    skip_empty_yield: bool = False,
    symbols_filter: set[str] | None = None,
) -> None:
    """Generic daily fetcher for fundamental endpoints with 5-year truncation.

    When ``skip_empty_yield`` is True, symbols whose ``yield_status[endpoint]``
    is False are not queried; an ``empty_content`` issue is recorded so the
    next full-run finalize resolves the cell back to False (finalize treats
    ``empty_content`` + no parquet file as False for fundamentals).

    When ``symbols_filter`` is set, the catalog is restricted to those symbols
    before iteration -- used by the weekend adjustment to retry only the
    ``(symbol, endpoint)`` pairs flagged in the ingestion report.
    """
    catalog = read_catalog_symbols(catalog_dir, asset_type)
    if symbols_filter is not None:
        catalog = catalog.filter(pl.col("symbol").is_in(list(symbols_filter)))
    output_dir = daily_dir / asset_type / endpoint
    output_dir.mkdir(parents=True, exist_ok=True)

    skip_symbols: set[str] = (
        read_yield_skip_set(catalog_dir, endpoint) if skip_empty_yield else set()
    )

    cutoff = years_before(folder_date, 5)
    total = catalog.height
    logger.info(
        f"{endpoint} ({asset_type}): {total} symbols to process "
        f"(cutoff fiscalDateEnding >= {cutoff}; "
        f"skip_empty_yield={skip_empty_yield}, skip_set={len(skip_symbols)})"
    )

    for idx, row in enumerate(catalog.iter_rows(named=True), 1):
        symbol = row["symbol"]
        annual_path = output_dir / f"{symbol}_annual.parquet"
        quarterly_path = output_dir / f"{symbol}_quarterly.parquet"

        if annual_path.exists() and quarterly_path.exists():
            continue

        if symbol in skip_symbols:
            issue_tracker.record(
                symbol, asset_type, endpoint,
                "empty_content",
                "skipped: yield_status False, revalidate on weekend",
            )
            continue

        logger.info(f"[{idx}/{total}] {symbol}")

        url = (
            f"{AV_BASE}/query?function={av_function}"
            f"&symbol={symbol}&apikey={api_key}"
        )

        try:
            data = await fetch_av_json(url, session, rate_limiter)
        except AVResponseError as e:
            issue_tracker.record(symbol, asset_type, endpoint, "av_throttle", str(e))
            continue
        except Exception as e:
            issue_tracker.record(
                symbol, asset_type, endpoint,
                "structure_error", f"fetch failed: {e}",
            )
            continue

        expected_keys = {"symbol", annual_key, quarterly_key}
        missing = expected_keys - data.keys()
        if missing:
            issue_tracker.record(
                symbol, asset_type, endpoint,
                "structure_error", f"missing top-level keys: {missing}",
            )
            del data
            continue

        annual_records = data.get(annual_key, [])
        quarterly_records = data.get(quarterly_key, [])
        del data

        if not annual_records:
            issue_tracker.record(
                symbol, asset_type, endpoint,
                "empty_content", f"empty {annual_key}",
            )
        if not quarterly_records:
            issue_tracker.record(
                symbol, asset_type, endpoint,
                "empty_content", f"empty {quarterly_key}",
            )

        annual_df = _build_fundamental_df(
            annual_records, symbol, asset_type, endpoint, "annual", issue_tracker,
        )
        if annual_df is not None:
            _write_truncated(annual_df, annual_path, symbol, "annual", cutoff)
            del annual_df

        quarterly_df = _build_fundamental_df(
            quarterly_records, symbol, asset_type, endpoint, "quarterly", issue_tracker,
        )
        if quarterly_df is not None:
            _write_truncated(quarterly_df, quarterly_path, symbol, "quarterly", cutoff)
            del quarterly_df

        del annual_records, quarterly_records
