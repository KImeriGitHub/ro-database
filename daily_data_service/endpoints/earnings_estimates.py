"""Daily pull of earnings estimates (EARNINGS_ESTIMATES) for stocks.

The EARNINGS_ESTIMATES response is shaped differently from the other
fundamental endpoints (flat ``estimates`` list split by ``horizon``), so this
does not reuse the shared daily fundamental helper.
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


async def fetch_earnings_estimates(
    catalog_dir: Path,
    daily_dir: Path,
    api_key: str,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    issue_tracker: IssueTracker,
    asset_type: str,
    folder_date: date,
    previous_date: date,
    skip_empty_yield: bool = False,
    symbols_filter: set[str] | None = None,
) -> None:
    catalog = read_catalog_symbols(catalog_dir, asset_type)
    if symbols_filter is not None:
        catalog = catalog.filter(pl.col("symbol").is_in(list(symbols_filter)))
    output_dir = daily_dir / asset_type / "earnings_estimates"
    output_dir.mkdir(parents=True, exist_ok=True)

    skip_symbols: set[str] = (
        read_yield_skip_set(catalog_dir, "earnings_estimates")
        if skip_empty_yield else set()
    )

    cutoff = years_before(folder_date, 5)
    total = catalog.height
    logger.info(
        f"earnings_estimates ({asset_type}): {total} symbols to process "
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
                symbol, asset_type, "earnings_estimates",
                "empty_content",
                "skipped: yield_status False, revalidate on weekend",
            )
            continue

        logger.info(f"[{idx}/{total}] {symbol}")

        url = (
            f"{AV_BASE}/query?function=EARNINGS_ESTIMATES"
            f"&symbol={symbol}&apikey={api_key}"
        )

        try:
            data = await fetch_av_json(url, session, rate_limiter)
        except AVResponseError as e:
            issue_tracker.record(
                symbol, asset_type, "earnings_estimates", "av_throttle", str(e),
            )
            continue
        except Exception as e:
            issue_tracker.record(
                symbol, asset_type, "earnings_estimates",
                "structure_error", f"fetch failed: {e}",
            )
            continue

        expected_keys = {"symbol", "estimates"}
        missing = expected_keys - data.keys()
        if missing:
            issue_tracker.record(
                symbol, asset_type, "earnings_estimates",
                "structure_error", f"missing top-level keys: {missing}",
            )
            del data
            continue

        estimates = data.get("estimates", [])
        del data

        if not estimates:
            issue_tracker.record(
                symbol, asset_type, "earnings_estimates",
                "empty_content", "empty estimates",
            )
            continue

        annual_records: list[dict] = []
        quarterly_records: list[dict] = []

        for rec in estimates:
            horizon = rec.get("horizon")
            mapped = {"fiscalDateEnding": rec.get("date")}
            for k, v in rec.items():
                if k not in ("date", "horizon"):
                    mapped[k] = v

            if horizon == "fiscal year":
                annual_records.append(mapped)
            elif horizon == "fiscal quarter":
                quarterly_records.append(mapped)

        del estimates

        if not annual_records:
            issue_tracker.record(
                symbol, asset_type, "earnings_estimates",
                "empty_content", "no annual estimates (fiscal year)",
            )
        if not quarterly_records:
            issue_tracker.record(
                symbol, asset_type, "earnings_estimates",
                "empty_content", "no quarterly estimates (fiscal quarter)",
            )

        annual_df = _build_fundamental_df(
            annual_records, symbol, asset_type,
            "earnings_estimates", "annual", issue_tracker,
        )
        if annual_df is not None:
            truncated = annual_df.filter(since_expr("fiscalDateEnding", cutoff))
            truncated.write_parquet(annual_path, compression="zstd")
            if truncated.height == 0:
                logger.info(
                    f"  {symbol}: saved empty annual frame (no rows >= {cutoff})"
                )
            else:
                logger.info(f"  {symbol}: saved {truncated.height} annual rows")
            del annual_df, truncated

        quarterly_df = _build_fundamental_df(
            quarterly_records, symbol, asset_type,
            "earnings_estimates", "quarterly", issue_tracker,
        )
        if quarterly_df is not None:
            truncated = quarterly_df.filter(since_expr("fiscalDateEnding", cutoff))
            truncated.write_parquet(quarterly_path, compression="zstd")
            if truncated.height == 0:
                logger.info(
                    f"  {symbol}: saved empty quarterly frame (no rows >= {cutoff})"
                )
            else:
                logger.info(f"  {symbol}: saved {truncated.height} quarterly rows")
            del quarterly_df, truncated

        del annual_records, quarterly_records
