"""Download historical earnings estimates data (EARNINGS_ESTIMATES) for stocks.

Unlike other fundamental endpoints, the response uses ``"symbol"`` +
``"estimates"`` top-level keys, and the annual/quarterly split is based on
the ``horizon`` field (``"fiscal year"`` vs ``"fiscal quarter"``).
"""

import logging
from pathlib import Path

import aiohttp

from historical_data_setup._common import (
    AV_BASE,
    AVResponseError,
    IssueTracker,
    RateLimiter,
    _build_fundamental_df,
    fetch_av_json,
    read_catalog_symbols,
)

logger = logging.getLogger(__name__)


async def fetch_earnings_estimates(
    catalog_dir: Path,
    historical_dir: Path,
    api_key: str,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    issue_tracker: IssueTracker,
    asset_type: str = "stocks",
) -> None:
    """Download earnings estimates data for all symbols of the given asset type."""
    catalog = read_catalog_symbols(catalog_dir, asset_type)
    output_dir = historical_dir / asset_type / "earnings_estimates"
    output_dir.mkdir(parents=True, exist_ok=True)

    total = catalog.height
    logger.info(f"earnings_estimates: {total} symbols to process")

    for idx, row in enumerate(catalog.iter_rows(named=True), 1):
        symbol = row["symbol"]
        annual_path = output_dir / f"{symbol}_annual.parquet"
        quarterly_path = output_dir / f"{symbol}_quarterly.parquet"

        if annual_path.exists() and quarterly_path.exists():
            continue

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

        # Validate top-level keys
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

        # Split by horizon, rename "date" -> "fiscalDateEnding", drop "horizon"
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

        # Check empty content after split
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

        # Build and save annual
        annual_df = _build_fundamental_df(
            annual_records, symbol, asset_type,
            "earnings_estimates", "annual", issue_tracker,
        )
        if annual_df is not None:
            annual_df.write_parquet(annual_path, compression="zstd")
            logger.info(f"  earnings_estimates: {symbol} saved {annual_df.height} annual rows")
            del annual_df

        # Build and save quarterly
        quarterly_df = _build_fundamental_df(
            quarterly_records, symbol, asset_type,
            "earnings_estimates", "quarterly", issue_tracker,
        )
        if quarterly_df is not None:
            quarterly_df.write_parquet(quarterly_path, compression="zstd")
            logger.info(f" earnings_estimates: {symbol} saved {quarterly_df.height} quarterly rows")
            del quarterly_df

        del annual_records, quarterly_records
