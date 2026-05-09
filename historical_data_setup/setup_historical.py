"""
Historical Data Setup - setup_historical.py

Orchestrates the one-time historical data download from Alpha Vantage.
Designed to be resumable: already-downloaded symbols are skipped.

Endpoint tasks (one per asset_type x endpoint pair) are executed concurrently
with ``asyncio.gather`` and share a single sliding-window rate limiter, so
slow endpoints (prices, sentiment) can run alongside fast ones without
exceeding the global API budget.

Usage:
    python setup_historical.py [--catalog-dir PATH] [--historical-dir PATH]
                               [--asset-types stocks etfs]
                               [--endpoints prices prices_daily]
                               [--api-tier premium]
"""

import asyncio
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import logging

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from maintainance_scripts.get_api_key import get_alpha_vantage_key
from maintainance_scripts.logging_setup import configure_logging

from asset_catalog_service.updates import finalize_yield_status
from config.settings import AV_RATE_LIMIT_PER_MIN
from historical_data_setup.earnings_calendar import fetch_earnings_calendar
from historical_data_setup.ensure_folders import ensure_historical_folders
from historical_data_setup._common import (
    IssueTracker,
    RateLimiter,
    get_av_call_count,
    reset_av_call_count,
)
from monitoring_service.report import run_report_and_persist
from historical_data_setup.endpoints.prices import fetch_intraday_prices
from historical_data_setup.endpoints.prices_daily import fetch_daily_prices
from historical_data_setup.endpoints.income_statement import fetch_income_statement
from historical_data_setup.endpoints.balance_sheet import fetch_balance_sheet
from historical_data_setup.endpoints.cash_flow import fetch_cash_flow
from historical_data_setup.endpoints.earnings import fetch_earnings
from historical_data_setup.endpoints.earnings_estimates import fetch_earnings_estimates
from historical_data_setup.endpoints.insider import fetch_insider
from historical_data_setup.endpoints.sentiment import fetch_sentiment
from historical_data_setup.endpoints.etf_profile import fetch_etf_profile
from historical_data_setup.endpoints.forex import fetch_forex
from historical_data_setup.endpoints.cryptocurrencies import fetch_cryptocurrencies
from historical_data_setup.endpoints.commodities import fetch_commodities
from historical_data_setup.endpoints.economic import fetch_economic
from historical_data_setup.endpoints.indices import fetch_indices

logger = logging.getLogger(__name__)

ENDPOINT_MAP = {
    "prices": fetch_intraday_prices,
    "prices_daily": fetch_daily_prices,
    "income_statement": fetch_income_statement,
    "balance_sheet": fetch_balance_sheet,
    "cash_flow": fetch_cash_flow,
    "earnings": fetch_earnings,
    "earnings_estimates": fetch_earnings_estimates,
    "insider": fetch_insider,
    "sentiment": fetch_sentiment,
    "etf_profile": fetch_etf_profile,
    "forex": fetch_forex,
    "cryptocurrencies": fetch_cryptocurrencies,
    "commodities": fetch_commodities,
    "economic": fetch_economic,
    "indices": fetch_indices,
}

ASSET_ENDPOINTS = {
    "stocks": [
        "prices", "prices_daily", "income_statement", "balance_sheet",
        "cash_flow", "earnings", "earnings_estimates", "insider", "sentiment",
    ],
    "etfs": ["prices", "prices_daily", "etf_profile"],
    "forex": ["forex"],
    "cryptocurrencies": ["cryptocurrencies"],
    "commodities": ["commodities"],
    "economic": ["economic"],
    "indices": ["indices"],
}

async def _run_endpoint_task(
    label: str,
    coro_factory,
) -> None:
    """Await an endpoint coroutine, logging any top-level exception instead
    of tearing down the whole gather."""
    logger.info(f"--- Starting {label} ---")
    try:
        await coro_factory()
    except Exception:
        logger.exception(f"Failed: {label}")
    else:
        logger.info(f"--- Finished {label} ---")


async def run_historical_setup(
    catalog_dir: Path | None = None,
    historical_dir: Path | None = None,
    asset_types: list[str] | None = None,
    endpoints: list[str] | None = None,
    api_tier: str = "premium",
    stocks_dir: Path | None = None,
    etfs_dir: Path | None = None,
    run_monitor: bool = True,
) -> None:
    """Orchestrate the historical data download with cross-endpoint concurrency.

    Every applicable (asset_type, endpoint) pair becomes an asyncio task.
    All tasks share one ``aiohttp.ClientSession``, one ``RateLimiter``
    (sliding window, 74/min), and one ``IssueTracker``. The rate limiter
    enforces the global AV budget across all concurrent tasks.
    """
    project_root = Path(__file__).resolve().parent.parent

    if catalog_dir is None:
        catalog_dir = project_root / "catalog"
    if historical_dir is None:
        historical_dir = project_root / "historical"
    # "Full run" means no subsetting flags were passed. Only full runs
    # finalize yield_status at the end.
    full_run = asset_types is None and endpoints is None
    # earnings_calendar is a single sync AV call producing one global parquet,
    # not a per-symbol asyncio task. Run it on a full run, or when explicitly
    # named via --endpoints earnings_calendar.
    run_earnings_calendar = endpoints is None or "earnings_calendar" in endpoints
    if asset_types is None:
        asset_types = list(ASSET_ENDPOINTS.keys())
    if endpoints is None:
        endpoints = list(ENDPOINT_MAP.keys())
    else:
        # Strip the synthetic name so it doesn't leak into the asyncio plan.
        endpoints = [ep for ep in endpoints if ep != "earnings_calendar"]

    ensure_historical_folders(historical_dir)

    # mtime-based recovery of the original start time. Touch the marker on
    # the first run; resumed runs reuse the existing mtime so the data-
    # complete date is stable across crashes/resumes.
    start_marker = historical_dir / ".setup_started_at"
    if not start_marker.exists():
        start_marker.touch()
    started_at = datetime.fromtimestamp(
        start_marker.stat().st_mtime, tz=ZoneInfo("America/New_York"),
    )

    api_key = get_alpha_vantage_key(api_tier)
    rate_limiter = RateLimiter(float(AV_RATE_LIMIT_PER_MIN))
    issue_tracker = IssueTracker()
    reset_av_call_count()

    # Single sync AV call; runs before the asyncio plan so the rate limiter
    # window is clean. Skip-if-exists guard inside the function makes this
    # cheap on resume.
    if run_earnings_calendar:
        try:
            fetch_earnings_calendar(api_key, historical_dir)
        except Exception:
            logger.exception("earnings_calendar fetch failed; continuing")

    # Build the list of (label, coroutine-factory) pairs.
    plan: list[tuple[str, object]] = []
    for asset_type in asset_types:
        applicable = ASSET_ENDPOINTS.get(asset_type, [])
        for ep_name in endpoints:
            if ep_name not in applicable:
                continue

            func = ENDPOINT_MAP.get(ep_name)
            if func is None:
                logger.warning(f"Unknown endpoint '{ep_name}', skipping")
                continue

            label = f"{ep_name} ({asset_type})"

            extra_kwargs: dict = {}
            if ep_name in ("prices", "prices_daily"):
                if asset_type == "stocks":
                    extra_kwargs["frd_dir"] = stocks_dir
                elif asset_type == "etfs":
                    extra_kwargs["frd_dir"] = etfs_dir

            plan.append((label, func, asset_type, extra_kwargs))

    if not plan:
        logger.info("No endpoint tasks to run.")
        return

    logger.info(
        f"Scheduling {len(plan)} endpoint task(s) concurrently: "
        f"{', '.join(label for label, *_ in plan)}"
    )

    connector = aiohttp.TCPConnector(limit=len(plan))
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for label, func, asset_type, extra_kwargs in plan:
            def make_factory(f=func, at=asset_type, ek=extra_kwargs):
                async def _call():
                    await f(
                        catalog_dir=catalog_dir,
                        historical_dir=historical_dir,
                        api_key=api_key,
                        session=session,
                        rate_limiter=rate_limiter,
                        issue_tracker=issue_tracker,
                        asset_type=at,
                        **ek,
                    )
                return _call

            tasks.append(_run_endpoint_task(label, make_factory()))

        await asyncio.gather(*tasks, return_exceptions=False)

    report_path = historical_dir / "ingestion_report.parquet"
    issue_tracker.save(report_path)
    logger.info(
        f"Historical setup complete. {issue_tracker.count} issues recorded."
    )

    if full_run:
        finalize_yield_status(catalog_dir, historical_dir, started_at)
        start_marker.unlink(missing_ok=True)
    else:
        logger.info(
            "Skipping yield_status finalize (partial run via --asset-types/--endpoints)"
        )

    if run_monitor:
        try:
            run_report_and_persist(
                mode="historical",
                folder_date=started_at.date(),
                catalog_dir=catalog_dir,
                folder_dir=historical_dir,
                previous_report_path=None,
                api_call_count=get_av_call_count(),
            )
        except Exception:
            logger.exception("Monitoring report failed; setup is unaffected.")


if __name__ == "__main__":
    import argparse

    configure_logging()
    parser = argparse.ArgumentParser(description="Download historical data from Alpha Vantage")
    parser.add_argument(
        "--catalog-dir", type=Path, default=None,
        help="Catalog directory (default: <project>/catalog)",
    )
    parser.add_argument(
        "--historical-dir", type=Path, default=None,
        help="Historical directory (default: <project>/historical)",
    )
    parser.add_argument(
        "--asset-types", nargs="+", default=None,
        choices=list(ASSET_ENDPOINTS.keys()),
        help="Asset types to process (default: all)",
    )
    parser.add_argument(
        "--endpoints", nargs="+", default=None,
        choices=list(ENDPOINT_MAP.keys()) + ["earnings_calendar"],
        help="Endpoints to fetch (default: all)",
    )
    parser.add_argument(
        "--api-tier", default="premium",
        choices=("standard", "premium"),
        help="API key tier (default: premium)",
    )
    parser.add_argument(
        "--stocks-dir", type=Path, default=None,
        help="FirstRate Data stocks directory (flat folder with per-symbol CSVs)",
    )
    parser.add_argument(
        "--etfs-dir", type=Path, default=None,
        help="FirstRate Data ETFs directory (flat folder with per-symbol CSVs)",
    )
    parser.add_argument(
        "--no-monitor", action="store_true",
        help="Skip the end-of-run monitoring report.",
    )
    args = parser.parse_args()

    asyncio.run(run_historical_setup(
        catalog_dir=args.catalog_dir,
        historical_dir=args.historical_dir,
        asset_types=args.asset_types,
        endpoints=args.endpoints,
        api_tier=args.api_tier,
        stocks_dir=args.stocks_dir,
        etfs_dir=args.etfs_dir,
        run_monitor=not args.no_monitor,
    ))
