"""
Daily Data Service - setup_daily.py

Orchestrates the daily incremental pull from Alpha Vantage. Folder-date is
computed from the execution start time in ET via a top-level
``daily/.setup_started_at`` marker whose mtime survives crashes/resumes.
Previous-date is read from ``catalog/yield_status.parquet``.

When ``previous_date == folder_date`` the run is a no-op. Otherwise endpoint
tasks (one per asset_type x endpoint pair) are executed concurrently with
``asyncio.gather`` and share a single sliding-window rate limiter, so slow
endpoints (prices, sentiment) can run alongside fast ones without exceeding
the global API budget. All output goes under ``daily/<folder-date>/``.

Usage:
    python setup_daily.py [--catalog-dir PATH] [--daily-dir PATH]
                          [--asset-types stocks etfs]
                          [--endpoints prices prices_daily]
                          [--api-tier premium]
"""

import asyncio
import sys
from pathlib import Path
import logging

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from maintainance_scripts.get_api_key import get_alpha_vantage_key

from asset_catalog_service.updates import finalize_yield_status
from daily_data_service._common import (
    read_previous_date,
    resolve_start_marker,
)
from daily_data_service.ensure_folders import ensure_daily_folders
from historical_data_setup._common import RateLimiter, IssueTracker
from daily_data_service.endpoints.prices import fetch_intraday_prices
from daily_data_service.endpoints.prices_daily import fetch_daily_prices
from daily_data_service.endpoints.income_statement import fetch_income_statement
from daily_data_service.endpoints.balance_sheet import fetch_balance_sheet
from daily_data_service.endpoints.cash_flow import fetch_cash_flow
from daily_data_service.endpoints.earnings import fetch_earnings
from daily_data_service.endpoints.earnings_estimates import fetch_earnings_estimates
from daily_data_service.endpoints.insider import fetch_insider
from daily_data_service.endpoints.sentiment import fetch_sentiment
from daily_data_service.endpoints.etf_profile import fetch_etf_profile
from daily_data_service.endpoints.forex import fetch_forex
from daily_data_service.endpoints.cryptocurrencies import fetch_cryptocurrencies
from daily_data_service.endpoints.commodities import fetch_commodities
from daily_data_service.endpoints.economic import fetch_economic
from daily_data_service.endpoints.indices import fetch_indices

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
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

# Endpoints that honour ``skip_empty_yield`` (fundamental endpoints with
# partial-save behaviour, where a prior False means both annual and quarterly
# came back empty). Other endpoints ignore the flag.
YIELD_SKIP_ENDPOINTS = {
    "income_statement", "balance_sheet", "cash_flow",
    "earnings", "earnings_estimates",
}


async def _run_endpoint_task(label: str, coro_factory) -> None:
    """Await an endpoint coroutine, logging any top-level exception instead
    of tearing down the whole gather."""
    logger.info(f"--- Starting {label} ---")
    try:
        await coro_factory()
    except Exception:
        logger.exception(f"Failed: {label}")
    else:
        logger.info(f"--- Finished {label} ---")


async def run_daily_pull(
    catalog_dir: Path | None = None,
    daily_dir: Path | None = None,
    asset_types: list[str] | None = None,
    endpoints: list[str] | None = None,
    api_tier: str = "premium",
    skip_empty_yield: bool = False,
) -> None:
    """Orchestrate the daily pull with cross-endpoint concurrency.

    Output lives under ``daily_dir/<folder-date>/``. On a full run (no
    subsetting flags) the ingestion report is saved there and
    ``yield_status.parquet`` is refreshed with the folder-date.

    When ``skip_empty_yield`` is True, fundamental endpoints (see
    ``YIELD_SKIP_ENDPOINTS``) skip API calls for symbols whose yield_status
    cell is False, and record an ``empty_content`` issue in its place so the
    finalize step keeps the cell False. Intended for weekday runs; weekend
    runs should leave the flag False to re-validate cold cells.
    """
    project_root = Path(__file__).resolve().parent.parent

    if catalog_dir is None:
        catalog_dir = project_root / "catalog"
    if daily_dir is None:
        daily_dir = project_root / "daily"

    full_run = asset_types is None and endpoints is None
    if asset_types is None:
        asset_types = list(ASSET_ENDPOINTS.keys())
    if endpoints is None:
        endpoints = list(ENDPOINT_MAP.keys())

    started_at, folder_date, marker = resolve_start_marker(daily_dir)
    previous_date = read_previous_date(catalog_dir)

    logger.info(
        f"Daily pull: folder_date={folder_date}, previous_date={previous_date}, "
        f"started_at={started_at.isoformat()}"
    )

    if previous_date >= folder_date:
        logger.info(
            f"No-op: previous_date ({previous_date}) >= folder_date "
            f"({folder_date}); nothing to pull."
        )
        marker.unlink(missing_ok=True)
        return

    day_root = ensure_daily_folders(daily_dir, folder_date)

    api_key = get_alpha_vantage_key(api_tier)
    rate_limiter = RateLimiter(74.0)
    issue_tracker = IssueTracker()

    plan: list[tuple[str, object, str, str]] = []
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
            plan.append((label, func, asset_type, ep_name))

    if not plan:
        logger.info("No endpoint tasks to run.")
        return

    logger.info(
        f"Scheduling {len(plan)} endpoint task(s) concurrently: "
        f"{', '.join(label for label, *_ in plan)}"
    )
    if skip_empty_yield:
        logger.info(
            "skip_empty_yield=True: fundamental endpoints will skip symbols "
            "with False yield_status cells"
        )

    connector = aiohttp.TCPConnector(limit=len(plan))
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for label, func, asset_type, ep_name in plan:
            def make_factory(f=func, at=asset_type, ep=ep_name):
                async def _call():
                    extra: dict = {}
                    if ep in YIELD_SKIP_ENDPOINTS:
                        extra["skip_empty_yield"] = skip_empty_yield
                    await f(
                        catalog_dir=catalog_dir,
                        daily_dir=day_root,
                        api_key=api_key,
                        session=session,
                        rate_limiter=rate_limiter,
                        issue_tracker=issue_tracker,
                        asset_type=at,
                        folder_date=folder_date,
                        previous_date=previous_date,
                        **extra,
                    )
                return _call

            tasks.append(_run_endpoint_task(label, make_factory()))

        await asyncio.gather(*tasks, return_exceptions=False)

    report_path = day_root / "ingestion_report.parquet"
    issue_tracker.save(report_path)
    logger.info(
        f"Daily pull complete. {issue_tracker.count} issues recorded at {report_path}"
    )

    if full_run:
        finalize_yield_status(catalog_dir, day_root, started_at)
        marker.unlink(missing_ok=True)
    else:
        logger.info(
            "Skipping yield_status finalize (partial run via --asset-types/--endpoints)"
        )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Daily incremental pull from Alpha Vantage")
    parser.add_argument(
        "--catalog-dir", type=Path, default=None,
        help="Catalog directory (default: <project>/catalog)",
    )
    parser.add_argument(
        "--daily-dir", type=Path, default=None,
        help="Daily directory (default: <project>/daily)",
    )
    parser.add_argument(
        "--asset-types", nargs="+", default=None,
        choices=list(ASSET_ENDPOINTS.keys()),
        help="Asset types to process (default: all)",
    )
    parser.add_argument(
        "--endpoints", nargs="+", default=None,
        choices=list(ENDPOINT_MAP.keys()),
        help="Endpoints to fetch (default: all)",
    )
    parser.add_argument(
        "--api-tier", default="premium",
        choices=("standard", "premium"),
        help="API key tier (default: premium)",
    )
    parser.add_argument(
        "--skip-empty-yield", action="store_true",
        help=(
            "Skip API calls for fundamental endpoints on symbols whose "
            "yield_status cell is False (records empty_content instead). "
            "Use on weekday daily runs; leave off on weekend runs to "
            "re-validate cold cells."
        ),
    )
    args = parser.parse_args()

    asyncio.run(run_daily_pull(
        catalog_dir=args.catalog_dir,
        daily_dir=args.daily_dir,
        asset_types=args.asset_types,
        endpoints=args.endpoints,
        api_tier=args.api_tier,
        skip_empty_yield=args.skip_empty_yield,
    ))
