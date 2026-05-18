"""Daily pull of cash flow data (CASH_FLOW) for stocks."""

from datetime import date
from pathlib import Path

import aiohttp

from historical_data_setup._common import IssueTracker, RateLimiter
from daily_data_service.endpoints._fundamental import fetch_fundamental_endpoint_daily


async def fetch_cash_flow(
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
    active_only: bool = True,
) -> None:
    await fetch_fundamental_endpoint_daily(
        catalog_dir=catalog_dir,
        daily_dir=daily_dir,
        api_key=api_key,
        session=session,
        rate_limiter=rate_limiter,
        issue_tracker=issue_tracker,
        asset_type=asset_type,
        av_function="CASH_FLOW",
        endpoint="cash_flow",
        annual_key="annualReports",
        quarterly_key="quarterlyReports",
        folder_date=folder_date,
        skip_empty_yield=skip_empty_yield,
        symbols_filter=symbols_filter,
        active_only=active_only,
    )
