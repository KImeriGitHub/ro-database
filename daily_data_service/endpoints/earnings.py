"""Daily pull of earnings data (EARNINGS) for stocks."""

from datetime import date
from pathlib import Path

import aiohttp

from historical_data_setup._common import IssueTracker, RateLimiter
from daily_data_service.endpoints._fundamental import fetch_fundamental_endpoint_daily


async def fetch_earnings(
    catalog_dir: Path,
    daily_dir: Path,
    api_key: str,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    issue_tracker: IssueTracker,
    asset_type: str,
    folder_date: date,
    previous_date: date,
) -> None:
    await fetch_fundamental_endpoint_daily(
        catalog_dir=catalog_dir,
        daily_dir=daily_dir,
        api_key=api_key,
        session=session,
        rate_limiter=rate_limiter,
        issue_tracker=issue_tracker,
        asset_type=asset_type,
        av_function="EARNINGS",
        endpoint="earnings",
        annual_key="annualEarnings",
        quarterly_key="quarterlyEarnings",
        folder_date=folder_date,
    )
