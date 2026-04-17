"""Download historical earnings data (EARNINGS) for stocks."""

from pathlib import Path

import aiohttp

from historical_data_setup._common import (
    IssueTracker,
    RateLimiter,
    fetch_fundamental_endpoint,
)


async def fetch_earnings(
    catalog_dir: Path,
    historical_dir: Path,
    api_key: str,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    issue_tracker: IssueTracker,
    asset_type: str = "stocks",
) -> None:
    """Download earnings data for all symbols of the given asset type."""
    await fetch_fundamental_endpoint(
        catalog_dir=catalog_dir,
        historical_dir=historical_dir,
        api_key=api_key,
        session=session,
        rate_limiter=rate_limiter,
        issue_tracker=issue_tracker,
        asset_type=asset_type,
        av_function="EARNINGS",
        endpoint="earnings",
        annual_key="annualEarnings",
        quarterly_key="quarterlyEarnings",
    )
