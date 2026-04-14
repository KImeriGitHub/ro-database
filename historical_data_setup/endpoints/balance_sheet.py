"""Download historical balance sheet data (BALANCE_SHEET) for stocks."""

from pathlib import Path

from historical_data_setup._common import (
    IssueTracker,
    RateLimiter,
    fetch_fundamental_endpoint,
)


def fetch_balance_sheet(
    catalog_dir: Path,
    historical_dir: Path,
    api_key: str,
    rate_limiter: RateLimiter,
    issue_tracker: IssueTracker,
    asset_type: str = "stocks",
) -> None:
    """Download balance sheet data for all symbols of the given asset type."""
    fetch_fundamental_endpoint(
        catalog_dir=catalog_dir,
        historical_dir=historical_dir,
        api_key=api_key,
        rate_limiter=rate_limiter,
        issue_tracker=issue_tracker,
        asset_type=asset_type,
        av_function="BALANCE_SHEET",
        endpoint="balance_sheet",
        annual_key="annualReports",
        quarterly_key="quarterlyReports",
    )
