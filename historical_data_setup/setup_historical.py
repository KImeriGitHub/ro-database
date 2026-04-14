"""
Historical Data Setup - setup_historical.py

Orchestrates the one-time historical data download from Alpha Vantage.
Designed to be resumable: already-downloaded symbols are skipped.

Usage:
    python setup_historical.py [--catalog-dir PATH] [--historical-dir PATH]
                               [--asset-types stocks etfs]
                               [--endpoints prices prices_daily]
                               [--api-tier premium]
"""

import sys
from pathlib import Path
import logging

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from maintainance_scripts.get_api_key import get_alpha_vantage_key

from historical_data_setup.ensure_folders import ensure_historical_folders
from historical_data_setup._common import RateLimiter, IssueTracker
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

def run_historical_setup(
    catalog_dir: Path | None = None,
    historical_dir: Path | None = None,
    asset_types: list[str] | None = None,
    endpoints: list[str] | None = None,
    api_tier: str = "premium",
) -> None:
    """Orchestrate the historical data download."""
    project_root = Path(__file__).resolve().parent.parent

    if catalog_dir is None:
        catalog_dir = project_root / "catalog"
    if historical_dir is None:
        historical_dir = project_root / "historical"
    if asset_types is None:
        asset_types = list(ASSET_ENDPOINTS.keys())
    if endpoints is None:
        endpoints = list(ENDPOINT_MAP.keys())

    ensure_historical_folders(historical_dir)

    api_key = get_alpha_vantage_key(api_tier)
    rate_limiter = RateLimiter(74.9)
    issue_tracker = IssueTracker()

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
            logger.info(f"--- Starting {label} ---")
            try:
                func(
                    catalog_dir=catalog_dir,
                    historical_dir=historical_dir,
                    api_key=api_key,
                    rate_limiter=rate_limiter,
                    issue_tracker=issue_tracker,
                    asset_type=asset_type,
                )
            except Exception:
                logger.exception(f"Failed: {label}")

    report_path = historical_dir / "ingestion_report.parquet"
    issue_tracker.save(report_path)
    logger.info(
        f"Historical setup complete. {issue_tracker.count} issues recorded."
    )


if __name__ == "__main__":
    import argparse

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
        choices=list(ENDPOINT_MAP.keys()),
        help="Endpoints to fetch (default: all)",
    )
    parser.add_argument(
        "--api-tier", default="premium",
        choices=("standard", "premium"),
        help="API key tier (default: premium)",
    )
    args = parser.parse_args()

    run_historical_setup(
        catalog_dir=args.catalog_dir,
        historical_dir=args.historical_dir,
        asset_types=args.asset_types,
        endpoints=args.endpoints,
        api_tier=args.api_tier,
    )
