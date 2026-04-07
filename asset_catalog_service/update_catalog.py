"""
Asset Catalog Service - update_catalog.py

Manages all catalog parquet files.  Designed to run:
  1. During initial historical data setup (no parquet files exist)
  2. Daily before fetching new data (updates existing catalogs)

Usage:
    python update_catalog.py [--catalog-dir PATH]
"""

import sys
from pathlib import Path
import logging

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from maintainance_scripts.get_api_key import get_alpha_vantage_key

from asset_catalog_service.updates import (
    update_stocks_etfs,
    update_indices,
    update_forex,
    update_cryptocurrencies,
    update_commodities,
    update_economic,
    update_yield_status,
    update_earnings_calendar,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def update_all(catalog_dir: Path | None = None) -> None:
    """Run every catalog update in the correct order."""
    if catalog_dir is None:
        catalog_dir = Path(__file__).resolve().parent.parent / "catalog"
    catalog_dir.mkdir(parents=True, exist_ok=True)

    api_key = get_alpha_vantage_key()

    steps = [
        ("stocks & ETFs", lambda: update_stocks_etfs(api_key, catalog_dir)),
        ("indices", lambda: update_indices(api_key, catalog_dir)),
        ("forex", lambda: update_forex(catalog_dir)),
        ("cryptocurrencies", lambda: update_cryptocurrencies(catalog_dir)),
        ("commodities", lambda: update_commodities(catalog_dir)),
        ("economic", lambda: update_economic(catalog_dir)),
        ("yield status", lambda: update_yield_status(catalog_dir)),
        (
            "earnings calendar",
            lambda: update_earnings_calendar(api_key, catalog_dir),
        ),
    ]

    for name, func in steps:
        try:
            func()
        except Exception:
            logger.exception(f"Failed to update {name}")

    logger.info("Catalog update complete")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Update asset catalogs")
    parser.add_argument(
        "--catalog-dir",
        type=Path,
        default=None,
        help="Catalog directory (default: <project>/catalog)",
    )
    args = parser.parse_args()
    update_all(args.catalog_dir)
