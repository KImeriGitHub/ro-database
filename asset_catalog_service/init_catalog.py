"""
Asset Catalog Service - init_catalog.py

Initial setup for all catalog parquet files.  Creates catalogs from scratch,
optionally incorporating FirstRate Data for survivorship bias-free coverage.

Usage:
    # AV only (~10k OVERVIEW queries for stock sectors, ~3 hours)
    python asset_catalog_service\init_catalog.py [--catalog-dir PATH]

    # With FirstRate Data
    python asset_catalog_service\init_catalog.py --stocks-dir PATH --etfs-dir PATH

"""

import sys
from pathlib import Path
import logging

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from maintainance_scripts.get_api_key import get_alpha_vantage_key
from maintainance_scripts.logging_setup import configure_logging
from maintainance_scripts.paths import configured_database_dir, local_catalog_dir

from asset_catalog_service.updates import (
    init_stocks_etfs,
    validate_firstrate_csvs,
    update_indices,
    update_forex,
    update_cryptocurrencies,
    update_commodities,
    update_economic,
    update_yield_status,
)

logger = logging.getLogger(__name__)


def init_all(
    catalog_dir: Path | None = None,
    stocks_dir: Path | None = None,
    etfs_dir: Path | None = None,
) -> None:
    """Run initial catalog setup in the correct order."""
    if catalog_dir is None:
        catalog_dir = local_catalog_dir(configured_database_dir())
    catalog_dir.mkdir(parents=True, exist_ok=True)

    # Validate FirstRate CSVs before any API calls or writes.
    # Raises ValueError on failure, aborting the entire process.
    validate_firstrate_csvs(stocks_dir, etfs_dir)

    api_key = get_alpha_vantage_key()

    steps = [
        (
            "stocks & ETFs",
            lambda: init_stocks_etfs(api_key, catalog_dir, stocks_dir, etfs_dir),
        ),
        ("indices", lambda: update_indices(api_key, catalog_dir)),
        ("forex", lambda: update_forex(catalog_dir)),
        ("cryptocurrencies", lambda: update_cryptocurrencies(catalog_dir)),
        ("commodities", lambda: update_commodities(catalog_dir)),
        ("economic", lambda: update_economic(catalog_dir)),
        ("yield status", lambda: update_yield_status(catalog_dir)),
    ]

    for name, func in steps:
        try:
            func()
        except Exception:
            logger.exception(f"Failed to init {name}")

    logger.info("Catalog init complete")


if __name__ == "__main__":
    import argparse

    configure_logging()
    parser = argparse.ArgumentParser(description="Initial catalog setup")
    parser.add_argument(
        "--catalog-dir",
        type=Path,
        default=None,
        help=(
            "Catalog directory (default: <database_dir>/catalog from "
            "secrets/dir_location.txt, or <project>/catalog when unset)."
        ),
    )
    parser.add_argument(
        "--stocks-dir",
        type=Path,
        default=None,
        help="FirstRate Data stocks directory (contains catalog_stocks.csv)",
    )
    parser.add_argument(
        "--etfs-dir",
        type=Path,
        default=None,
        help="FirstRate Data ETFs directory (contains catalog_etfs.csv)",
    )
    args = parser.parse_args()
    init_all(args.catalog_dir, args.stocks_dir, args.etfs_dir)
