"""
Asset Catalog Service - update_catalog.py

Daily maintenance for all catalog parquet files.  Assumes catalogs already
exist (run init_catalog.py first for initial setup).

Usage:
    python update_catalog.py [--catalog-dir PATH]
"""

import sys
from pathlib import Path
import logging

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from maintainance_scripts.get_api_key import get_alpha_vantage_key
from maintainance_scripts.logging_setup import configure_logging
from maintainance_scripts.paths import configured_database_dir, local_catalog_dir

from asset_catalog_service.updates import (
    update_stocks_etfs,
    update_indices,
    update_forex,
    update_cryptocurrencies,
    update_commodities,
    update_economic,
    update_yield_status,
)

logger = logging.getLogger(__name__)


def update_all(catalog_dir: Path | None = None) -> None:
    """Run every catalog update in the correct order."""
    if catalog_dir is None:
        catalog_dir = local_catalog_dir(configured_database_dir())
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
    ]

    for name, func in steps:
        try:
            func()
        except Exception:
            logger.exception(f"Failed to update {name}")

    logger.info("Catalog update complete")


if __name__ == "__main__":
    import argparse

    configure_logging()
    parser = argparse.ArgumentParser(description="Update asset catalogs")
    parser.add_argument(
        "--catalog-dir",
        type=Path,
        default=None,
        help=(
            "Catalog directory (default: <database_dir>/catalog from "
            "secrets/dir_location.txt, or <project>/catalog when unset)."
        ),
    )
    args = parser.parse_args()
    update_all(args.catalog_dir)
