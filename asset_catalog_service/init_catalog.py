"""
Asset Catalog Service - init_catalog.py

Initial setup for all catalog parquet files.  Creates catalogs from scratch,
optionally incorporating FirstRate Data for survivorship bias-free coverage.

Usage:
    # AV only (~10k OVERVIEW queries for stock sectors, ~3 hours)
    python init_catalog.py [--catalog-dir PATH]

    # With FirstRate Data
    python init_catalog.py --stocks-dir PATH --etfs-dir PATH
"""

import sys
from pathlib import Path
import logging

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from maintainance_scripts.get_api_key import get_alpha_vantage_key
from maintainance_scripts.logging_setup import configure_logging

from asset_catalog_service.updates import (
    init_stocks_etfs,
    validate_firstrate_csvs,
    update_indices,
    update_forex,
    update_cryptocurrencies,
    update_commodities,
    update_economic,
    update_yield_status,
    update_earnings_calendar,
)

logger = logging.getLogger(__name__)


def init_all(
    catalog_dir: Path | None = None,
    stocks_dir: Path | None = None,
    etfs_dir: Path | None = None,
) -> None:
    """Run initial catalog setup in the correct order."""
    if catalog_dir is None:
        catalog_dir = Path(__file__).resolve().parent.parent / "catalog"
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
        (
            "earnings calendar",
            lambda: update_earnings_calendar(api_key, catalog_dir),
        ),
    ]

    for name, func in steps:
        try:
            func()
        except Exception:
            logger.exception(f"Failed to init {name}")

    # Log symbols with null status or null ipoDate across all catalogs
    for filename in sorted(catalog_dir.glob("*.parquet")):
        df = pl.read_parquet(filename)
        name = filename.stem
        if "status" in df.columns:
            null_status = df.filter(pl.col("status").is_null())
            if null_status.height > 0:
                syms = null_status["symbol"].to_list()
                logger.warning(
                    f"{name}: {len(syms)} symbols with null status: "
                    f"{syms[:20]}"
                    + (f" ... and {len(syms) - 20} more" if len(syms) > 20 else "")
                )
        if "ipoDate" in df.columns:
            null_ipo = df.filter(pl.col("ipoDate").is_null())
            if null_ipo.height > 0:
                syms = null_ipo["symbol"].to_list()
                logger.warning(
                    f"{name}: {len(syms)} symbols with null ipoDate: "
                    f"{syms[:20]}"
                    + (f" ... and {len(syms) - 20} more" if len(syms) > 20 else "")
                )

    logger.info("Catalog init complete")


if __name__ == "__main__":
    import argparse

    configure_logging()
    parser = argparse.ArgumentParser(description="Initial catalog setup")
    parser.add_argument(
        "--catalog-dir",
        type=Path,
        default=None,
        help="Catalog directory (default: <project>/catalog)",
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
