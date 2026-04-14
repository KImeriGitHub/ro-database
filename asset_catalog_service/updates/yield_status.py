"""Initialise yield_status.parquet from all asset catalogs.  No-op if it exists."""

import logging
from datetime import date
from pathlib import Path

import polars as pl

from asset_catalog_service.updates._common import YIELD_ENDPOINTS

logger = logging.getLogger(__name__)

CATALOG_FILES = [
    "stocks.parquet",
    "etfs.parquet",
    "forex.parquet",
    "indices.parquet",
    "cryptocurrencies.parquet",
    "commodities.parquet",
    "economic.parquet",
]


def update_yield_status(catalog_dir: Path) -> None:
    path = catalog_dir / "yield_status.parquet"
    if path.exists():
        logger.info("yield_status.parquet exists, no changes needed")
        return

    all_symbols = []
    for fname in CATALOG_FILES:
        fpath = catalog_dir / fname
        if not fpath.exists():
            logger.warning(f"Cannot include {fname} in yield_status: file not found")
            continue
        cat = pl.read_parquet(fpath)
        all_symbols.extend(cat["symbol"].to_list())

    if not all_symbols:
        logger.warning("Cannot init yield_status: no catalog files found")
        return

    today = date.today()

    data: dict = {"symbol": all_symbols}
    for ep in YIELD_ENDPOINTS:
        data[ep] = [None] * len(all_symbols)
    data["date"] = [today] * len(all_symbols)

    schema: dict = {"symbol": pl.Utf8}
    for ep in YIELD_ENDPOINTS:
        schema[ep] = pl.Boolean
    schema["date"] = pl.Date

    df = pl.DataFrame(data, schema=schema)
    df.write_parquet(path, compression="zstd")
    logger.info(
        f"Established yield_status.parquet "
        f"({df.height} rows, {len(YIELD_ENDPOINTS)} endpoints)"
    )
