"""Create commodities.parquet (static catalog, never modified)."""

import logging
from pathlib import Path

import polars as pl

from asset_catalog_service.updates._common import COMMODITY_ENTRIES

logger = logging.getLogger(__name__)


def update_commodities(catalog_dir: Path) -> None:
    path = catalog_dir / "commodities.parquet"
    if path.exists():
        logger.info("commodities.parquet exists, skipping (static catalog)")
        return

    df = pl.DataFrame({
        "symbol": list(COMMODITY_ENTRIES.keys()),
        "name": list(COMMODITY_ENTRIES.values()),
        "status": ["Active"] * len(COMMODITY_ENTRIES),
    })
    df.write_parquet(path, compression="zstd")
    logger.info(f"Established commodities.parquet ({df.height} rows)")
