"""Create economic.parquet (static catalog, never modified)."""

import logging
from pathlib import Path

import polars as pl

from asset_catalog_service.updates._common import ECONOMIC_ENTRIES

logger = logging.getLogger(__name__)


def update_economic(catalog_dir: Path) -> None:
    path = catalog_dir / "economic.parquet"
    if path.exists():
        logger.info("economic.parquet exists, skipping (static catalog)")
        return

    df = pl.DataFrame({
        "symbol": list(ECONOMIC_ENTRIES.keys()),
        "name": list(ECONOMIC_ENTRIES.values()),
        "status": ["Active"] * len(ECONOMIC_ENTRIES),
    })
    df.write_parquet(path, compression="zstd")
    logger.info(f"Established economic.parquet ({df.height} rows)")
