"""Create or update forex.parquet from the physical currency list."""

import io
import logging
from pathlib import Path

import polars as pl

from asset_catalog_service.updates._common import AV_BASE, fetch_text, update_simple_catalog

logger = logging.getLogger(__name__)


def update_forex(catalog_dir: Path) -> None:
    path = catalog_dir / "forex.parquet"

    logger.info("Fetching physical currency list...")
    csv_text = fetch_text(f"{AV_BASE}/physical_currency_list/")
    raw = pl.read_csv(io.StringIO(csv_text))

    fresh = (
        raw
        .filter(pl.col("currency code") != "USD")
        .select(
            pl.concat_str([pl.col("currency code"), pl.lit("USD")]).alias("symbol"),
            pl.col("currency name").alias("name"),
        )
    )

    if not path.exists():
        fresh = fresh.with_columns(
            pl.lit(None).cast(pl.Date).alias("ipoDate"),
            pl.lit(None).cast(pl.Date).alias("delistingDate"),
            pl.lit(None).cast(pl.Utf8).alias("status"),
        )
        fresh.write_parquet(path, compression="zstd")
        logger.info(f"Established forex.parquet ({fresh.height} rows)")
    else:
        update_simple_catalog("forex", path, fresh)
