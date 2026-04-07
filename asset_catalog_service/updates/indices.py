"""Create or update indices.parquet from INDEX_CATALOG."""

import logging
from pathlib import Path

import polars as pl

from asset_catalog_service.updates._common import AV_BASE, fetch_json, update_simple_catalog

logger = logging.getLogger(__name__)


def update_indices(api_key: str, catalog_dir: Path) -> None:
    path = catalog_dir / "indices.parquet"

    logger.info("Fetching INDEX_CATALOG...")
    data = fetch_json(f"{AV_BASE}/query?function=INDEX_CATALOG&apikey={api_key}")

    fresh = pl.DataFrame(
        {"symbol": list(data.keys()), "name": list(data.values())}
    )

    if not path.exists():
        fresh = fresh.with_columns(
            pl.lit(None).cast(pl.Date).alias("ipoDate"),
            pl.lit(None).cast(pl.Date).alias("delistingDate"),
            pl.lit(None).cast(pl.Utf8).alias("status"),
        )
        fresh.write_parquet(path, compression="zstd")
        logger.info(f"Established indices.parquet ({fresh.height} rows)")
    else:
        update_simple_catalog("indices", path, fresh)
