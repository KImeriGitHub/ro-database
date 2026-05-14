"""Create or update cryptocurrencies.parquet from the crypto list."""

import io
import logging
from pathlib import Path

import polars as pl

from asset_catalog_service.updates._common import (
    AV_BASE,
    fetch_text,
    update_simple_catalog,
    with_network_retry,
)

logger = logging.getLogger(__name__)


def update_cryptocurrencies(catalog_dir: Path) -> None:
    path = catalog_dir / "cryptocurrencies.parquet"

    logger.info("Fetching cryptocurrency list...")
    csv_text = with_network_retry(
        fetch_text,
        f"{AV_BASE}/cryptocurrency_list/",
        label="cryptocurrency_list",
    )
    raw = pl.read_csv(io.StringIO(csv_text))

    # from_currency = Symbol, to_currency = Market; keep USD only
    usd_only = raw.filter(pl.col("to_currency") == "USD")

    fresh = usd_only.select(
        pl.col("from_currency").alias("symbol"),
        pl.concat_str([
            pl.lit("Cryptocurrency "),
            pl.col("from_currency"),
            pl.lit(" for Market "),
            pl.col("to_currency"),
        ]).alias("name"),
    )

    if not path.exists():
        fresh = fresh.with_columns(
            pl.lit(None).cast(pl.Date).alias("ipoDate"),
            pl.lit(None).cast(pl.Date).alias("delistingDate"),
            pl.lit(None).cast(pl.Utf8).alias("status"),
        )
        fresh.write_parquet(path, compression="zstd")
        logger.info(f"Established cryptocurrencies.parquet ({fresh.height} rows)")
    else:
        update_simple_catalog("cryptocurrencies", path, fresh)
