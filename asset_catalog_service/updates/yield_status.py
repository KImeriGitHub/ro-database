"""Initialise yield_status.parquet from stocks.  No-op if it exists."""

import logging
from datetime import date
from pathlib import Path

import polars as pl

from asset_catalog_service.updates._common import YIELD_ENDPOINTS

logger = logging.getLogger(__name__)


def update_yield_status(catalog_dir: Path) -> None:
    path = catalog_dir / "yield_status.parquet"
    if path.exists():
        logger.info("yield_status.parquet exists, no changes needed")
        return

    stocks_path = catalog_dir / "stocks.parquet"
    if not stocks_path.exists():
        logger.warning("Cannot init yield_status: stocks.parquet not found")
        return

    stocks = pl.read_parquet(stocks_path)
    symbols = stocks["symbol"].to_list()
    today = date.today()

    data: dict = {"symbol": symbols}
    for ep in YIELD_ENDPOINTS:
        data[ep] = [None] * len(symbols)
    data["date"] = [today] * len(symbols)

    schema: dict = {"symbol": pl.Utf8}
    for ep in YIELD_ENDPOINTS:
        schema[ep] = pl.Utf8
    schema["date"] = pl.Date

    df = pl.DataFrame(data, schema=schema)
    df.write_parquet(path, compression="zstd")
    logger.info(
        f"Established yield_status.parquet "
        f"({df.height} rows, {len(YIELD_ENDPOINTS)} endpoints)"
    )
