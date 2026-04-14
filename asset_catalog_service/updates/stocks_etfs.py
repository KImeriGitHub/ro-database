"""Create or update stocks.parquet and etfs.parquet from LISTING_STATUS."""

import io
import logging
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from asset_catalog_service.updates._common import AV_BASE, fetch_text

logger = logging.getLogger(__name__)


def update_stocks_etfs(api_key: str, catalog_dir: Path) -> None:
    stocks_path = catalog_dir / "stocks.parquet"
    etfs_path = catalog_dir / "etfs.parquet"

    logger.info("Fetching LISTING_STATUS (active + delisted)...")
    active_csv = fetch_text(
        f"{AV_BASE}/query?function=LISTING_STATUS&state=active&apikey={api_key}"
    )
    delisted_csv = fetch_text(
        f"{AV_BASE}/query?function=LISTING_STATUS&state=delisted&apikey={api_key}"
    )

    active_df = pl.read_csv(
        io.StringIO(active_csv), null_values=["null"], infer_schema_length=0
    )
    delisted_df = pl.read_csv(
        io.StringIO(delisted_csv), null_values=["null"], infer_schema_length=0
    )
    combined = pl.concat([active_df, delisted_df], how="vertical_relaxed")

    fresh_stocks = (
        combined.filter(pl.col("assetType") == "Stock")
        .with_columns(
            pl.col("ipoDate").cast(pl.Date, strict=False),
            pl.col("delistingDate").cast(pl.Date, strict=False),
        )
        .drop("assetType")
    )
    fresh_etfs = (
        combined.filter(pl.col("assetType") == "ETF")
        .with_columns(
            pl.col("ipoDate").cast(pl.Date, strict=False),
            pl.col("delistingDate").cast(pl.Date, strict=False),
        )
        .drop("assetType")
    )

    stocks_exists = stocks_path.exists()
    etfs_exists = etfs_path.exists()

    if stocks_exists != etfs_exists:
        missing = "stocks.parquet" if not stocks_exists else "etfs.parquet"
        present = "etfs.parquet" if not stocks_exists else "stocks.parquet"
        logger.warning(f"{missing} missing but {present} exists - re-establishing both")

    if not stocks_exists or not etfs_exists:
        fresh_stocks.write_parquet(stocks_path, compression="zstd")
        fresh_etfs.write_parquet(etfs_path, compression="zstd")
        logger.info(f"Established stocks.parquet ({fresh_stocks.height} rows)")
        logger.info(f"Established etfs.parquet ({fresh_etfs.height} rows)")
    else:
        _update_listing("stocks", stocks_path, fresh_stocks)
        _update_listing("etfs", etfs_path, fresh_etfs)


def _update_listing(label: str, path: Path, fresh: pl.DataFrame) -> None:
    """Compare existing listing catalog with fresh data and apply changes."""
    existing = pl.read_parquet(path)
    today = date.today()
    one_month_ago = today - timedelta(days=30)

    existing_syms = set(existing["symbol"].to_list())
    fresh_syms = set(fresh["symbol"].to_list())

    added = sorted(fresh_syms - existing_syms)
    vanished = sorted(existing_syms - fresh_syms)
    common = sorted(existing_syms & fresh_syms)

    result = existing.clone()

    # 1. New symbols
    if added:
        new_rows = fresh.filter(pl.col("symbol").is_in(added))
        result = pl.concat([result, new_rows], how="vertical_relaxed")
        logger.info(f"{label}: {len(added)} new entries:")
        for row in new_rows.iter_rows(named=True):
            logger.info(
                f"  + {row['symbol']} | {row['name']} | {row['exchange']} "
                f"| ipo={row['ipoDate']} | status={row['status']}"
            )

    # 2. Vanished symbols
    if vanished:
        vanished_rows = result.filter(pl.col("symbol").is_in(vanished))

        # 2a. No delistingDate yet -> set today + Corrupted
        no_delist = vanished_rows.filter(
            pl.col("delistingDate").is_null()
        )["symbol"].to_list()

        if no_delist:
            logger.info(
                f"{label}: {len(no_delist)} vanished without delistingDate, "
                f"setting delistingDate={today}, status=Corrupted:"
            )
            for s in sorted(no_delist):
                logger.info(f"  ! {s}")
            result = result.with_columns(
                pl.when(pl.col("symbol").is_in(no_delist))
                .then(pl.lit(today))
                .otherwise(pl.col("delistingDate"))
                .alias("delistingDate"),
                pl.when(pl.col("symbol").is_in(no_delist))
                .then(pl.lit("Corrupted"))
                .otherwise(pl.col("status"))
                .alias("status"),
            )

        # 2b. Has delistingDate older than 30 days -> Delisted
        has_delist = vanished_rows.filter(pl.col("delistingDate").is_not_null())
        if has_delist.height > 0:
            old = has_delist.filter(pl.col("delistingDate") < one_month_ago)
            if old.height > 0:
                old_syms = old["symbol"].to_list()
                logger.info(
                    f"{label}: {len(old_syms)} vanished > 30 days, "
                    f"marking Delisted:"
                )
                for row in old.iter_rows(named=True):
                    logger.info(
                        f"  x {row['symbol']} "
                        f"(delisted since {row['delistingDate']})"
                    )
                result = result.with_columns(
                    pl.when(pl.col("symbol").is_in(old_syms))
                    .then(pl.lit("Delisted"))
                    .otherwise(pl.col("status"))
                    .alias("status")
                )

    # 3. Check common symbols for ipoDate / delistingDate changes
    if common:
        fresh_common = fresh.filter(pl.col("symbol").is_in(common)).select(
            "symbol",
            pl.col("ipoDate").alias("_ipo_new"),
            pl.col("delistingDate").alias("_delist_new"),
        )
        merged = (
            result.filter(pl.col("symbol").is_in(common))
            .join(fresh_common, on="symbol", how="left")
        )

        # 3a. ipoDate changed -> Corrupted
        ipo_changed = merged.filter(
            pl.col("ipoDate").is_not_null()
            & pl.col("_ipo_new").is_not_null()
            & (pl.col("ipoDate") != pl.col("_ipo_new"))
        )
        if ipo_changed.height > 0:
            ipo_syms = ipo_changed["symbol"].to_list()
            logger.info(
                f"{label}: {len(ipo_syms)} ipoDate changes, marking Corrupted:"
            )
            for row in ipo_changed.iter_rows(named=True):
                logger.info(
                    f"  ! {row['symbol']}: ipo {row['ipoDate']} -> {row['_ipo_new']}"
                )
            result = result.with_columns(
                pl.when(pl.col("symbol").is_in(ipo_syms))
                .then(pl.lit("Corrupted"))
                .otherwise(pl.col("status"))
                .alias("status")
            )

        # 3b. delistingDate changed -> update value
        delist_changed = merged.filter(
            (pl.col("delistingDate") != pl.col("_delist_new"))
            | (
                pl.col("delistingDate").is_null()
                & pl.col("_delist_new").is_not_null()
            )
            | (
                pl.col("delistingDate").is_not_null()
                & pl.col("_delist_new").is_null()
            )
        )
        if delist_changed.height > 0:
            delist_syms = delist_changed["symbol"].to_list()
            logger.info(f"{label}: {len(delist_syms)} delistingDate changes:")
            for row in delist_changed.iter_rows(named=True):
                logger.info(
                    f"  ~ {row['symbol']}: "
                    f"{row['delistingDate']} -> {row['_delist_new']}"
                )
            updates = delist_changed.select(
                "symbol", pl.col("_delist_new").alias("_upd")
            )
            result = (
                result.join(updates, on="symbol", how="left")
                .with_columns(
                    pl.when(pl.col("symbol").is_in(delist_syms))
                    .then(pl.col("_upd"))
                    .otherwise(pl.col("delistingDate"))
                    .alias("delistingDate")
                )
                .drop("_upd")
            )

    result.write_parquet(path, compression="zstd")
    logger.info(f"{label}: saved ({result.height} total rows)")
