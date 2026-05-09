"""Fetch the EARNINGS_CALENDAR (6-month horizon) and write it to a folder.

One AV call. Synchronous. Skips the call if the destination parquet already
exists, which is what makes the historical/daily orchestrators safe to resume
without re-paying the API call.

The fetch + cast logic is identical to the previous catalog-side
``update_earnings_calendar``; only the destination has moved out of
``catalog/`` and into the per-run folders (``historical/`` and
``daily/<date>/``).
"""

import io
import logging
from pathlib import Path

import polars as pl

from asset_catalog_service.updates._common import (
    AV_BASE,
    CatalogFetchError,
    fetch_text,
)

logger = logging.getLogger(__name__)


def fetch_earnings_calendar(api_key: str, out_dir: Path) -> None:
    path = out_dir / "earnings_calendar.parquet"
    if path.exists():
        logger.info(f"earnings_calendar: {path} already exists, skipping")
        return

    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. Fetch
    logger.info("Fetching EARNINGS_CALENDAR (6-month horizon)...")
    try:
        csv_text = fetch_text(
            f"{AV_BASE}/query?function=EARNINGS_CALENDAR"
            f"&horizon=6month&apikey={api_key}"
        )
        logger.info("earnings_calendar: CSV fetched successfully")
    except CatalogFetchError as e:
        logger.error(f"earnings_calendar: failed to fetch CSV - {e}")
        return

    # 2. Transform
    raw = pl.read_csv(io.StringIO(csv_text), infer_schema_length=0)
    # AV's EARNINGS_CALENDAR CSV uses "reportDate"; the EARNINGS endpoint
    # uses "reportedDate". Normalise here so downstream sees one name.
    raw = raw.rename({"reportDate": "reportedDate"})

    df = raw.with_columns(
        pl.col("reportedDate")
        .str.to_date("%Y-%m-%d", strict=False, exact=False)
        .alias("reportedDate_parsed"),
        pl.col("fiscalDateEnding")
        .str.to_date("%Y-%m-%d", strict=False, exact=False)
        .alias("fiscalDateEnding_parsed"),
        pl.col("estimate")
        .cast(pl.Float32, strict=False)
        .alias("estimate_parsed"),
    )

    # Build cast_issues column
    df = df.with_columns(
        pl.concat_str(
            [
                pl.when(
                    pl.col("reportedDate").is_not_null()
                    & (pl.col("reportedDate") != "")
                    & pl.col("reportedDate_parsed").is_null()
                )
                .then(pl.lit("reportedDate"))
                .otherwise(pl.lit("")),
                pl.when(
                    pl.col("fiscalDateEnding").is_not_null()
                    & (pl.col("fiscalDateEnding") != "")
                    & pl.col("fiscalDateEnding_parsed").is_null()
                )
                .then(pl.lit("fiscalDateEnding"))
                .otherwise(pl.lit("")),
                pl.when(
                    pl.col("estimate").is_not_null()
                    & (pl.col("estimate") != "")
                    & pl.col("estimate_parsed").is_null()
                )
                .then(pl.lit("estimate"))
                .otherwise(pl.lit("")),
            ],
            separator=",",
            ignore_nulls=True,
        )
        .str.strip_chars(",")
        .str.replace_all(r",{2,}", ",")
        .alias("cast_issues")
    )
    df = df.with_columns(
        pl.when(pl.col("cast_issues") == "")
        .then(pl.lit(None).cast(pl.Utf8))
        .otherwise(pl.col("cast_issues"))
        .alias("cast_issues")
    )

    # Select final columns with parsed types
    df = df.select(
        "symbol",
        "name",
        pl.col("reportedDate_parsed").alias("reportedDate"),
        pl.col("fiscalDateEnding_parsed").alias("fiscalDateEnding"),
        pl.col("estimate_parsed").alias("estimate"),
        "currency",
        "timeOfTheDay",
        "cast_issues",
    )

    # Log cast status
    n_issues = df.filter(pl.col("cast_issues").is_not_null()).height
    if n_issues > 0:
        logger.warning(f"earnings_calendar: {n_issues} rows with cast issues")
    else:
        logger.info("earnings_calendar: all casts successful")

    # 3. Save
    df.write_parquet(path, compression="zstd")
    logger.info(f"earnings_calendar: saved {path} ({df.height} rows)")
