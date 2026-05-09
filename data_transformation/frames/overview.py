"""Build ``assets_overview.parquet``: one row per (symbol, asset_type) across
all 7 catalog files, joined with the next upcoming earnings entry and the
stocks-only sector.
"""

from __future__ import annotations

import logging
import re
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from data_transformation._common import ASSET_TYPES, cast_to_schema

logger = logging.getLogger(__name__)

_DATE_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


OVERVIEW_SCHEMA: dict[str, Any] = {
    "symbol": pl.Utf8,
    "assetType": pl.Utf8,
    "about": pl.Utf8,
    "reportedDate": pl.Date,
    "timeOfTheDay": pl.Utf8,
    "sector": pl.Utf8,
}


def _resolve_earnings_calendar_path(
    daily_dir: Path | None, historical_dir: Path | None
) -> Path | None:
    """Newest ``daily/<YYYY-MM-DD>/earnings_calendar.parquet``, falling back
    to ``historical/earnings_calendar.parquet``. ``None`` when neither dir
    yields a file (caller logs a warning and emits null fields)."""
    if daily_dir is not None and daily_dir.exists():
        candidates: list[date] = []
        for child in daily_dir.iterdir():
            if not child.is_dir():
                continue
            if not _DATE_DIR_RE.match(child.name):
                continue
            try:
                candidates.append(date.fromisoformat(child.name))
            except ValueError:
                continue
        for d in sorted(candidates, reverse=True):
            p = daily_dir / d.isoformat() / "earnings_calendar.parquet"
            if p.exists():
                return p
    if historical_dir is not None:
        p = historical_dir / "earnings_calendar.parquet"
        if p.exists():
            return p
    return None


def build_assets_overview(
    catalog_dir: Path,
    today: date | None = None,
    daily_dir: Path | None = None,
    historical_dir: Path | None = None,
) -> pl.DataFrame:
    """Build the overview frame from ``catalog/*.parquet``.

    *today* gates the "next upcoming earnings reportedDate" lookup; defaults
    to the system date. Pass an explicit value in tests for determinism.

    *daily_dir* / *historical_dir* locate ``earnings_calendar.parquet`` --
    the newest ``daily/<date>/`` copy wins, falling back to ``historical/``.
    When both are ``None`` (or no file is found) the join is skipped and
    ``reportedDate`` / ``timeOfTheDay`` come back null.

    Returns a DataFrame conforming exactly to ``OVERVIEW_SCHEMA``.
    """
    today = today or date.today()

    asset_parts: list[pl.DataFrame] = []
    for asset_type in ASSET_TYPES:
        path = catalog_dir / f"{asset_type}.parquet"
        if not path.exists():
            logger.warning("catalog file missing, skipping: %s", path)
            continue
        df = pl.read_parquet(path)
        if "symbol" not in df.columns:
            logger.warning("catalog %s lacks 'symbol' column, skipping", path)
            continue
        about_expr = (
            pl.col("name").cast(pl.Utf8).alias("about")
            if "name" in df.columns
            else pl.lit("", dtype=pl.Utf8).alias("about")
        )
        asset_parts.append(
            df.select(
                pl.col("symbol").cast(pl.Utf8),
                pl.lit(asset_type, dtype=pl.Utf8).alias("assetType"),
                about_expr,
            )
        )

    if not asset_parts:
        logger.warning("no asset catalogs found under %s", catalog_dir)
        return pl.DataFrame(schema=OVERVIEW_SCHEMA)

    overview = pl.concat(asset_parts, how="vertical")

    overview = _join_earnings_calendar(
        overview, daily_dir, historical_dir, today
    )
    overview = _join_stock_sector(overview, catalog_dir)

    overview = overview.with_columns(
        pl.col("about").fill_null(""),
        pl.col("timeOfTheDay").fill_null(""),
        pl.col("sector").fill_null(""),
    )

    overview = overview.sort(["assetType", "symbol"])
    return cast_to_schema(overview, OVERVIEW_SCHEMA, "assets_overview")


def _join_earnings_calendar(
    overview: pl.DataFrame,
    daily_dir: Path | None,
    historical_dir: Path | None,
    today: date,
) -> pl.DataFrame:
    ec_path = _resolve_earnings_calendar_path(daily_dir, historical_dir)
    if ec_path is None:
        logger.warning(
            "earnings_calendar.parquet not found under daily_dir=%s or "
            "historical_dir=%s; leaving columns null",
            daily_dir, historical_dir,
        )
        return overview.with_columns(
            pl.lit(None, dtype=pl.Date).alias("reportedDate"),
            pl.lit(None, dtype=pl.Utf8).alias("timeOfTheDay"),
        )

    ec = pl.read_parquet(ec_path)
    needed = {"symbol", "reportedDate", "timeOfTheDay"}
    missing = needed - set(ec.columns)
    if missing:
        logger.warning(
            "earnings_calendar.parquet missing columns %s, leaving null", missing
        )
        return overview.with_columns(
            pl.lit(None, dtype=pl.Date).alias("reportedDate"),
            pl.lit(None, dtype=pl.Utf8).alias("timeOfTheDay"),
        )

    next_earnings = (
        ec.select(
            pl.col("symbol").cast(pl.Utf8),
            pl.col("reportedDate").cast(pl.Date),
            pl.col("timeOfTheDay").cast(pl.Utf8),
        )
        .filter(pl.col("reportedDate").is_not_null() & (pl.col("reportedDate") >= today))
        .sort("reportedDate")
        .group_by("symbol", maintain_order=True)
        .agg(
            pl.col("reportedDate").first(),
            pl.col("timeOfTheDay").first(),
        )
    )
    return overview.join(next_earnings, on="symbol", how="left")


def _join_stock_sector(overview: pl.DataFrame, catalog_dir: Path) -> pl.DataFrame:
    stocks_path = catalog_dir / "stocks.parquet"
    if not stocks_path.exists():
        return overview.with_columns(pl.lit(None, dtype=pl.Utf8).alias("sector"))

    stocks = pl.read_parquet(stocks_path)
    if "sector" not in stocks.columns:
        return overview.with_columns(pl.lit(None, dtype=pl.Utf8).alias("sector"))

    sec = stocks.select(
        pl.col("symbol").cast(pl.Utf8),
        pl.col("sector").cast(pl.Utf8).alias("_sector_from_stocks"),
    )
    # Join on symbol but only keep the sector for stock-typed rows; an
    # accidental same-symbol collision in another catalog must not inherit
    # the stock's sector.
    return (
        overview.join(sec, on="symbol", how="left")
        .with_columns(
            pl.when(pl.col("assetType") == "stocks")
            .then(pl.col("_sector_from_stocks"))
            .otherwise(pl.lit(None, dtype=pl.Utf8))
            .alias("sector")
        )
        .drop("_sector_from_stocks")
    )


def write_assets_overview(
    catalog_dir: Path,
    dest_dir: Path,
    today: date | None = None,
    daily_dir: Path | None = None,
    historical_dir: Path | None = None,
) -> Path:
    """Compute the overview and write to ``<dest_dir>/assets_overview.parquet``.

    Returns the written path.
    """
    overview = build_assets_overview(
        catalog_dir,
        today=today,
        daily_dir=daily_dir,
        historical_dir=historical_dir,
    )
    dest_dir.mkdir(parents=True, exist_ok=True)
    out_path = dest_dir / "assets_overview.parquet"
    overview.write_parquet(out_path)
    logger.info("wrote %s rows=%d", out_path, overview.height)
    return out_path
