"""Shared constants and HTTP helpers for catalog updates."""

import logging
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import requests

logger = logging.getLogger(__name__)

AV_BASE = "https://www.alphavantage.co"

COMMODITY_ENTRIES = {
    "XAU": "Gold",
    "XAG": "Silver",
    "WTI": "West Texas Intermediate Crude Oil",
    "BRENT": "Brent Crude Oil",
    "NATURAL_GAS": "Natural Gas",
    "COPPER": "Copper",
    "ALUMINUM": "Aluminum",
    "WHEAT": "Wheat",
    "CORN": "Corn",
    "COTTON": "Cotton",
    "SUGAR": "Sugar",
    "COFFEE": "Coffee",
    "ALL_COMMODITIES": "All Commodities",
}

ECONOMIC_ENTRIES = {
    "REAL_GDP": "Real GDP",
    "REAL_GDP_PER_CAPITA": "Real GDP Per Capita",
    "TREASURY_YIELD_30Y": "Treasury Yield 30 Years",
    "TREASURY_YIELD_10Y": "Treasury Yield 10 Years",
    "TREASURY_YIELD_7Y": "Treasury Yield 7 Years",
    "TREASURY_YIELD_5Y": "Treasury Yield 5 Years",
    "TREASURY_YIELD_2Y": "Treasury Yield 2 Years",
    "TREASURY_YIELD_3M": "Treasury Yield 3 Months",
    "FEDERAL_FUNDS_RATE": "Federal Funds Rate",
    "CPI": "Consumer Price Index",
    "INFLATION": "Inflation",
    "RETAIL_SALES": "Retail Sales",
    "DURABLES": "Durables",
    "UNEMPLOYMENT": "Unemployment",
    "NONFARM_PAYROLL": "Nonfarm Payroll",
}

YIELD_ENDPOINTS = [
    "prices",
    "prices_daily",
    "income_statement",
    "balance_sheet",
    "cash_flow",
    "earnings",
    "earnings_estimates",
    "insider",
    "sentiment",
]


def fetch_text(url: str) -> str:
    """Fetch text from a URL.  Raises on HTTP errors or unexpected JSON."""
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    text = resp.text.strip()
    if text.startswith("{"):
        raise ValueError(f"Expected CSV but got JSON: {text[:200]}")
    return text


def fetch_json(url: str) -> dict:
    """Fetch JSON from a URL."""
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return resp.json()


def update_simple_catalog(
    label: str, path: Path, fresh: pl.DataFrame
) -> None:
    """Update logic shared by indices, forex, and cryptocurrency catalogs.

    ``fresh`` must have columns: symbol, name.
    The existing parquet has: symbol, name, ipoDate, delistingDate, status.
    """
    existing = pl.read_parquet(path)
    today = date.today()
    one_month_ago = today - timedelta(days=30)

    existing_syms = set(existing["symbol"].to_list())
    fresh_syms = set(fresh["symbol"].to_list())

    added = sorted(fresh_syms - existing_syms)
    missing = sorted(existing_syms - fresh_syms)

    result = existing.clone()

    # 1. New entries
    if added:
        new_rows = fresh.filter(pl.col("symbol").is_in(added)).with_columns(
            pl.lit(None).cast(pl.Date).alias("ipoDate"),
            pl.lit(None).cast(pl.Date).alias("delistingDate"),
            pl.lit(None).cast(pl.Utf8).alias("status"),
        )
        result = pl.concat([result, new_rows], how="vertical_relaxed")
        logger.info(f"{label}: {len(added)} new entries:")
        for row in new_rows.iter_rows(named=True):
            logger.info(f"  + {row['symbol']} ({row['name']})")

    # 2. Missing entries
    if missing:
        missing_rows = result.filter(pl.col("symbol").is_in(missing))

        # 2a. No delistingDate yet -> set today + Corrupted
        no_delist = missing_rows.filter(
            pl.col("delistingDate").is_null()
        )["symbol"].to_list()

        if no_delist:
            logger.info(
                f"{label}: {len(no_delist)} newly missing, "
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

        # 2b. Has delistingDate older than 1 month -> Delisted
        has_delist = missing_rows.filter(pl.col("delistingDate").is_not_null())
        if has_delist.height > 0:
            old = has_delist.filter(pl.col("delistingDate") < one_month_ago)
            if old.height > 0:
                old_syms = old["symbol"].to_list()
                logger.info(
                    f"{label}: {len(old_syms)} missing > 1 month, "
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

    result.write_parquet(path, compression="zstd")
    logger.info(f"{label}: saved ({result.height} total rows)")
