"""Per-catalog-file rollups.

Reads the parquet files produced by ``asset_catalog_service`` and returns a
flat dict suitable for JSON serialisation. Missing files are recorded as
``{"missing": True}`` so the full report is still emitted.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import polars as pl

from asset_catalog_service.updates._common import YIELD_ENDPOINTS

logger = logging.getLogger(__name__)

# Catalogs that carry a ``status`` column; broken out by Active / Delisted /
# Corrupted plus a total. The remaining catalogs only have a row count.
_STATUSED_CATALOGS = ("stocks", "etfs")
_COUNT_ONLY_CATALOGS = (
    "indices", "forex", "cryptocurrencies", "commodities", "economic",
)

# AV LISTING_STATUS uses lowercase "active"/"delisted"; the update logic in
# ``asset_catalog_service.updates.stocks_etfs._update_listing`` writes
# "Corrupted" and "Delisted" with leading capitals. Match case-insensitively.
_STATUS_BUCKETS = {
    "active": ("Active", "active"),
    "delisted": ("Delisted", "delisted"),
    "corrupted": ("Corrupted", "corrupted"),
}


def _statused_summary(path: Path) -> dict:
    if not path.exists():
        return {"missing": True}
    df = pl.read_parquet(path, columns=["status"])
    total = df.height
    out: dict = {"total": total}
    counted = 0
    for bucket, accepted in _STATUS_BUCKETS.items():
        n = df.filter(pl.col("status").is_in(list(accepted))).height
        out[bucket] = n
        counted += n
    other = total - counted
    if other:
        out["other_status"] = other
    return out


def _count_only(path: Path) -> dict:
    if not path.exists():
        return {"missing": True}
    df = pl.read_parquet(path, columns=["symbol"])
    return {"total": df.height}


def _yield_status_summary(path: Path) -> dict:
    if not path.exists():
        return {"missing": True}
    df = pl.read_parquet(path)
    total = df.height
    out: dict = {"total_rows": total, "endpoints": {}}
    for ep in YIELD_ENDPOINTS:
        if ep not in df.columns:
            continue
        col = df[ep]
        true_count = int(col.sum() or 0)
        false_count = int((col == False).sum() or 0)  # noqa: E712
        null_count = int(col.null_count())
        denom = true_count + false_count
        true_ratio = (true_count / denom) if denom else 0.0
        false_ratio = (false_count / denom) if denom else 0.0
        out["endpoints"][ep] = {
            "true": true_count,
            "false": false_count,
            "null": null_count,
            "true_ratio": round(true_ratio, 4),
            "false_ratio": round(false_ratio, 4),
        }
    return out


def _earnings_calendar_summary(path: Path, today: date) -> dict:
    if not path.exists():
        return {"missing": True}
    df = pl.read_parquet(path)
    total = df.height
    cast_issues = (
        df.filter(pl.col("cast_issues").is_not_null()).height
        if "cast_issues" in df.columns else 0
    )

    avg_days: float | None = None
    if "reportedDate" in df.columns:
        future = df.filter(
            pl.col("reportedDate").is_not_null()
            & (pl.col("reportedDate") >= today)
        )
        if future.height:
            avg_days = float(
                (future["reportedDate"] - today).dt.total_days().mean() or 0.0
            )

    return {
        "total": total,
        "cast_issues": cast_issues,
        "avg_days_to_next_reportedDate": (
            round(avg_days, 2) if avg_days is not None else None
        ),
    }


def analyze_catalog(
    catalog_dir: Path,
    today: date | None = None,
    folder_dir: Path | None = None,
) -> dict:
    """Build the ``catalog`` section of a monitoring report.

    *today* defaults to ``date.today()`` and is only used to compute the
    earnings_calendar's ``avg_days_to_next_reportedDate``.

    *folder_dir* is the historical/daily folder being analysed; it is the
    source of ``earnings_calendar.parquet`` (the file moved out of
    ``catalog/`` in favour of one copy per data-pull folder). When omitted,
    earnings_calendar is reported as missing.
    """
    today = today or date.today()
    out: dict = {}

    for name in _STATUSED_CATALOGS:
        out[name] = _statused_summary(catalog_dir / f"{name}.parquet")
    for name in _COUNT_ONLY_CATALOGS:
        out[name] = _count_only(catalog_dir / f"{name}.parquet")

    out["yield_status"] = _yield_status_summary(
        catalog_dir / "yield_status.parquet"
    )
    ec_path = (
        folder_dir / "earnings_calendar.parquet"
        if folder_dir is not None
        else None
    )
    out["earnings_calendar"] = (
        _earnings_calendar_summary(ec_path, today)
        if ec_path is not None
        else {"missing": True}
    )
    return out
