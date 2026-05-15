"""Report-table construction for the financials builder.

The per-symbol ``report_table`` is the canonical chronological axis of
``(reportedDate, fiscalDateEnding, reportTime)`` rows that Phase 6c walks
to build per-row PIT-aware financials cells. This module owns the
construction; :mod:`data_transformation.frames.financials` consumes it
and resolves data cells against the d-PIT snapshot lookups.

Public surface (used by ``financials.py``):

* :data:`FISCAL_MATCH_DAYS` -- 10-day fuzzy-match margin shared across
  fiscalDateEnding alignment.
* :func:`_normalize_report_time` -- canonicalises raw reportTime /
  timeOfTheDay strings to ``{pre-market, post-market, other}``.
* :func:`_build_report_table` -- quarterly report_table.
* :func:`_build_annual_report_table` -- annual report_table.
"""

from __future__ import annotations

from bisect import bisect_left
from datetime import date
from typing import Any

import polars as pl

from data_transformation._common import TransformationReport


# 10-day fuzzy-match margin for fiscalDateEnding alignment across
# IS / BS / CF / E and between report_table anchors and estimates.
FISCAL_MATCH_DAYS: int = 10


_REPORT_TABLE_SCHEMA: dict[str, Any] = {
    "reportedDate": pl.Date,
    "fiscalDateEnding": pl.Date,
    "reportTime": pl.Utf8,
}


def _normalize_report_time(value: Any) -> str:
    """Map a raw reportTime / timeOfTheDay value to one of the canonical
    labels {pre-market, post-market, other}. Empty / null / unknown -> other.
    """
    if value is None:
        return "other"
    s = str(value).strip().lower()
    if s in ("pre-market", "premarket", "before-market"):
        return "pre-market"
    if s in ("post-market", "postmarket", "after-market", "after-hours", "afterhours"):
        return "post-market"
    return "other"


def _nearest_within(
    sorted_fdes: list[date], target: date, max_days: int,
) -> date | None:
    if not sorted_fdes:
        return None
    idx = bisect_left(sorted_fdes, target)
    candidates: list[date] = []
    if idx < len(sorted_fdes):
        candidates.append(sorted_fdes[idx])
    if idx > 0:
        candidates.append(sorted_fdes[idx - 1])
    best = min(candidates, key=lambda d: abs((d - target).days))
    if abs((best - target).days) > max_days:
        return None
    return best


def _build_report_table(
    earnings_q_union: pl.DataFrame,
    overview_row: dict | None,
    estimates_q_extended: pl.DataFrame | None,
) -> pl.DataFrame:
    """Build the per-symbol report_table.

    Schema: reportedDate (Date, nullable), fiscalDateEnding (Date),
    reportTime (Utf8 in {pre-market, post-market, other}, nullable).
    Sorted by reportedDate ascending; future-extension rows (null
    reportedDate) appended in fiscalDateEnding ascending order.
    """
    rows: list[dict[str, Any]] = []

    # 1. Past entries from union earnings_q.
    if not earnings_q_union.is_empty() and "fiscalDateEnding" in earnings_q_union.columns:
        for r in earnings_q_union.iter_rows(named=True):
            fde = r.get("fiscalDateEnding")
            rd = r.get("reportedDate")
            if fde is None or rd is None:
                continue
            rows.append({
                "reportedDate": rd,
                "fiscalDateEnding": fde,
                "reportTime": _normalize_report_time(r.get("reportTime")),
            })

    latest_past_fde = max((r["fiscalDateEnding"] for r in rows), default=None)

    # Build sorted list of estimate fiscalDateEndings for upcoming /
    # future-extension lookup.
    est_fdes_sorted: list[date] = []
    if estimates_q_extended is not None and not estimates_q_extended.is_empty():
        if "fiscalDateEnding" in estimates_q_extended.columns:
            est_fdes_sorted = sorted({
                f for f in estimates_q_extended["fiscalDateEnding"].drop_nulls().to_list()
            })

    # 2. Next-upcoming entry from assets_overview.
    upcoming_fde: date | None = None
    if overview_row is not None:
        ov_rd = overview_row.get("reportedDate")
        ov_tod = overview_row.get("timeOfTheDay")
        if ov_rd is not None and est_fdes_sorted:
            for f in est_fdes_sorted:
                if latest_past_fde is None or f > latest_past_fde:
                    upcoming_fde = f
                    break
            if upcoming_fde is not None:
                rows.append({
                    "reportedDate": ov_rd,
                    "fiscalDateEnding": upcoming_fde,
                    "reportTime": _normalize_report_time(ov_tod),
                })

    # 3. Further-future entries from estimates_q.
    cutoff = upcoming_fde if upcoming_fde is not None else latest_past_fde
    for f in est_fdes_sorted:
        if cutoff is not None and f <= cutoff:
            continue
        rows.append({
            "reportedDate": None,
            "fiscalDateEnding": f,
            "reportTime": None,
        })

    if not rows:
        return pl.DataFrame(schema=_REPORT_TABLE_SCHEMA)

    df = pl.DataFrame(rows, schema=_REPORT_TABLE_SCHEMA)
    known = df.filter(pl.col("reportedDate").is_not_null()).sort("reportedDate")
    unknown = df.filter(pl.col("reportedDate").is_null()).sort("fiscalDateEnding")
    return pl.concat([known, unknown], how="vertical")


def _build_annual_report_table(
    symbol: str,
    earnings_a_union: pl.DataFrame,
    quarterly_report_table: pl.DataFrame,
    estimates_a_latest: pl.DataFrame | None,
    report: TransformationReport,
) -> pl.DataFrame:
    """Build the annual report_table.

    For each annual fiscalDateEnding F_a from earnings_a_union, find the
    quarterly report_table row whose fiscalDateEnding is within
    +/- FISCAL_MATCH_DAYS. Use that row's reportedDate and reportTime.
    Annuals with no quarterly match are dropped (logged
    ``financials_annual_no_quarterly_match``).

    Future entries from estimates_a_latest (whose fiscalDateEnding is
    strictly later than the last matched annual) extend the table with
    null reportedDate / reportTime.
    """
    # Sorted quarterly fiscalDateEndings (with their report rows) for
    # nearest-neighbor matching.
    q_fdes: list[date] = []
    q_rows_by_fde: dict[date, dict[str, Any]] = {}
    if not quarterly_report_table.is_empty():
        for r in quarterly_report_table.iter_rows(named=True):
            fde = r.get("fiscalDateEnding")
            if fde is None:
                continue
            q_rows_by_fde[fde] = r
        q_fdes = sorted(q_rows_by_fde.keys())

    rows: list[dict[str, Any]] = []
    matched_annual_fdes: set[date] = set()
    no_match_count = 0

    if not earnings_a_union.is_empty() and "fiscalDateEnding" in earnings_a_union.columns:
        for r in earnings_a_union.iter_rows(named=True):
            fde = r.get("fiscalDateEnding")
            if fde is None:
                continue
            best_q = _nearest_within(q_fdes, fde, FISCAL_MATCH_DAYS)
            if best_q is None:
                no_match_count += 1
                continue
            q_row = q_rows_by_fde[best_q]
            rows.append({
                "reportedDate": q_row["reportedDate"],
                "fiscalDateEnding": fde,
                "reportTime": q_row["reportTime"],
            })
            matched_annual_fdes.add(fde)

    if no_match_count:
        report.record(
            symbol, "stocks", "financials_annually",
            "financials_annual_no_quarterly_match",
            count=no_match_count,
            detail=(
                f"{no_match_count} annual fiscalDateEnding(s) had no "
                f"quarterly match within {FISCAL_MATCH_DAYS} days"
            ),
        )

    latest_past_fde = max((r["fiscalDateEnding"] for r in rows), default=None)

    # Future-extension from estimates_a (fiscalDateEnding strictly > latest_past_fde).
    if estimates_a_latest is not None and not estimates_a_latest.is_empty():
        if "fiscalDateEnding" in estimates_a_latest.columns:
            for f in sorted({
                f for f in estimates_a_latest["fiscalDateEnding"].drop_nulls().to_list()
            }):
                if latest_past_fde is not None and f <= latest_past_fde:
                    continue
                rows.append({
                    "reportedDate": None,
                    "fiscalDateEnding": f,
                    "reportTime": None,
                })

    if not rows:
        return pl.DataFrame(schema=_REPORT_TABLE_SCHEMA)

    df = pl.DataFrame(rows, schema=_REPORT_TABLE_SCHEMA)
    known = df.filter(pl.col("reportedDate").is_not_null()).sort("reportedDate")
    unknown = df.filter(pl.col("reportedDate").is_null()).sort("fiscalDateEnding")
    return pl.concat([known, unknown], how="vertical")
