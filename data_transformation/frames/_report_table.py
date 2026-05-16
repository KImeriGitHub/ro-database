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


REPORT_TABLE_SCHEMA: dict[str, Any] = {
    "reportedDate": pl.Date,
    "fiscalDateEnding": pl.Date,
    "reportTime": pl.Utf8,
    # Tags the origin of each row so the incremental build path can tell
    # PIT-correct past entries (kept across runs) from refreshable
    # next-upcoming and future-extension rows. One of:
    # - "earnings_q" / "earnings_a": came from a real earnings snapshot
    # - "overview": next-upcoming entry projected from assets_overview
    # - "estimate": further-future row from earnings_estimates
    "_source": pl.Utf8,
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


def _earnings_q_past_rows(
    earnings_q_union: pl.DataFrame,
) -> list[dict[str, Any]]:
    """Extract past-entry dicts (reportedDate, fiscalDateEnding,
    reportTime, _source="earnings_q") from a union earnings_q frame.
    Used both by the fresh build of report_table and as the seed for
    the incremental rebuild (combined with newly-arrived earnings_q rows).
    """
    rows: list[dict[str, Any]] = []
    if earnings_q_union.is_empty() or "fiscalDateEnding" not in earnings_q_union.columns:
        return rows
    for r in earnings_q_union.iter_rows(named=True):
        fde = r.get("fiscalDateEnding")
        rd = r.get("reportedDate")
        if fde is None or rd is None:
            continue
        rows.append({
            "reportedDate": rd,
            "fiscalDateEnding": fde,
            "reportTime": _normalize_report_time(r.get("reportTime")),
            "_source": "earnings_q",
        })
    return rows


def _build_report_table_from_past_rows(
    past_rows: list[dict[str, Any]],
    overview_row: dict | None,
    estimates_q_extended: pl.DataFrame | None,
) -> pl.DataFrame:
    """Build the report_table from already-collected past-entry rows.

    *past_rows* must carry the report_table schema fields plus a
    ``_source`` value (typically ``"earnings_q"``). Next-upcoming
    (``_source="overview"``) and future-extension (``_source="estimate"``)
    rows are derived here from *overview_row* and *estimates_q_extended*.
    """
    rows: list[dict[str, Any]] = list(past_rows)

    latest_past_fde = max((r["fiscalDateEnding"] for r in rows), default=None)

    est_fdes_sorted: list[date] = []
    if estimates_q_extended is not None and not estimates_q_extended.is_empty():
        if "fiscalDateEnding" in estimates_q_extended.columns:
            est_fdes_sorted = sorted({
                f for f in estimates_q_extended["fiscalDateEnding"].drop_nulls().to_list()
            })

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
                    "_source": "overview",
                })

    cutoff = upcoming_fde if upcoming_fde is not None else latest_past_fde
    for f in est_fdes_sorted:
        if cutoff is not None and f <= cutoff:
            continue
        rows.append({
            "reportedDate": None,
            "fiscalDateEnding": f,
            "reportTime": None,
            "_source": "estimate",
        })

    if not rows:
        return pl.DataFrame(schema=REPORT_TABLE_SCHEMA)

    df = pl.DataFrame(rows, schema=REPORT_TABLE_SCHEMA)
    known = df.filter(pl.col("reportedDate").is_not_null()).sort("reportedDate")
    unknown = df.filter(pl.col("reportedDate").is_null()).sort("fiscalDateEnding")
    return pl.concat([known, unknown], how="vertical")


def _build_report_table(
    earnings_q_union: pl.DataFrame,
    overview_row: dict | None,
    estimates_q_extended: pl.DataFrame | None,
) -> pl.DataFrame:
    """Build the per-symbol report_table.

    Schema: reportedDate (Date, nullable), fiscalDateEnding (Date),
    reportTime (Utf8 in {pre-market, post-market, other}, nullable),
    _source (Utf8). Sorted by reportedDate ascending; future-extension
    rows (null reportedDate) appended in fiscalDateEnding ascending order.
    """
    return _build_report_table_from_past_rows(
        _earnings_q_past_rows(earnings_q_union),
        overview_row,
        estimates_q_extended,
    )


def _earnings_a_past_rows(
    symbol: str,
    earnings_a_union: pl.DataFrame,
    quarterly_report_table: pl.DataFrame,
    report: TransformationReport,
) -> list[dict[str, Any]]:
    """Extract annual past-entry dicts (reportedDate, fiscalDateEnding,
    reportTime, _source="earnings_a") by matching each annual fde to the
    nearest quarterly report_table fde within FISCAL_MATCH_DAYS.

    Annuals with no quarterly match are dropped (logged as
    ``financials_annual_no_quarterly_match``). The annual axis inherits
    its reportedDate / reportTime from the matched quarterly row because
    AV's annual EARNINGS payload only carries fde + reportedEPS.
    """
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
                "_source": "earnings_a",
            })

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
    return rows


def _build_annual_report_table_from_past_rows(
    past_rows: list[dict[str, Any]],
    estimates_a_latest: pl.DataFrame | None,
) -> pl.DataFrame:
    """Compose the annual report_table from collected past-entry rows
    (``_source="earnings_a"``) and the latest annual estimates.

    Future-extension rows from *estimates_a_latest* whose fde is strictly
    greater than the latest past annual fde are appended with null
    reportedDate / reportTime and ``_source="estimate"``.
    """
    rows: list[dict[str, Any]] = list(past_rows)
    latest_past_fde = max((r["fiscalDateEnding"] for r in rows), default=None)

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
                    "_source": "estimate",
                })

    if not rows:
        return pl.DataFrame(schema=REPORT_TABLE_SCHEMA)

    df = pl.DataFrame(rows, schema=REPORT_TABLE_SCHEMA)
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
    past_rows = _earnings_a_past_rows(
        symbol, earnings_a_union, quarterly_report_table, report,
    )
    return _build_annual_report_table_from_past_rows(
        past_rows, estimates_a_latest,
    )
