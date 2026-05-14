"""Phase 6c: financials_quarterly and financials_annually for stocks.

Builds per-row PIT-aware financials frames from the five pairs of
(annual, quarterly) parquet files under
``historical/stocks/{endpoint}/`` and
``daily/<YYYY-MM-DD>/stocks/{endpoint}/`` for the endpoints
``income_statement``, ``balance_sheet``, ``cash_flow``, ``earnings``,
and ``earnings_estimates``.

Row axis is ``shareprice_daily.Date``. For each row date d we resolve
the right d-PIT snapshot for the statement endpoints (IS / BS / CF /
quarterly EARNINGS) and look up cells via a per-symbol ``report_table``
of ``(reportedDate, fiscalDateEnding, reportTime)`` sorted by
``reportedDate`` ascending, with future-extension rows tail-sorted by
``fiscalDateEnding``.

The ``report_table`` is built by merging earnings_q rows from all
available snapshots (historical + every daily); a ``reportedDate``
mismatch across snapshots for the same ``fiscalDateEnding`` triggers a
full no-op on the symbol (both financials frames saved empty).
``earnings_estimates`` uses the latest snapshot (extended via /4
synthesis from the annual estimates file).

Returns ``(financials_quarterly, financials_annually)``. Driven from
``frames/stocks_etfs.py``.
"""

from __future__ import annotations

import logging
import re
from bisect import bisect_left, bisect_right
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from data_transformation._common import TransformationReport
from data_transformation.AssetDataService import (
    SCHEMAS,
    _AM_BASE_FIELDS,
    _AP_BASE_FIELDS,
    _QM_BASE_FIELDS,
    _QP_BASE_FIELDS,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ENDPOINTS: tuple[str, ...] = (
    "income_statement", "balance_sheet", "cash_flow", "earnings",
    "earnings_estimates",
)
SUFFIXES: tuple[str, ...] = ("_quarterly", "_annual")

# 10-day fuzzy-match margin for fiscalDateEnding alignment across
# IS / BS / CF / E and between report_table anchors and estimates.
FISCAL_MATCH_DAYS: int = 10

# Tolerance for the "no upcoming reportedDate" case: when d is past every
# known reportedDate but within this many days of the latest one, treat
# m_anchor as past-the-end of the known prefix (so qm0 nulls but qm{m>=1}
# still walks back through past quarters and qp_{n} can use future-extension
# rows). Beyond this, fall back to the all-null defensive case. assets_overview
# does not always supply an upcoming reportedDate (see int_test_transform.py),
# so this tolerance keeps recent rows usable.
NO_ANCHOR_TOLERANCE_DAYS: int = 60

# Statement endpoints whose rows feed the _qm{m>=1} / _am{m>=1} cells.
# Order matters: when a field appears in multiple endpoints (netIncome
# lives in both INCOME_STATEMENT and CASH_FLOW), the FIRST endpoint in
# this tuple that has the row supplies the value (income_statement
# wins over cash_flow for netIncome).
_DATA_ENDPOINTS: tuple[str, ...] = (
    "income_statement", "balance_sheet", "cash_flow", "earnings",
)

# Anchor fields that come from report_table, not from snapshot rows.
_ANCHOR_FIELDS_QM: frozenset[str] = frozenset({
    "days_to_fiscalDateEnding", "days_to_reportedDate", "reportTime",
})
_ANCHOR_FIELDS_AM: frozenset[str] = frozenset({
    "days_to_fiscalDateEnding", "days_to_reportedDate",
})
# Data-only field lists (what we look up in snapshots).
_QM_DATA_FIELDS: list[tuple[str, Any]] = [
    (n, t) for n, t in _QM_BASE_FIELDS if n not in _ANCHOR_FIELDS_QM
]
_AM_DATA_FIELDS: list[tuple[str, Any]] = [
    (n, t) for n, t in _AM_BASE_FIELDS if n not in _ANCHOR_FIELDS_AM
]
_QP_DATA_FIELDS: list[tuple[str, Any]] = list(_QP_BASE_FIELDS)
_AP_DATA_FIELDS: list[tuple[str, Any]] = list(_AP_BASE_FIELDS)

# How many days before a reportedDate the qm0 / am0 cells stay populated
# when no earnings_calendar snapshot is available at d (historical regime).
# A 14-day window approximates the typical advance-notice that companies
# publish their earnings date.
NO_EC_PRE_REPORT_DAYS: int = 14

# m / n axis ranges per the schemas.
_QM_MS: list[int] = list(range(1, 17))   # m>=1 carries data; m=0 only anchors
_AM_MS: list[int] = list(range(1, 5))
_QP_NS: list[int] = list(range(-8, 5))   # -8..4
_AP_NS: list[int] = list(range(-2, 2))   # -2..1

# Estimate fields that are NOT divided by 4 when synthesising quarterly
# rows from annual estimates: counts and revisions copy verbatim.
_ANNUAL_TO_Q_NO_DIVIDE: frozenset[str] = frozenset({
    "eps_estimate_analyst_count",
    "revenue_estimate_analyst_count",
    "eps_estimate_revision_down_trailing_30_days",
    "eps_estimate_revision_down_trailing_7_days",
    "eps_estimate_revision_up_trailing_30_days",
    "eps_estimate_revision_up_trailing_7_days",
})

_SNAPSHOT_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _signed_suffix(n: int) -> str:
    if n < 0:
        return f"m{-n}"
    if n > 0:
        return f"p{n}"
    return "0"


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


def _snapshot_date_from_path(p: Path) -> date | None:
    """Return the snapshot date for a path under
    ``daily/<YYYY-MM-DD>/stocks/<endpoint>/file.parquet``, else ``None``
    (historical or any other layout).
    """
    if len(p.parents) < 3:
        return None
    name = p.parents[2].name
    if not _SNAPSHOT_DIR_RE.match(name):
        return None
    try:
        return date.fromisoformat(name)
    except ValueError:
        return None


def _read_parquet_or_none(path: Path | None) -> pl.DataFrame | None:
    if path is None:
        return None
    try:
        return pl.read_parquet(path)
    except Exception as exc:
        logger.warning("failed to read %s: %s", path, exc)
        return None


def _organize_paths(
    source_paths: dict[tuple[str, str], list[Path]],
) -> tuple[
    dict[date, dict[tuple[str, str], Path]],
    dict[tuple[str, str], Path],
]:
    """Split source_paths into per-snapshot (daily) and historical."""
    snapshots: dict[date, dict[tuple[str, str], Path]] = {}
    historical: dict[tuple[str, str], Path] = {}
    for key, paths in source_paths.items():
        for p in paths:
            sd = _snapshot_date_from_path(p)
            if sd is None:
                historical[key] = p
            else:
                snapshots.setdefault(sd, {})[key] = p
    return snapshots, historical


def _resolve_snapshot_date(
    d: date,
    snapshot_dates_sorted: list[date],
) -> tuple[date | None, bool]:
    """Return (snapshot_date, is_fallback) for row date d.

    snapshot_date == None means "use historical baseline (no daily)".
    is_fallback is True when snapshot_date < d (used the most recent
    earlier daily snapshot rather than an exact match).
    """
    if not snapshot_dates_sorted:
        return None, False
    idx = bisect_left(snapshot_dates_sorted, d)
    if idx < len(snapshot_dates_sorted) and snapshot_dates_sorted[idx] == d:
        return d, False
    if idx == 0:
        return None, False
    return snapshot_dates_sorted[idx - 1], True


# ---------------------------------------------------------------------------
# FdeLookup: index a frame by fiscalDateEnding for nearest-neighbor matching
# ---------------------------------------------------------------------------

class FdeLookup:
    """Index a DataFrame by ``fiscalDateEnding`` for O(log K) nearest-neighbor
    lookup within a +/- N day margin.
    """

    __slots__ = ("rows", "sorted_fdes")

    def __init__(self, df: pl.DataFrame | None):
        self.rows: dict[date, dict[str, Any]] = {}
        self.sorted_fdes: list[date] = []
        if df is None or df.is_empty() or "fiscalDateEnding" not in df.columns:
            return
        for row in df.iter_rows(named=True):
            fde = row.get("fiscalDateEnding")
            if fde is None:
                continue
            # Last-writer-wins on duplicate fde (shouldn't happen in
            # practice, but be defensive).
            self.rows[fde] = row
        self.sorted_fdes = sorted(self.rows.keys())

    def find_within(
        self, target: date, max_days: int,
    ) -> tuple[dict[str, Any] | None, int]:
        """Return (row_dict, days_diff) for the closest fiscalDateEnding
        within +/- max_days. days_diff = (best_fde - target).days.
        Returns (None, 0) if none within margin.
        """
        if not self.sorted_fdes:
            return None, 0
        idx = bisect_left(self.sorted_fdes, target)
        candidates: list[date] = []
        if idx < len(self.sorted_fdes):
            candidates.append(self.sorted_fdes[idx])
        if idx > 0:
            candidates.append(self.sorted_fdes[idx - 1])
        best = min(candidates, key=lambda d: abs((d - target).days))
        diff = (best - target).days
        if abs(diff) > max_days:
            return None, diff
        return self.rows[best], diff

    def has_fde_within(self, target: date, max_days: int) -> bool:
        if not self.sorted_fdes:
            return False
        idx = bisect_left(self.sorted_fdes, target)
        for cand_idx in (idx, idx - 1):
            if 0 <= cand_idx < len(self.sorted_fdes):
                if abs((self.sorted_fdes[cand_idx] - target).days) <= max_days:
                    return True
        return False


# ---------------------------------------------------------------------------
# Earnings-calendar snapshot lookup (per-symbol, per-snapshot-date)
# ---------------------------------------------------------------------------

class EarningsCalendarSnap:
    """Per-symbol earnings_calendar lookup for one daily snapshot date.

    Two access patterns used by the qm0 / am0 gating:

    * ``max_rd_row``: the row with the largest ``reportedDate`` for the
      symbol. Used to gate qm0 (the next-upcoming quarterly).
    * ``fde_lookup``: nearest-neighbor lookup by ``fiscalDateEnding`` for
      gating am0 against the matched annual fde.
    """

    __slots__ = ("max_rd_row", "fde_lookup")

    def __init__(self, rows: list[dict[str, Any]]):
        self.max_rd_row: dict[str, Any] | None = None
        df = pl.DataFrame(rows) if rows else None
        self.fde_lookup = FdeLookup(df)
        # Pick the row with max non-null reportedDate.
        best: dict[str, Any] | None = None
        for r in rows:
            rd = r.get("reportedDate")
            if rd is None:
                continue
            if best is None or rd > best["reportedDate"]:
                best = r
        self.max_rd_row = best


def _build_earnings_calendar_index(
    daily_dir: Path,
) -> tuple[dict[date, dict[str, EarningsCalendarSnap]], list[date]]:
    """Scan ``daily/<YYYY-MM-DD>/earnings_calendar.parquet`` once and return
    ``(index, snap_dates_sorted)`` where ``index[snap_date][symbol]`` is the
    per-symbol :class:`EarningsCalendarSnap` for that snapshot.

    Snapshots that lack the file are absent from both the index keys and
    the sorted-dates list. The historical ``earnings_calendar.parquet``
    intentionally is **not** included: it carries no PIT timestamp and
    using it for arbitrary historical row dates would leak.
    """
    if not daily_dir.is_dir():
        return {}, []
    index: dict[date, dict[str, EarningsCalendarSnap]] = {}
    snap_dates: list[date] = []
    for entry in daily_dir.iterdir():
        if not entry.is_dir() or not _SNAPSHOT_DIR_RE.match(entry.name):
            continue
        try:
            sd = date.fromisoformat(entry.name)
        except ValueError:
            continue
        ec_path = entry / "earnings_calendar.parquet"
        if not ec_path.exists():
            continue
        df = _read_parquet_or_none(ec_path)
        if df is None or df.is_empty() or "symbol" not in df.columns:
            continue
        by_sym: dict[str, list[dict[str, Any]]] = {}
        for r in df.iter_rows(named=True):
            sym = r.get("symbol")
            if sym is None:
                continue
            by_sym.setdefault(sym, []).append(r)
        index[sd] = {s: EarningsCalendarSnap(rows) for s, rows in by_sym.items()}
        snap_dates.append(sd)
    snap_dates.sort()
    return index, snap_dates


def _resolve_ec_snap(
    d: date, snap_dates_sorted: list[date],
) -> date | None:
    """Return the largest snap_date <= d, or None if no snapshot exists
    at or before d (same fallback semantics as statement files).
    """
    if not snap_dates_sorted:
        return None
    idx = bisect_left(snap_dates_sorted, d)
    if idx < len(snap_dates_sorted) and snap_dates_sorted[idx] == d:
        return d
    if idx == 0:
        return None
    return snap_dates_sorted[idx - 1]


# ---------------------------------------------------------------------------
# All-snapshot earnings union with reportedDate consistency check
# ---------------------------------------------------------------------------

def _union_earnings_with_consistency(
    symbol: str,
    suffix: str,
    snapshots: dict[date, dict[tuple[str, str], Path]],
    historical: dict[tuple[str, str], Path],
    report: TransformationReport,
) -> tuple[pl.DataFrame, bool]:
    """Union earnings rows across all snapshots (historical + every daily).

    Keyed by fiscalDateEnding. Most recent snapshot wins for non-key fields.

    Two cross-snapshot consistency checks run while merging:

    * Same ``fiscalDateEnding`` with different ``reportedDate`` values
      (the provider rewrote a reportedDate retroactively): log
      ``financials_reportedDate_mismatch`` and return ``(empty, True)``
      so the caller can no-op the symbol.
    * Same ``reportedDate`` with different ``fiscalDateEnding`` values
      (the provider rewrote a fiscalDateEnding retroactively): log
      ``financials_fiscalDateEnding_offcycle`` per drift but **do not**
      no-op the symbol; both rows survive in the merged output keyed by
      their respective fdes.

    Returns (union_df, mismatch_flag). union_df has at least
    ``fiscalDateEnding`` and (for ``_quarterly``) ``reportedDate`` and
    ``reportTime`` columns when source data is present.
    """
    key = ("earnings", suffix)
    sources: list[tuple[str, pl.DataFrame]] = []
    if key in historical:
        df = _read_parquet_or_none(historical[key])
        if df is not None:
            sources.append(("historical", df))
    for sd in sorted(snapshots.keys()):
        if key in snapshots[sd]:
            df = _read_parquet_or_none(snapshots[sd][key])
            if df is not None:
                sources.append((sd.isoformat(), df))

    if not sources:
        return pl.DataFrame(), False

    merged: dict[date, dict[str, Any]] = {}
    seen_reported: dict[date, date] = {}
    seen_fde_for_rd: dict[date, date] = {}
    fde_drift_count = 0
    fde_drift_samples: list[str] = []
    frame_label = (
        "financials_quarterly" if suffix == "_quarterly"
        else "financials_annually"
    )

    for src_label, df in sources:
        for row in df.iter_rows(named=True):
            fde = row.get("fiscalDateEnding")
            if fde is None:
                continue
            rd = row.get("reportedDate")
            if rd is not None:
                prev = seen_reported.get(fde)
                if prev is not None and prev != rd:
                    report.record(
                        symbol, "stocks", frame_label,
                        "financials_reportedDate_mismatch", count=1,
                        detail=(
                            f"fde={fde.isoformat()} reportedDate "
                            f"{prev.isoformat()} vs {rd.isoformat()} "
                            f"(snapshot {src_label})"
                        ),
                    )
                    return pl.DataFrame(), True
                seen_reported[fde] = rd

                prev_fde = seen_fde_for_rd.get(rd)
                if prev_fde is not None and prev_fde != fde:
                    fde_drift_count += 1
                    if len(fde_drift_samples) < 3:
                        fde_drift_samples.append(
                            f"reportedDate={rd.isoformat()} fde "
                            f"{prev_fde.isoformat()} vs {fde.isoformat()} "
                            f"(snapshot {src_label})"
                        )
                seen_fde_for_rd[rd] = fde
            merged[fde] = row

    if fde_drift_count:
        report.record(
            symbol, "stocks", frame_label,
            "financials_fiscalDateEnding_offcycle",
            count=fde_drift_count,
            detail=("; ".join(fde_drift_samples))[:500],
        )

    if not merged:
        return pl.DataFrame(), False
    return pl.DataFrame(list(merged.values())), False


# ---------------------------------------------------------------------------
# Latest-snapshot helpers (for estimates and consistency-checked earnings_a)
# ---------------------------------------------------------------------------

def _latest_path(
    snapshots: dict[date, dict[tuple[str, str], Path]],
    historical: dict[tuple[str, str], Path],
    key: tuple[str, str],
) -> Path | None:
    """Return the most recent snapshot's path for *key*, falling back to
    historical when no daily snapshot has it.
    """
    for sd in sorted(snapshots.keys(), reverse=True):
        if key in snapshots[sd]:
            return snapshots[sd][key]
    return historical.get(key)


def _latest_dataframe(
    snapshots: dict[date, dict[tuple[str, str], Path]],
    historical: dict[tuple[str, str], Path],
    key: tuple[str, str],
) -> pl.DataFrame | None:
    return _read_parquet_or_none(_latest_path(snapshots, historical, key))


# ---------------------------------------------------------------------------
# Annual estimate /4 synthesis to extend quarterly estimates
# ---------------------------------------------------------------------------

def _extend_quarterly_estimates_with_annual(
    estimates_q: pl.DataFrame | None,
    estimates_a: pl.DataFrame | None,
) -> pl.DataFrame | None:
    """Merge annual estimates into quarterly estimates by synthesising a
    quarterly row for every annual ``fiscalDateEnding`` that does not
    already have a quarterly entry within +/- ``FISCAL_MATCH_DAYS``.

    Counts and revision fields are copied verbatim; every other numeric
    field is divided by 4.
    """
    if estimates_a is None or estimates_a.is_empty():
        return estimates_q
    if estimates_q is None:
        estimates_q = pl.DataFrame()

    q_lookup = FdeLookup(estimates_q)
    synthesised_rows: list[dict[str, Any]] = []
    for row in estimates_a.iter_rows(named=True):
        fde = row.get("fiscalDateEnding")
        if fde is None:
            continue
        if q_lookup.has_fde_within(fde, FISCAL_MATCH_DAYS):
            continue
        new_row: dict[str, Any] = {"fiscalDateEnding": fde}
        for k, v in row.items():
            if k == "fiscalDateEnding":
                continue
            if v is None or k in _ANNUAL_TO_Q_NO_DIVIDE or not isinstance(v, (int, float)):
                new_row[k] = v
            else:
                new_row[k] = v / 4.0
        synthesised_rows.append(new_row)

    if not synthesised_rows:
        return estimates_q

    if estimates_q.is_empty():
        return pl.DataFrame(synthesised_rows)

    synth_df = pl.DataFrame(synthesised_rows)
    # Align dtypes by extending columns to the union; missing columns
    # in either side become null.
    return pl.concat([estimates_q, synth_df], how="diagonal_relaxed")


# ---------------------------------------------------------------------------
# Report-table construction
# ---------------------------------------------------------------------------

_REPORT_TABLE_SCHEMA: dict[str, Any] = {
    "reportedDate": pl.Date,
    "fiscalDateEnding": pl.Date,
    "reportTime": pl.Utf8,
}


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


# ---------------------------------------------------------------------------
# Snapshot loading + caching
# ---------------------------------------------------------------------------

_STATEMENT_KEYS_Q: tuple[tuple[str, str], ...] = tuple(
    (ep, "_quarterly") for ep in _DATA_ENDPOINTS
)
_STATEMENT_KEYS_A: tuple[tuple[str, str], ...] = tuple(
    (ep, "_annual") for ep in _DATA_ENDPOINTS
)


def _load_snapshot_lookups(
    snapshot_date: date | None,
    snapshots: dict[date, dict[tuple[str, str], Path]],
    historical: dict[tuple[str, str], Path],
) -> dict[tuple[str, str], FdeLookup]:
    """Load FdeLookup for every (statement_endpoint, suffix) at the given
    snapshot. Per-file fallback: if the snapshot folder lacks a particular
    file, fall back to historical's version of that file.
    """
    snap = snapshots.get(snapshot_date, {}) if snapshot_date is not None else {}
    out: dict[tuple[str, str], FdeLookup] = {}
    for key in (*_STATEMENT_KEYS_Q, *_STATEMENT_KEYS_A):
        path = snap.get(key) or historical.get(key)
        out[key] = FdeLookup(_read_parquet_or_none(path))
    return out


# ---------------------------------------------------------------------------
# Cell mapping helpers
# ---------------------------------------------------------------------------

def _lookup_data_at_fde(
    target_fde: date,
    suffix: str,
    snap_lookups: dict[tuple[str, str], FdeLookup],
    fde_offcycle: set[tuple[str, str, date]],
) -> dict[str, Any]:
    """Return {field_name: value-or-None} for the matched fiscalDateEnding
    across the four statement endpoints (IS, BS, CF, E).

    Endpoints are searched in priority order: the FIRST endpoint that has
    a non-null value for a given field wins (so income_statement.netIncome
    overrides cash_flow.netIncome).

    Fields not present in any matched endpoint row are left null.
    fiscalDateEnding mismatches >FISCAL_MATCH_DAYS in any endpoint are
    recorded in the *fde_offcycle* set for later flush.
    """
    out: dict[str, Any] = {}
    for ep in _DATA_ENDPOINTS:
        lookup = snap_lookups.get((ep, suffix))
        if lookup is None or not lookup.sorted_fdes:
            continue
        row, _diff = lookup.find_within(target_fde, FISCAL_MATCH_DAYS)
        if row is None:
            # Only flag as off-cycle when target_fde sits inside the
            # endpoint's covered fde range. If it's older than the
            # earliest fde or newer than the latest, it's plain data
            # absence (e.g. an old quarter that never existed in the
            # endpoint's history), not a restatement.
            if lookup.sorted_fdes[0] <= target_fde <= lookup.sorted_fdes[-1]:
                fde_offcycle.add((ep, suffix, target_fde))
            continue
        for k, v in row.items():
            if k in ("fiscalDateEnding", "reportedDate", "reportTime",
                     "reportedCurrency"):
                continue
            if v is None:
                continue
            if k not in out:
                out[k] = v
    return out


# ---------------------------------------------------------------------------
# build_financials
# ---------------------------------------------------------------------------

def build_financials(
    symbol: str,
    shareprice_daily: pl.DataFrame,
    overview_row: dict | None,
    source_paths: dict[tuple[str, str], list[Path]],
    report: TransformationReport,
    ec_index_for_symbol: dict[date, "EarningsCalendarSnap"] | None = None,
    ec_snap_dates_sorted: list[date] | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build (financials_quarterly, financials_annually) for one stock.

    See ``data_transformation/SPEC.md`` for the full design.

    *overview_row* is the per-symbol row from ``assets_overview.parquet``
    as a dict-like with keys ``reportedDate`` (Date or None) and
    ``timeOfTheDay`` (str). Pass ``None`` if no overview entry exists.

    *ec_index_for_symbol* maps daily snapshot dates to the per-symbol
    ``EarningsCalendarSnap`` from ``daily/<d>/earnings_calendar.parquet``;
    *ec_snap_dates_sorted* is the sorted list of snapshot dates that had a
    calendar file (regardless of whether the symbol had a row in it). Both
    are produced once per run by :func:`_build_earnings_calendar_index` in
    the orchestrator. When omitted, qm0 / am0 fall back to the 14-day
    pre-report rule for every row date.
    """
    if ec_index_for_symbol is None:
        ec_index_for_symbol = {}
    if ec_snap_dates_sorted is None:
        ec_snap_dates_sorted = []
    empty_q = pl.DataFrame(schema=SCHEMAS["financials_quarterly"])
    empty_a = pl.DataFrame(schema=SCHEMAS["financials_annually"])

    if shareprice_daily.is_empty() or "Date" not in shareprice_daily.columns:
        return empty_q, empty_a
    dates: list[date] = shareprice_daily["Date"].to_list()
    if not dates:
        return empty_q, empty_a

    snapshots, historical = _organize_paths(source_paths)
    snapshot_dates_sorted = sorted(snapshots.keys())

    # Need earnings_q somewhere; without it no report_table.
    has_eq = ("earnings", "_quarterly") in historical or any(
        ("earnings", "_quarterly") in snapshots[sd] for sd in snapshots
    )
    if not has_eq:
        report.record(
            symbol, "stocks", "financials_quarterly",
            "financials_no_earnings_file", count=1,
            detail="no earnings/SYMBOL_quarterly.parquet anywhere",
        )
        return empty_q, empty_a

    # All-snapshot earnings union with reportedDate consistency check.
    earnings_q_union, mismatch = _union_earnings_with_consistency(
        symbol, "_quarterly", snapshots, historical, report,
    )
    if mismatch:
        return empty_q, empty_a
    earnings_a_union, mismatch_a = _union_earnings_with_consistency(
        symbol, "_annual", snapshots, historical, report,
    )
    if mismatch_a:
        return empty_q, empty_a

    # Latest extended estimates.
    latest_estimates_q = _latest_dataframe(
        snapshots, historical, ("earnings_estimates", "_quarterly"),
    )
    latest_estimates_a = _latest_dataframe(
        snapshots, historical, ("earnings_estimates", "_annual"),
    )
    estimates_q_extended = _extend_quarterly_estimates_with_annual(
        latest_estimates_q, latest_estimates_a,
    )

    # Report tables.
    report_table = _build_report_table(
        earnings_q_union, overview_row, estimates_q_extended,
    )
    n_known_q = (
        report_table.filter(pl.col("reportedDate").is_not_null()).height
        if not report_table.is_empty() else 0
    )
    rt_rows = report_table.to_dicts() if not report_table.is_empty() else []
    rt_known_rd = [rt_rows[i]["reportedDate"] for i in range(n_known_q)]

    report_table_annual = _build_annual_report_table(
        symbol, earnings_a_union, report_table, latest_estimates_a, report,
    )
    n_known_a = (
        report_table_annual.filter(pl.col("reportedDate").is_not_null()).height
        if not report_table_annual.is_empty() else 0
    )
    rt_a_rows = report_table_annual.to_dicts() if not report_table_annual.is_empty() else []
    rt_a_known_rd = [rt_a_rows[i]["reportedDate"] for i in range(n_known_a)]

    # Estimate lookups.
    est_q_lookup = FdeLookup(estimates_q_extended)
    est_a_lookup = FdeLookup(latest_estimates_a)

    # Per-d row construction with snapshot caching.
    quarterly_rows: list[dict[str, Any]] = []
    annual_rows: list[dict[str, Any]] = []

    cache_sentinel: object = object()
    cached_snap_date: Any = cache_sentinel
    cached_snap_lookups: dict[tuple[str, str], FdeLookup] = {}

    snapshot_fallback: set[date] = set()
    fde_offcycle_q: set[tuple[str, str, date]] = set()
    fde_offcycle_a: set[tuple[str, str, date]] = set()
    estimate_offcycle_q: set[date] = set()
    estimate_offcycle_a: set[date] = set()
    # "Soft" no-anchor: d is past the last known reportedDate but within
    # NO_ANCHOR_TOLERANCE_DAYS, so we keep qm{m>=1} / qp_{n} populated and
    # only the qm0 anchor cells null. "Hard" no-anchor: d is well past
    # everything (or there are no known reportedDates at all), so the row
    # is fully nulled defensively.
    soft_no_anchor_q_count = 0
    soft_no_anchor_a_count = 0
    hard_no_anchor_q_count = 0
    hard_no_anchor_a_count = 0

    latest_known_rd_q = rt_known_rd[-1] if rt_known_rd else None
    latest_known_rd_a = rt_a_known_rd[-1] if rt_a_known_rd else None

    for d in dates:
        snap_date, is_fallback = _resolve_snapshot_date(d, snapshot_dates_sorted)

        if snap_date != cached_snap_date:
            cached_snap_lookups = _load_snapshot_lookups(
                snap_date, snapshots, historical,
            )
            cached_snap_date = snap_date

        if is_fallback and snap_date is not None:
            snapshot_fallback.add(snap_date)

        # Resolve the earnings_calendar snapshot for d (largest snap <= d).
        ec_snap_date = _resolve_ec_snap(d, ec_snap_dates_sorted)
        ec_snap = (
            ec_index_for_symbol.get(ec_snap_date)
            if ec_snap_date is not None
            else None
        )

        # Quarterly row. When no reportedDate > d exists in report_table:
        # - if d is within NO_ANCHOR_TOLERANCE_DAYS of the latest known
        #   reportedDate, fall through with m_anchor = n_known_q so qm0
        #   nulls but qm{m>=1} walks past quarters and qp_{n} can use
        #   future-extension rows;
        # - otherwise null every financials column defensively.
        m_anchor = bisect_right(rt_known_rd, d) if rt_known_rd else 0
        if m_anchor >= n_known_q:
            within_tol = (
                latest_known_rd_q is not None
                and (d - latest_known_rd_q).days < NO_ANCHOR_TOLERANCE_DAYS
            )
            if within_tol:
                soft_no_anchor_q_count += 1
                quarterly_rows.append(_build_quarterly_row(
                    d, n_known_q, n_known_q, rt_rows, len(rt_rows),
                    cached_snap_lookups, est_q_lookup,
                    fde_offcycle_q, estimate_offcycle_q,
                    ec_snap, ec_snap_date,
                ))
            else:
                hard_no_anchor_q_count += 1
                quarterly_rows.append(_build_empty_quarterly_row(d))
        else:
            quarterly_rows.append(_build_quarterly_row(
                d, m_anchor, n_known_q, rt_rows, len(rt_rows),
                cached_snap_lookups, est_q_lookup,
                fde_offcycle_q, estimate_offcycle_q,
                ec_snap, ec_snap_date,
            ))

        # Annual row. Same two-tier rule on the annual axis.
        m_anchor_a = bisect_right(rt_a_known_rd, d) if rt_a_known_rd else 0
        if m_anchor_a >= n_known_a:
            within_tol_a = (
                latest_known_rd_a is not None
                and (d - latest_known_rd_a).days < NO_ANCHOR_TOLERANCE_DAYS
            )
            if within_tol_a:
                soft_no_anchor_a_count += 1
                annual_rows.append(_build_annual_row(
                    d, n_known_a, n_known_a, rt_a_rows, len(rt_a_rows),
                    cached_snap_lookups, est_a_lookup,
                    fde_offcycle_a, estimate_offcycle_a,
                    ec_snap, ec_snap_date,
                ))
            else:
                hard_no_anchor_a_count += 1
                annual_rows.append(_build_empty_annual_row(d))
        else:
            annual_rows.append(_build_annual_row(
                d, m_anchor_a, n_known_a, rt_a_rows, len(rt_a_rows),
                cached_snap_lookups, est_a_lookup,
                fde_offcycle_a, estimate_offcycle_a,
                ec_snap, ec_snap_date,
            ))

    # Flush accumulated issue logs. Soft no-anchor rows are common when
    # assets_overview lacks an upcoming reportedDate, so they go to info;
    # hard no-anchor rows still warn (d is genuinely stale).
    if soft_no_anchor_q_count or soft_no_anchor_a_count:
        logger.info(
            "stocks/%s: %d quarterly / %d annual row(s) had no upcoming "
            "reportedDate within %d days; qm0/am0 nulled, past quarters "
            "and estimates kept.",
            symbol, soft_no_anchor_q_count, soft_no_anchor_a_count,
            NO_ANCHOR_TOLERANCE_DAYS,
        )
    if hard_no_anchor_q_count or hard_no_anchor_a_count:
        logger.info(
            "stocks/%s: %d quarterly / %d annual row(s) had no m_anchor "
            "and d was beyond the %d-day tolerance from the latest known "
            "reportedDate; financials columns nulled defensively.",
            symbol, hard_no_anchor_q_count, hard_no_anchor_a_count,
            NO_ANCHOR_TOLERANCE_DAYS,
        )
    if snapshot_fallback:
        report.record(
            symbol, "stocks", "financials_quarterly",
            "financials_snapshot_fallback",
            count=len(snapshot_fallback),
            detail=(
                f"{len(snapshot_fallback)} distinct snapshots used as "
                f"fallback for one or more row dates"
            ),
        )
    for offcycle, frame_label in (
        (fde_offcycle_q, "financials_quarterly"),
        (fde_offcycle_a, "financials_annually"),
    ):
        if offcycle:
            report.record(
                symbol, "stocks", frame_label,
                "financials_fiscalDateEnding_offcycle",
                count=len(offcycle),
                detail=(
                    f"{len(offcycle)} distinct (endpoint, fde) anchors had no "
                    f"row within {FISCAL_MATCH_DAYS} days in some d-PIT snapshot"
                ),
            )
    for offcycle, frame_label in (
        (estimate_offcycle_q, "financials_quarterly"),
        (estimate_offcycle_a, "financials_annually"),
    ):
        if offcycle:
            report.record(
                symbol, "stocks", frame_label,
                "financials_estimate_offcycle",
                count=len(offcycle),
                detail=(
                    f"{len(offcycle)} anchor fiscalDateEndings had no "
                    f"estimates row within {FISCAL_MATCH_DAYS} days"
                ),
            )

    q_df = pl.DataFrame(
        quarterly_rows, schema=SCHEMAS["financials_quarterly"],
    )
    a_df = pl.DataFrame(
        annual_rows, schema=SCHEMAS["financials_annually"],
    )
    return q_df, a_df


def _build_empty_quarterly_row(d: date) -> dict[str, Any]:
    """All-null quarterly row for d. Used when m_anchor cannot be
    resolved (no reportedDate in report_table >= d). Per the SPEC,
    every financials column on this row is nulled defensively.
    """
    row: dict[str, Any] = {"Date": d}
    row["days_to_fiscalDateEnding_qm0"] = None
    row["days_to_reportedDate_qm0"] = None
    row["reportTime_qm0"] = None
    for m in _QM_MS:
        row[f"days_to_fiscalDateEnding_qm{m}"] = None
        row[f"days_to_reportedDate_qm{m}"] = None
        row[f"reportTime_qm{m}"] = None
        for fld_name, _t in _QM_DATA_FIELDS:
            row[f"{fld_name}_qm{m}"] = None
    for n in _QP_NS:
        suffix = _signed_suffix(n)
        for fld_name, _t in _QP_DATA_FIELDS:
            row[f"{fld_name}_qp_{suffix}"] = None
    return row


def _build_empty_annual_row(d: date) -> dict[str, Any]:
    """All-null annual row for d. Same defensive rule as quarterly."""
    row: dict[str, Any] = {"Date": d}
    row["days_to_fiscalDateEnding_am0"] = None
    row["days_to_reportedDate_am0"] = None
    for m in _AM_MS:
        row[f"days_to_fiscalDateEnding_am{m}"] = None
        row[f"days_to_reportedDate_am{m}"] = None
        for fld_name, _t in _AM_DATA_FIELDS:
            row[f"{fld_name}_am{m}"] = None
    for n in _AP_NS:
        suffix = _signed_suffix(n)
        for fld_name, _t in _AP_DATA_FIELDS:
            row[f"{fld_name}_ap_{suffix}"] = None
    return row


def _build_quarterly_row(
    d: date,
    m_anchor: int,
    n_known: int,
    rt_rows: list[dict[str, Any]],
    rt_len: int,
    snap_lookups: dict[tuple[str, str], FdeLookup],
    est_q_lookup: FdeLookup,
    fde_offcycle: set[tuple[str, str, date]],
    estimate_offcycle: set[date],
    ec_snap: "EarningsCalendarSnap | None",
    ec_snap_date: date | None,
) -> dict[str, Any]:
    row: dict[str, Any] = {"Date": d}

    # m=0 anchor cells are PIT-gated. Three branches, in order:
    # 1. earnings_calendar snapshot exists at or before d (ec_snap_date is
    #    not None): the snapshot decides. If the symbol has a row whose
    #    largest reportedDate > d, populate qm0 from that row's PIT-honest
    #    values (reportedDate, fiscalDateEnding, timeOfTheDay). Otherwise
    #    null qm0 -- at d the next earnings date was not yet announced.
    # 2. No earnings_calendar snapshot at all for d (historical regime):
    #    apply the 14-day pre-report rule using rt_rows[pos0]. Populate
    #    qm0 only when the eventually-filed reportedDate is within 14 days
    #    of d.
    # 3. Otherwise (pos0 out of range and no calendar): null.
    pos0 = m_anchor
    qm0_anchor: dict[str, Any] | None = None
    if ec_snap_date is not None:
        if ec_snap is not None and ec_snap.max_rd_row is not None:
            best_rd = ec_snap.max_rd_row["reportedDate"]
            if best_rd is not None and best_rd > d:
                qm0_anchor = ec_snap.max_rd_row
    elif 0 <= pos0 < n_known:
        ar = rt_rows[pos0]
        rd = ar["reportedDate"]
        if rd is not None and 1 <= (rd - d).days <= NO_EC_PRE_REPORT_DAYS:
            qm0_anchor = ar

    if qm0_anchor is not None:
        fde0 = qm0_anchor.get("fiscalDateEnding")
        rd0 = qm0_anchor.get("reportedDate")
        # earnings_calendar rows expose ``timeOfTheDay``; rt_rows expose
        # ``reportTime`` (already normalised). Prefer the calendar field
        # when present.
        tod0 = qm0_anchor.get("timeOfTheDay")
        rt0 = _normalize_report_time(tod0) if tod0 is not None else qm0_anchor.get("reportTime")
        row["days_to_fiscalDateEnding_qm0"] = (
            float((d - fde0).days) if fde0 is not None else None
        )
        row["days_to_reportedDate_qm0"] = (
            float((d - rd0).days) if rd0 is not None else None
        )
        row["reportTime_qm0"] = rt0
    else:
        row["days_to_fiscalDateEnding_qm0"] = None
        row["days_to_reportedDate_qm0"] = None
        row["reportTime_qm0"] = None

    # m=1..16: anchor + data fields.
    for m in _QM_MS:
        pos = m_anchor - m
        if 0 <= pos < n_known:
            ar = rt_rows[pos]
            f_m = ar["fiscalDateEnding"]
            row[f"days_to_fiscalDateEnding_qm{m}"] = float((d - f_m).days)
            row[f"days_to_reportedDate_qm{m}"] = float((d - ar["reportedDate"]).days)
            row[f"reportTime_qm{m}"] = ar["reportTime"]
            data = _lookup_data_at_fde(
                f_m, "_quarterly", snap_lookups, fde_offcycle,
            )
            for fld_name, _t in _QM_DATA_FIELDS:
                row[f"{fld_name}_qm{m}"] = data.get(fld_name)
        else:
            row[f"days_to_fiscalDateEnding_qm{m}"] = None
            row[f"days_to_reportedDate_qm{m}"] = None
            row[f"reportTime_qm{m}"] = None
            for fld_name, _t in _QM_DATA_FIELDS:
                row[f"{fld_name}_qm{m}"] = None

    # n axis: estimates.
    for n in _QP_NS:
        suffix = _signed_suffix(n)
        pos = m_anchor + n
        if 0 <= pos < rt_len:
            ar = rt_rows[pos]
            f_n = ar["fiscalDateEnding"]
            est_row, _days_diff = est_q_lookup.find_within(f_n, FISCAL_MATCH_DAYS)
            if est_row is not None:
                for fld_name, _t in _QP_DATA_FIELDS:
                    row[f"{fld_name}_qp_{suffix}"] = est_row.get(fld_name)
            else:
                if (
                    est_q_lookup.sorted_fdes
                    and est_q_lookup.sorted_fdes[0] <= f_n <= est_q_lookup.sorted_fdes[-1]
                ):
                    estimate_offcycle.add(f_n)
                for fld_name, _t in _QP_DATA_FIELDS:
                    row[f"{fld_name}_qp_{suffix}"] = None
        else:
            for fld_name, _t in _QP_DATA_FIELDS:
                row[f"{fld_name}_qp_{suffix}"] = None

    return row


def _build_annual_row(
    d: date,
    m_anchor: int,
    n_known: int,
    rt_rows: list[dict[str, Any]],
    rt_len: int,
    snap_lookups: dict[tuple[str, str], FdeLookup],
    est_a_lookup: FdeLookup,
    fde_offcycle: set[tuple[str, str, date]],
    estimate_offcycle: set[date],
    ec_snap: "EarningsCalendarSnap | None",
    ec_snap_date: date | None,
) -> dict[str, Any]:
    row: dict[str, Any] = {"Date": d}

    # am=0 anchor cells are PIT-gated. Mirrors the quarterly logic but
    # matches earnings_calendar rows by fiscalDateEnding against the
    # annual anchor's fde -- am0 represents the next-upcoming *annual*
    # filing, and the calendar row whose fde lines up with that fde is
    # the announcement we care about. When no calendar exists for d the
    # 14-day pre-report rule on rt_rows[pos0] applies.
    pos0 = m_anchor
    pos0_in_range = 0 <= pos0 < n_known
    am0_anchor: dict[str, Any] | None = None
    if ec_snap_date is not None:
        if pos0_in_range and ec_snap is not None:
            target_fde = rt_rows[pos0]["fiscalDateEnding"]
            ec_row, _diff = ec_snap.fde_lookup.find_within(
                target_fde, FISCAL_MATCH_DAYS,
            )
            if ec_row is not None:
                ec_rd = ec_row.get("reportedDate")
                if ec_rd is not None and ec_rd > d:
                    am0_anchor = ec_row
    elif pos0_in_range:
        ar = rt_rows[pos0]
        rd = ar["reportedDate"]
        if rd is not None and 1 <= (rd - d).days <= NO_EC_PRE_REPORT_DAYS:
            am0_anchor = ar

    if am0_anchor is not None:
        fde0 = am0_anchor.get("fiscalDateEnding")
        rd0 = am0_anchor.get("reportedDate")
        row["days_to_fiscalDateEnding_am0"] = (
            float((d - fde0).days) if fde0 is not None else None
        )
        row["days_to_reportedDate_am0"] = (
            float((d - rd0).days) if rd0 is not None else None
        )
    else:
        row["days_to_fiscalDateEnding_am0"] = None
        row["days_to_reportedDate_am0"] = None

    # am=1..4: anchor + data fields.
    for m in _AM_MS:
        pos = m_anchor - m
        if 0 <= pos < n_known:
            ar = rt_rows[pos]
            f_m = ar["fiscalDateEnding"]
            row[f"days_to_fiscalDateEnding_am{m}"] = float((d - f_m).days)
            row[f"days_to_reportedDate_am{m}"] = float((d - ar["reportedDate"]).days)
            data = _lookup_data_at_fde(
                f_m, "_annual", snap_lookups, fde_offcycle,
            )
            for fld_name, _t in _AM_DATA_FIELDS:
                row[f"{fld_name}_am{m}"] = data.get(fld_name)
        else:
            row[f"days_to_fiscalDateEnding_am{m}"] = None
            row[f"days_to_reportedDate_am{m}"] = None
            for fld_name, _t in _AM_DATA_FIELDS:
                row[f"{fld_name}_am{m}"] = None

    # n axis: annual estimates.
    for n in _AP_NS:
        suffix = _signed_suffix(n)
        pos = m_anchor + n
        if 0 <= pos < rt_len:
            ar = rt_rows[pos]
            f_n = ar["fiscalDateEnding"]
            est_row, _days_diff = est_a_lookup.find_within(f_n, FISCAL_MATCH_DAYS)
            if est_row is not None:
                for fld_name, _t in _AP_DATA_FIELDS:
                    row[f"{fld_name}_ap_{suffix}"] = est_row.get(fld_name)
            else:
                if (
                    est_a_lookup.sorted_fdes
                    and est_a_lookup.sorted_fdes[0] <= f_n <= est_a_lookup.sorted_fdes[-1]
                ):
                    estimate_offcycle.add(f_n)
                for fld_name, _t in _AP_DATA_FIELDS:
                    row[f"{fld_name}_ap_{suffix}"] = None
        else:
            for fld_name, _t in _AP_DATA_FIELDS:
                row[f"{fld_name}_ap_{suffix}"] = None

    return row
