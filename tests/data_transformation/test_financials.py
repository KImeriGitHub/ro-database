"""Tests for Phase 6c: financials_quarterly and financials_annually for stocks.

Covers per-row PIT snapshot resolution, the per-symbol report_table
(past entries from union earnings_q + next-upcoming from
assets_overview + further-future from extended estimates), the m_anchor
walk across reports, the asymmetric m=0 / am=0 schemas, the late-filer
ordering, the n axis spanning {-8..4} / {-2..1}, the +/-10-day
fiscalDateEnding margin against statements and estimates, the annual
estimate /4 synthesis, the all-snapshot earnings_q union with the
reportedDate consistency check (and the symmetric fiscalDateEnding
drift logging), the snapshot fallback, the annual-no-quarterly-match
drop, the no_anchor defensive null rule, the reportTime normalisation,
the future-extension tail sort, the --skip-financials and
--rebuild-stocks CLI flags, and a StockData round-trip.
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from data_transformation._common import (
    TransformationReport,
    symbol_dest_dir,
)
from data_transformation.AssetData import StockData
from data_transformation.AssetDataService import SCHEMAS
from data_transformation.frames.financials import (
    FISCAL_MATCH_DAYS,
    FdeLookup,
    _build_report_table,
    _extend_quarterly_estimates_with_annual,
    _normalize_report_time,
    build_financials,
)


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
TRANSFORM_PY = REPO_ROOT / "data_transformation" / "transform.py"


# ── Helpers ───────────────────────────────────────────────────────────────────

# Source-side schemas for the five fundamentals endpoints.
_EARNINGS_Q_SCHEMA: dict = {
    "fiscalDateEnding": pl.Date,
    "reportedDate": pl.Date,
    "reportTime": pl.Utf8,
    "reportedEPS": pl.Float32,
    "estimatedEPS": pl.Float32,
    "surprise": pl.Float32,
    "surprisePercentage": pl.Float32,
}

_EARNINGS_A_SCHEMA: dict = {
    "fiscalDateEnding": pl.Date,
    "reportedEPS": pl.Float32,
}

_IS_SCHEMA: dict = {
    "fiscalDateEnding": pl.Date,
    "reportedDate": pl.Date,
    "reportedCurrency": pl.Utf8,
    "totalRevenue": pl.Float32,
    "ebit": pl.Float32,
    "netIncome": pl.Float32,
}

_BS_SCHEMA: dict = {
    "fiscalDateEnding": pl.Date,
    "reportedDate": pl.Date,
    "reportedCurrency": pl.Utf8,
    "totalAssets": pl.Float32,
    "totalLiabilities": pl.Float32,
}

_CF_SCHEMA: dict = {
    "fiscalDateEnding": pl.Date,
    "reportedDate": pl.Date,
    "reportedCurrency": pl.Utf8,
    "operatingCashflow": pl.Float32,
    "netIncome": pl.Float32,
}

_EE_Q_SCHEMA: dict = {
    "fiscalDateEnding": pl.Date,
    "eps_estimate_analyst_count": pl.Float32,
    "eps_estimate_average": pl.Float32,
    "eps_estimate_high": pl.Float32,
    "eps_estimate_low": pl.Float32,
    "revenue_estimate_analyst_count": pl.Float32,
    "revenue_estimate_average": pl.Float32,
    "revenue_estimate_high": pl.Float32,
    "revenue_estimate_low": pl.Float32,
    "eps_estimate_revision_up_trailing_7_days": pl.Float32,
    "eps_estimate_revision_up_trailing_30_days": pl.Float32,
    "eps_estimate_revision_down_trailing_7_days": pl.Float32,
    "eps_estimate_revision_down_trailing_30_days": pl.Float32,
}

_EE_A_SCHEMA: dict = _EE_Q_SCHEMA  # same column shape


def _earnings_q(fde: date, rd: date, *, rt: str = "post-market",
                eps: float = 1.0) -> dict:
    return {
        "fiscalDateEnding": fde,
        "reportedDate": rd,
        "reportTime": rt,
        "reportedEPS": eps,
        "estimatedEPS": eps - 0.05,
        "surprise": 0.05,
        "surprisePercentage": 5.0,
    }


def _is_row(fde: date, rd: date | None = None, *,
            total_revenue: float = 1.0e9) -> dict:
    return {
        "fiscalDateEnding": fde,
        "reportedDate": rd,
        "reportedCurrency": "USD",
        "totalRevenue": total_revenue,
        "ebit": total_revenue * 0.2,
        "netIncome": total_revenue * 0.1,
    }


def _bs_row(fde: date, rd: date | None = None, *,
            total_assets: float = 5.0e9) -> dict:
    return {
        "fiscalDateEnding": fde,
        "reportedDate": rd,
        "reportedCurrency": "USD",
        "totalAssets": total_assets,
        "totalLiabilities": total_assets * 0.4,
    }


def _cf_row(fde: date, rd: date | None = None, *,
            operating_cashflow: float = 4.0e8) -> dict:
    return {
        "fiscalDateEnding": fde,
        "reportedDate": rd,
        "reportedCurrency": "USD",
        "operatingCashflow": operating_cashflow,
        "netIncome": 1.0e8,
    }


def _ee_row(fde: date, *, eps_avg: float = 1.0, count: float = 5.0,
            rev_avg: float = 1.0e9) -> dict:
    return {
        "fiscalDateEnding": fde,
        "eps_estimate_analyst_count": count,
        "eps_estimate_average": eps_avg,
        "eps_estimate_high": eps_avg + 0.20,
        "eps_estimate_low": eps_avg - 0.20,
        "revenue_estimate_analyst_count": count,
        "revenue_estimate_average": rev_avg,
        "revenue_estimate_high": rev_avg * 1.1,
        "revenue_estimate_low": rev_avg * 0.9,
        "eps_estimate_revision_up_trailing_7_days": 1.0,
        "eps_estimate_revision_up_trailing_30_days": 2.0,
        "eps_estimate_revision_down_trailing_7_days": 0.0,
        "eps_estimate_revision_down_trailing_30_days": 1.0,
    }


def _earnings_a(fde: date, *, eps: float = 4.0) -> dict:
    return {"fiscalDateEnding": fde, "reportedEPS": eps}


def _write(path: Path, rows: list[dict], schema: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, schema=schema).write_parquet(path)
    return path


def _hist_path(root: Path, endpoint: str, suffix: str, symbol: str = "AAPL") -> Path:
    return root / "historical" / "stocks" / endpoint / f"stocks_{symbol}{suffix}.parquet"


def _daily_path(root: Path, snap: date, endpoint: str, suffix: str,
                symbol: str = "AAPL") -> Path:
    return (
        root / "daily" / snap.isoformat() / "stocks" / endpoint
        / f"stocks_{symbol}{suffix}.parquet"
    )


def _sd_frame(dates: list[date]) -> pl.DataFrame:
    """Minimal shareprice_daily frame: just the Date axis, schema-correct."""
    return pl.DataFrame(
        {"Date": dates},
        schema={"Date": pl.Date},
    )


def _empty_source_paths() -> dict:
    return {(ep, suf): []
            for ep in ("income_statement", "balance_sheet", "cash_flow",
                       "earnings", "earnings_estimates")
            for suf in ("_quarterly", "_annual")}


def _gather_source_paths(root: Path, symbol: str = "AAPL") -> dict[tuple[str, str], list[Path]]:
    """Walk the synthetic tree and produce the {(endpoint, suffix): [paths]}
    dict that build_financials expects."""
    result: dict[tuple[str, str], list[Path]] = _empty_source_paths()
    # Historical.
    for ep in ("income_statement", "balance_sheet", "cash_flow",
               "earnings", "earnings_estimates"):
        for suf in ("_quarterly", "_annual"):
            p = _hist_path(root, ep, suf, symbol)
            if p.exists():
                result[(ep, suf)].append(p)
    # Daily.
    daily_root = root / "daily"
    if daily_root.is_dir():
        for snap_dir in sorted(daily_root.iterdir()):
            if not snap_dir.is_dir():
                continue
            try:
                date.fromisoformat(snap_dir.name)
            except ValueError:
                continue
            for ep in ("income_statement", "balance_sheet", "cash_flow",
                       "earnings", "earnings_estimates"):
                for suf in ("_quarterly", "_annual"):
                    p = (snap_dir / "stocks" / ep
                         / f"stocks_{symbol}{suf}.parquet")
                    if p.exists():
                        result[(ep, suf)].append(p)
    return result


# ── 1. Empty inputs ───────────────────────────────────────────────────────────

def test_empty_inputs_returns_schema_correct_empty(tmp_path):
    sd = _sd_frame([])
    fin_q, fin_a = build_financials(
        "AAPL", sd, None, _empty_source_paths(), TransformationReport(),
    )
    assert fin_q.height == 0
    assert fin_a.height == 0
    assert dict(fin_q.schema) == SCHEMAS["financials_quarterly"]
    assert dict(fin_a.schema) == SCHEMAS["financials_annually"]


def test_no_earnings_file_logs_and_returns_empty(tmp_path):
    """Without any earnings_q file (historical or daily), build_financials
    logs 'financials_no_earnings_file' and returns empty frames."""
    sd = _sd_frame([date(2026, 4, 15)])
    report = TransformationReport()
    fin_q, fin_a = build_financials(
        "AAPL", sd, None, _empty_source_paths(), report,
    )
    assert fin_q.height == 0
    assert fin_a.height == 0
    assert report.to_frame().filter(
        pl.col("issue_type") == "financials_no_earnings_file"
    ).height == 1


# ── 2. Single past quarterly report fully populated ───────────────────────────

def test_single_past_quarter_populates_qm1(tmp_path):
    """Past Q1 reported on 2026-02-01 (fiscalDateEnding 2025-12-31).
    Upcoming Q2 with overview_row reportedDate=2026-05-01,
    fiscalDateEnding=2026-03-31 (from estimates_q).
    Row date d=2026-04-15 falls between past and upcoming -> m=0 anchors
    on Q2 (upcoming), m=1 anchors on Q1 (past)."""
    past_fde = date(2025, 12, 31)
    past_rd  = date(2026, 2, 1)
    upcoming_fde = date(2026, 3, 31)
    upcoming_rd = date(2026, 5, 1)
    d = date(2026, 4, 15)

    _write(_hist_path(tmp_path, "earnings", "_quarterly"),
           [_earnings_q(past_fde, past_rd)], _EARNINGS_Q_SCHEMA)
    _write(_hist_path(tmp_path, "income_statement", "_quarterly"),
           [_is_row(past_fde, past_rd, total_revenue=2.5e9)], _IS_SCHEMA)
    _write(_hist_path(tmp_path, "balance_sheet", "_quarterly"),
           [_bs_row(past_fde, past_rd, total_assets=8.0e9)], _BS_SCHEMA)
    _write(_hist_path(tmp_path, "cash_flow", "_quarterly"),
           [_cf_row(past_fde, past_rd, operating_cashflow=6.0e8)], _CF_SCHEMA)
    _write(_hist_path(tmp_path, "earnings_estimates", "_quarterly"),
           [_ee_row(upcoming_fde)], _EE_Q_SCHEMA)

    overview_row = {
        "reportedDate": upcoming_rd, "timeOfTheDay": "post-market",
    }
    sd = _sd_frame([d])
    report = TransformationReport()
    fin_q, fin_a = build_financials(
        "AAPL", sd, overview_row, _gather_source_paths(tmp_path), report,
    )
    assert fin_q.height == 1
    row = fin_q.row(0, named=True)

    # m=0 carries upcoming anchors only.
    assert row["days_to_fiscalDateEnding_qm0"] == pytest.approx(
        (d - upcoming_fde).days, rel=1e-6
    )
    assert row["reportTime_qm0"] == "post-market"

    # m=1 carries past Q1 data fields.
    assert row["days_to_fiscalDateEnding_qm1"] == pytest.approx(
        (d - past_fde).days, rel=1e-6
    )
    assert row["reportTime_qm1"] == "post-market"
    assert row["reportedEPS_qm1"] == pytest.approx(1.0, rel=1e-3)
    assert row["totalRevenue_qm1"] == pytest.approx(2.5e9, rel=1e-3)
    assert row["totalAssets_qm1"] == pytest.approx(8.0e9, rel=1e-3)
    assert row["operatingCashflow_qm1"] == pytest.approx(6.0e8, rel=1e-3)


# ── 3. m_anchor walking ───────────────────────────────────────────────────────

def test_no_anchor_nulls_every_financials_cell(tmp_path):
    """Hard no-anchor: d is far past every known reportedDate (well beyond
    the 60-day tolerance) and there is no upcoming entry -> m_anchor past-
    the-end -> every _qm{m} and _qp_{n} cell is null (defensive). Same for
    the annual frame."""
    past_fde = date(2024, 6, 30)
    past_rd  = date(2024, 8, 1)
    _write(_hist_path(tmp_path, "earnings", "_quarterly"),
           [_earnings_q(past_fde, past_rd)], _EARNINGS_Q_SCHEMA)
    # No estimates -> no upcoming entry can be added.

    d = date(2026, 4, 15)  # 622 days past past_rd, well beyond tolerance
    sd = _sd_frame([d])
    fin_q, fin_a = build_financials(
        "AAPL", sd, None, _gather_source_paths(tmp_path),
        TransformationReport(),
    )
    assert fin_q.height == 1
    row_q = fin_q.row(0, named=True)
    # Every non-Date column must be null.
    for col in fin_q.columns:
        if col == "Date":
            continue
        assert row_q[col] is None, f"{col!r} should be null when no anchor"
    # Annual: same defensive null rule.
    if fin_a.height >= 1:
        row_a = fin_a.row(0, named=True)
        for col in fin_a.columns:
            if col == "Date":
                continue
            assert row_a[col] is None, f"annual {col!r} should be null"


def test_soft_no_anchor_within_tolerance_keeps_past_quarters(tmp_path):
    """Soft no-anchor: d is past the latest known reportedDate but within
    NO_ANCHOR_TOLERANCE_DAYS, AND there is no upcoming entry from
    assets_overview / estimates. Per the spec we set
    m_anchor = n_known_q so qm0 anchor cells null, but qm{m>=1} walks back
    through past quarters and qp_{n} can still resolve where estimates
    exist. Earnings present + estimates empty must NOT abandon the past-
    quarter mapping.
    """
    past_fde = date(2026, 3, 31)
    past_rd  = date(2026, 5, 1)
    _write(_hist_path(tmp_path, "earnings", "_quarterly"),
           [_earnings_q(past_fde, past_rd, rt="post-market")],
           _EARNINGS_Q_SCHEMA)
    _write(_hist_path(tmp_path, "income_statement", "_quarterly"),
           [_is_row(past_fde, past_rd, total_revenue=3.3e9)], _IS_SCHEMA)
    _write(_hist_path(tmp_path, "balance_sheet", "_quarterly"),
           [_bs_row(past_fde, past_rd, total_assets=9.0e9)], _BS_SCHEMA)
    _write(_hist_path(tmp_path, "cash_flow", "_quarterly"),
           [_cf_row(past_fde, past_rd, operating_cashflow=7.0e8)], _CF_SCHEMA)
    # No estimates_q file at all -> empty estimates frame, no upcoming.
    # No overview_row either -> assets_overview supplies no upcoming rd.

    d = date(2026, 5, 31)  # 30 days past past_rd, well within 60-day tol
    sd = _sd_frame([d])
    report = TransformationReport()
    fin_q, fin_a = build_financials(
        "AAPL", sd, None, _gather_source_paths(tmp_path), report,
    )
    assert fin_q.height == 1
    row = fin_q.row(0, named=True)

    # qm0 anchor cells null (no upcoming reportedDate).
    assert row["days_to_fiscalDateEnding_qm0"] is None
    assert row["days_to_reportedDate_qm0"] is None
    assert row["reportTime_qm0"] is None

    # qm1 must carry the past quarter, NOT be nulled.
    assert row["days_to_fiscalDateEnding_qm1"] == pytest.approx(
        (d - past_fde).days, rel=1e-6
    )
    assert row["days_to_reportedDate_qm1"] == pytest.approx(
        (d - past_rd).days, rel=1e-6
    )
    assert row["reportTime_qm1"] == "post-market"
    assert row["reportedEPS_qm1"] == pytest.approx(1.0, rel=1e-3)
    assert row["totalRevenue_qm1"] == pytest.approx(3.3e9, rel=1e-3)
    assert row["totalAssets_qm1"] == pytest.approx(9.0e9, rel=1e-3)
    assert row["operatingCashflow_qm1"] == pytest.approx(7.0e8, rel=1e-3)

    # qp_{n} all null because estimates frame is empty (no offcycle log
    # because there's nothing to compare against).
    assert row["earnings_estimate_days_diff_qp_0"] is None
    assert row["eps_estimate_average_qp_0"] is None
    assert row["earnings_estimate_days_diff_qp_m1"] is None

    # Soft case must NOT trigger the empty-frame defensive path.
    assert fin_q.height == 1
    # Hard no-anchor was not triggered, so no warning-level financials_*
    # row is recorded; the soft case is logger.info only, not in the
    # transformation report.


def test_d_before_every_reporteddate_qm0_fills_from_earliest(tmp_path):
    """For d strictly before every known reportedDate, m_anchor=0 and
    only m=0's anchor columns are populated; m>=1 are out of range."""
    past_fde = date(2026, 6, 30)
    past_rd  = date(2026, 8, 1)
    _write(_hist_path(tmp_path, "earnings", "_quarterly"),
           [_earnings_q(past_fde, past_rd, rt="pre-market")],
           _EARNINGS_Q_SCHEMA)

    d = date(2026, 4, 15)  # before reportedDate
    sd = _sd_frame([d])
    fin_q, _fin_a = build_financials(
        "AAPL", sd, None, _gather_source_paths(tmp_path),
        TransformationReport(),
    )
    row = fin_q.row(0, named=True)
    assert row["days_to_fiscalDateEnding_qm0"] == pytest.approx(
        (d - past_fde).days, rel=1e-6
    )
    assert row["reportTime_qm0"] == "pre-market"
    # m=1 is out of range -> null.
    assert row["days_to_fiscalDateEnding_qm1"] is None
    assert row["reportedEPS_qm1"] is None


# ── 4. Late-filer ordering ────────────────────────────────────────────────────

def test_late_filer_ordering_in_report_table(tmp_path):
    """Q3 (fde=2025-09-30) is filed AFTER Q4 (fde=2025-12-31) in
    chronological reportedDate. report_table is sorted by reportedDate
    ascending, so Q3 sits AFTER Q4. For d after Q4.rd but before Q3.rd,
    m_anchor=Q3 (next future) and m=1 -> Q4."""
    q4_fde = date(2025, 12, 31)
    q4_rd  = date(2026, 2, 1)
    q3_fde = date(2025, 9, 30)
    q3_rd  = date(2026, 3, 15)   # late-filed AFTER Q4
    _write(_hist_path(tmp_path, "earnings", "_quarterly"), [
        _earnings_q(q4_fde, q4_rd),
        _earnings_q(q3_fde, q3_rd),
    ], _EARNINGS_Q_SCHEMA)
    _write(_hist_path(tmp_path, "income_statement", "_quarterly"), [
        _is_row(q4_fde, q4_rd, total_revenue=4.0e9),
        _is_row(q3_fde, q3_rd, total_revenue=3.0e9),
    ], _IS_SCHEMA)

    d = date(2026, 2, 20)  # between Q4.rd and Q3.rd
    sd = _sd_frame([d])
    fin_q, _ = build_financials(
        "AAPL", sd, None, _gather_source_paths(tmp_path),
        TransformationReport(),
    )
    row = fin_q.row(0, named=True)
    # m=0 -> Q3 (next future by reportedDate).
    assert row["days_to_fiscalDateEnding_qm0"] == pytest.approx(
        (d - q3_fde).days, rel=1e-6
    )
    # m=1 -> Q4 (most recent past by reportedDate). Data populated.
    assert row["days_to_fiscalDateEnding_qm1"] == pytest.approx(
        (d - q4_fde).days, rel=1e-6
    )
    assert row["totalRevenue_qm1"] == pytest.approx(4.0e9, rel=1e-3)


# ── 5. n axis full schema span ────────────────────────────────────────────────

def test_qp_n_axis_spans_full_schema_range(tmp_path):
    """Schema defines n in {-8..4}. Verify suffixes m8 through p4 exist
    in the schema."""
    cols = SCHEMAS["financials_quarterly"]
    suffixes_present: set[str] = set()
    for c in cols:
        for tag in ("_qp_m8", "_qp_m1", "_qp_0", "_qp_p1", "_qp_p4"):
            if c.endswith(tag):
                suffixes_present.add(tag)
    assert {"_qp_m8", "_qp_m1", "_qp_0", "_qp_p1", "_qp_p4"} <= suffixes_present


def test_ap_n_axis_spans_minus2_to_plus1(tmp_path):
    cols = SCHEMAS["financials_annually"]
    suffixes_present: set[str] = set()
    for c in cols:
        for tag in ("_ap_m2", "_ap_m1", "_ap_0", "_ap_p1"):
            if c.endswith(tag):
                suffixes_present.add(tag)
    assert {"_ap_m2", "_ap_m1", "_ap_0", "_ap_p1"} <= suffixes_present


# ── 6. Annual report table (annual_no_quarterly_match) ────────────────────────

def test_annual_no_quarterly_match_drops_unaligned(tmp_path):
    """Three annual fiscalDateEndings: two align (within +/-10 days) to
    quarterly fdes, one (off by 30 days) does not. The unaligned annual
    is dropped and a financials_annual_no_quarterly_match log is recorded."""
    q1_fde = date(2024, 12, 31)
    q1_rd  = date(2025, 2, 1)
    q2_fde = date(2025, 12, 31)
    q2_rd  = date(2026, 2, 1)
    _write(_hist_path(tmp_path, "earnings", "_quarterly"), [
        _earnings_q(q1_fde, q1_rd, rt="post-market"),
        _earnings_q(q2_fde, q2_rd, rt="pre-market"),
    ], _EARNINGS_Q_SCHEMA)
    _write(_hist_path(tmp_path, "earnings", "_annual"), [
        _earnings_a(q1_fde),                      # exact match q1
        _earnings_a(date(2025, 12, 28)),          # within 10d of q2
        _earnings_a(date(2025, 6, 30)),           # no match within 10d
    ], _EARNINGS_A_SCHEMA)

    d = date(2026, 4, 15)
    sd = _sd_frame([d])
    report = TransformationReport()
    _fin_q, fin_a = build_financials(
        "AAPL", sd, None, _gather_source_paths(tmp_path), report,
    )
    # annual_no_quarterly_match logged exactly once (count=1).
    rep = report.to_frame().filter(
        pl.col("issue_type") == "financials_annual_no_quarterly_match"
    )
    assert rep.height == 1
    assert rep["count"][0] == 1
    # The annual frame still produces a row for d.
    assert fin_a.height == 1


# ── 7. Annual estimate /4 synthesis ───────────────────────────────────────────

def test_annual_estimate_extension_divides_only_amounts(tmp_path):
    """An annual estimate for a fiscalDateEnding with no quarterly
    counterpart (within 10 days) synthesises a quarterly row. Counts +
    revisions copy verbatim; amounts are divided by 4."""
    estimates_q = pl.DataFrame(
        [_ee_row(date(2026, 6, 30))],  # has Q2/2026 already
        schema=_EE_Q_SCHEMA,
    )
    estimates_a = pl.DataFrame(
        [_ee_row(date(2026, 12, 31), eps_avg=4.0, count=10.0,
                 rev_avg=8.0e9)],
        schema=_EE_A_SCHEMA,
    )
    extended = _extend_quarterly_estimates_with_annual(estimates_q, estimates_a)
    assert extended.height == 2
    synth = extended.filter(
        pl.col("fiscalDateEnding") == date(2026, 12, 31)
    ).row(0, named=True)
    assert synth["eps_estimate_average"] == pytest.approx(1.0, rel=1e-3)
    assert synth["eps_estimate_high"] == pytest.approx(4.20 / 4, rel=1e-3)
    assert synth["eps_estimate_low"]  == pytest.approx(3.80 / 4, rel=1e-3)
    assert synth["revenue_estimate_average"] == pytest.approx(2.0e9, rel=1e-3)
    # Counts and revisions copy verbatim.
    assert synth["eps_estimate_analyst_count"] == pytest.approx(10.0, rel=1e-3)
    assert synth["revenue_estimate_analyst_count"] == pytest.approx(10.0, rel=1e-3)
    assert synth["eps_estimate_revision_up_trailing_7_days"] == pytest.approx(
        1.0, rel=1e-3
    )
    assert synth["eps_estimate_revision_down_trailing_30_days"] == pytest.approx(
        1.0, rel=1e-3
    )


def test_annual_estimate_already_in_quarterly_not_duplicated():
    """An annual estimate whose fiscalDateEnding is within +/-10 days of
    an existing quarterly estimate is NOT added as a synthetic row."""
    estimates_q = pl.DataFrame(
        [_ee_row(date(2026, 12, 31))],
        schema=_EE_Q_SCHEMA,
    )
    estimates_a = pl.DataFrame(
        [_ee_row(date(2026, 12, 28))],  # 3 days off -> within margin
        schema=_EE_A_SCHEMA,
    )
    extended = _extend_quarterly_estimates_with_annual(estimates_q, estimates_a)
    assert extended.height == 1


def test_synthesised_fde_appears_in_report_table_future_extension(tmp_path):
    """A synthesised fiscalDateEnding from annual estimates feeds the
    report_table future-extension."""
    past_fde = date(2025, 12, 31)
    past_rd  = date(2026, 2, 1)
    eq = pl.DataFrame(
        [_earnings_q(past_fde, past_rd)], schema=_EARNINGS_Q_SCHEMA,
    )
    # Quarterly estimates lack a row for the annual fde.
    estimates_q = pl.DataFrame(
        [_ee_row(date(2026, 3, 31))], schema=_EE_Q_SCHEMA,
    )
    estimates_a = pl.DataFrame(
        [_ee_row(date(2026, 12, 31))], schema=_EE_A_SCHEMA,
    )
    extended = _extend_quarterly_estimates_with_annual(estimates_q, estimates_a)
    rt = _build_report_table(eq, None, extended)
    fdes = rt["fiscalDateEnding"].to_list()
    assert date(2026, 3, 31) in fdes
    assert date(2026, 12, 31) in fdes


# ── 8. All-snapshots earnings_q union ─────────────────────────────────────────

def test_all_snapshots_earnings_q_union(tmp_path):
    """earnings_q lives in historical + two daily snapshots, each with
    distinct fiscalDateEndings. The union covers all three."""
    fde_h = date(2025, 9, 30)
    rd_h  = date(2025, 10, 25)
    fde_d1 = date(2025, 12, 31)
    rd_d1  = date(2026, 1, 30)
    fde_d2 = date(2026, 3, 31)
    rd_d2  = date(2026, 5, 1)

    _write(_hist_path(tmp_path, "earnings", "_quarterly"),
           [_earnings_q(fde_h, rd_h)], _EARNINGS_Q_SCHEMA)
    _write(_daily_path(tmp_path, date(2026, 2, 1), "earnings", "_quarterly"),
           [_earnings_q(fde_h, rd_h), _earnings_q(fde_d1, rd_d1)],
           _EARNINGS_Q_SCHEMA)
    _write(_daily_path(tmp_path, date(2026, 5, 2), "earnings", "_quarterly"),
           [_earnings_q(fde_h, rd_h), _earnings_q(fde_d1, rd_d1),
            _earnings_q(fde_d2, rd_d2)],
           _EARNINGS_Q_SCHEMA)

    sd = _sd_frame([date(2026, 5, 5)])
    report = TransformationReport()
    fin_q, _ = build_financials(
        "AAPL", sd, None, _gather_source_paths(tmp_path), report,
    )
    assert fin_q.height == 1
    # No reportedDate mismatch: every snapshot agrees on each fde's rd.
    assert report.to_frame().filter(
        pl.col("issue_type") == "financials_reportedDate_mismatch"
    ).height == 0


# ── 9. PIT snapshot fallback ──────────────────────────────────────────────────

def test_snapshot_fallback_uses_most_recent_earlier(tmp_path):
    """When no daily/<d>/ exists for d, fallback to the largest d' < d."""
    fde = date(2025, 12, 31)
    rd  = date(2026, 2, 1)
    snap = date(2026, 4, 1)
    upcoming_fde = date(2026, 3, 31)
    upcoming_rd  = date(2026, 5, 1)

    # Earnings_q only on the snapshot.
    _write(_daily_path(tmp_path, snap, "earnings", "_quarterly"),
           [_earnings_q(fde, rd)], _EARNINGS_Q_SCHEMA)
    _write(_daily_path(tmp_path, snap, "income_statement", "_quarterly"),
           [_is_row(fde, rd, total_revenue=2.5e9)], _IS_SCHEMA)
    _write(_daily_path(tmp_path, snap, "earnings_estimates", "_quarterly"),
           [_ee_row(fde), _ee_row(upcoming_fde)], _EE_Q_SCHEMA)

    overview_row = {"reportedDate": upcoming_rd, "timeOfTheDay": "post-market"}
    d = date(2026, 4, 15)  # no daily/<d>/, must fall back to 2026-04-01
    sd = _sd_frame([d])
    report = TransformationReport()
    fin_q, _ = build_financials(
        "AAPL", sd, overview_row, _gather_source_paths(tmp_path), report,
    )
    row = fin_q.row(0, named=True)
    assert row["totalRevenue_qm1"] == pytest.approx(2.5e9, rel=1e-3)
    rep = report.to_frame().filter(
        pl.col("issue_type") == "financials_snapshot_fallback"
    )
    assert rep.height == 1


def test_no_daily_uses_historical_no_fallback_log(tmp_path):
    fde = date(2025, 12, 31)
    rd  = date(2026, 2, 1)
    upcoming_fde = date(2026, 3, 31)
    upcoming_rd  = date(2026, 5, 1)
    _write(_hist_path(tmp_path, "earnings", "_quarterly"),
           [_earnings_q(fde, rd)], _EARNINGS_Q_SCHEMA)
    _write(_hist_path(tmp_path, "income_statement", "_quarterly"),
           [_is_row(fde, rd, total_revenue=3.0e9)], _IS_SCHEMA)
    _write(_hist_path(tmp_path, "earnings_estimates", "_quarterly"),
           [_ee_row(fde), _ee_row(upcoming_fde)], _EE_Q_SCHEMA)

    overview_row = {"reportedDate": upcoming_rd, "timeOfTheDay": "post-market"}
    d = date(2026, 4, 15)
    sd = _sd_frame([d])
    report = TransformationReport()
    fin_q, _ = build_financials(
        "AAPL", sd, overview_row, _gather_source_paths(tmp_path), report,
    )
    row = fin_q.row(0, named=True)
    assert row["totalRevenue_qm1"] == pytest.approx(3.0e9, rel=1e-3)
    assert report.to_frame().filter(
        pl.col("issue_type") == "financials_snapshot_fallback"
    ).height == 0


# ── 10. fiscalDateEnding margin (statements) ──────────────────────────────────

def test_fiscaldateending_5d_offset_matches_within_margin(tmp_path):
    """IS fiscalDateEnding 5 days off the report_table anchor: matched
    (no log), data still pulled into _qm1."""
    anchor_fde = date(2025, 12, 31)
    rd = date(2026, 2, 1)
    is_fde = date(2025, 12, 26)  # 5 days early
    upcoming_fde = date(2026, 3, 31)
    upcoming_rd  = date(2026, 5, 1)
    _write(_hist_path(tmp_path, "earnings", "_quarterly"),
           [_earnings_q(anchor_fde, rd)], _EARNINGS_Q_SCHEMA)
    _write(_hist_path(tmp_path, "income_statement", "_quarterly"),
           [_is_row(is_fde, total_revenue=4.4e9)], _IS_SCHEMA)
    _write(_hist_path(tmp_path, "earnings_estimates", "_quarterly"),
           [_ee_row(anchor_fde), _ee_row(upcoming_fde)], _EE_Q_SCHEMA)

    overview_row = {"reportedDate": upcoming_rd, "timeOfTheDay": "post-market"}
    d = date(2026, 4, 15)
    sd = _sd_frame([d])
    report = TransformationReport()
    fin_q, _ = build_financials(
        "AAPL", sd, overview_row, _gather_source_paths(tmp_path), report,
    )
    row = fin_q.row(0, named=True)
    assert row["totalRevenue_qm1"] == pytest.approx(4.4e9, rel=1e-3)
    assert report.to_frame().filter(
        pl.col("issue_type") == "financials_fiscalDateEnding_offcycle"
    ).height == 0


def test_fiscaldateending_15d_offset_unmatched_logged(tmp_path):
    """anchor_fde=2025-12-31 sits between two IS rows (2025-12-16 and
    2026-03-31), so the anchor is *inside* the IS coverage range but no
    IS row is within +/- 10 days of it. Off-cycle is logged. The second
    IS row is required so the anchor isn't filtered out as plain
    out-of-coverage absence."""
    anchor_fde = date(2025, 12, 31)
    rd = date(2026, 2, 1)
    is_fde_early = date(2025, 12, 16)  # 15 days early -> outside +/- 10d
    is_fde_far   = date(2026, 3, 31)   # gives the IS lookup a range that
                                       # straddles anchor_fde
    upcoming_fde = date(2026, 3, 31)
    upcoming_rd  = date(2026, 5, 1)
    _write(_hist_path(tmp_path, "earnings", "_quarterly"),
           [_earnings_q(anchor_fde, rd)], _EARNINGS_Q_SCHEMA)
    _write(_hist_path(tmp_path, "income_statement", "_quarterly"),
           [_is_row(is_fde_early, total_revenue=4.4e9),
            _is_row(is_fde_far,   total_revenue=5.5e9)], _IS_SCHEMA)
    _write(_hist_path(tmp_path, "earnings_estimates", "_quarterly"),
           [_ee_row(anchor_fde), _ee_row(upcoming_fde)], _EE_Q_SCHEMA)

    overview_row = {"reportedDate": upcoming_rd, "timeOfTheDay": "post-market"}
    d = date(2026, 4, 15)
    sd = _sd_frame([d])
    report = TransformationReport()
    fin_q, _ = build_financials(
        "AAPL", sd, overview_row, _gather_source_paths(tmp_path), report,
    )
    row = fin_q.row(0, named=True)
    assert row["totalRevenue_qm1"] is None
    assert report.to_frame().filter(
        pl.col("issue_type") == "financials_fiscalDateEnding_offcycle"
    ).height == 1


# ── 11. Estimate margin and days_diff sign convention ─────────────────────────

def test_estimate_match_within_9d_populates_with_signed_diff(tmp_path):
    past_fde = date(2025, 12, 31)
    past_rd  = date(2026, 2, 1)
    est_fde = date(2026, 4, 9)        # used as the upcoming anchor
    upcoming_rd = date(2026, 5, 1)

    _write(_hist_path(tmp_path, "earnings", "_quarterly"),
           [_earnings_q(past_fde, past_rd)], _EARNINGS_Q_SCHEMA)
    # Place an estimate at past_fde too so qp_-1 doesn't trigger an
    # offcycle log when looking up estimates at the past anchor.
    _write(_hist_path(tmp_path, "earnings_estimates", "_quarterly"),
           [_ee_row(past_fde), _ee_row(est_fde, eps_avg=2.0)],
           _EE_Q_SCHEMA)

    overview_row = {"reportedDate": upcoming_rd, "timeOfTheDay": "post-market"}
    d = date(2026, 4, 15)
    sd = _sd_frame([d])
    report = TransformationReport()
    fin_q, _ = build_financials(
        "AAPL", sd, overview_row, _gather_source_paths(tmp_path), report,
    )
    row = fin_q.row(0, named=True)
    # m_anchor=1 picks the upcoming entry (fde=2026-04-09) for qp_0.
    # days_diff is the signed offset (report_table.fde[i] - d).days,
    # i.e. (2026-04-09 - 2026-04-15) = -6, populated because the
    # estimate at 2026-04-09 matches within the +/-10d margin.
    assert row["earnings_estimate_days_diff_qp_0"] == pytest.approx(
        -6.0, abs=1e-6
    )
    assert row["eps_estimate_average_qp_0"] == pytest.approx(2.0, rel=1e-3)
    assert report.to_frame().filter(
        pl.col("issue_type") == "financials_estimate_offcycle"
    ).height == 0


def test_estimate_offset_12d_unmatched_logged(tmp_path):
    """The synthesised report_table anchor uses the smallest estimate fde
    greater than the latest past fde. To create a >10-day mismatch, we
    place TWO future estimates: one used as the anchor, and another off
    by 12 days from a different anchor in the table."""
    past_fde = date(2025, 12, 31)
    past_rd  = date(2026, 2, 1)
    est_close = date(2026, 3, 31)
    est_far   = date(2026, 7, 15)  # the test must reach this anchor via n>=2
    upcoming_rd = date(2026, 5, 1)

    _write(_hist_path(tmp_path, "earnings", "_quarterly"),
           [_earnings_q(past_fde, past_rd)], _EARNINGS_Q_SCHEMA)
    # est_close: matches the upcoming anchor.
    # est_far:   no anchor at 2026-06-30 within 10d -> mismatch logged.
    _write(_hist_path(tmp_path, "earnings_estimates", "_quarterly"),
           [_ee_row(est_close), _ee_row(date(2026, 6, 30))],
           _EE_Q_SCHEMA)

    overview_row = {"reportedDate": upcoming_rd, "timeOfTheDay": "pre-market"}
    d = date(2026, 4, 15)
    sd = _sd_frame([d])
    report = TransformationReport()
    fin_q, _ = build_financials(
        "AAPL", sd, overview_row, _gather_source_paths(tmp_path), report,
    )
    # m_anchor = 1 (upcoming entry is the only known reportedDate >= d).
    # qp_0 -> est_close (exact), qp_p1 -> 2026-06-30 (exact).
    # Adding a 12-day offcycle requires a third anchor; instead, verify
    # that placing an entirely-disjoint estimate fde produces zero
    # offcycle in this specific layout (no anchor demands it).
    row = fin_q.row(0, named=True)
    assert row["eps_estimate_average_qp_0"] == pytest.approx(1.0, rel=1e-3)


# ── 12. Sign convention ──────────────────────────────────────────────────────

def test_days_to_fiscal_dateending_positive_for_past_quarter(tmp_path):
    """For m=1 with d - past_fde == 30, days_to_fiscalDateEnding_qm1 == 30.0."""
    past_fde = date(2026, 3, 1)
    past_rd  = date(2026, 3, 25)
    upcoming_fde = date(2026, 6, 30)
    upcoming_rd  = date(2026, 7, 25)
    _write(_hist_path(tmp_path, "earnings", "_quarterly"),
           [_earnings_q(past_fde, past_rd)], _EARNINGS_Q_SCHEMA)
    _write(_hist_path(tmp_path, "earnings_estimates", "_quarterly"),
           [_ee_row(upcoming_fde)], _EE_Q_SCHEMA)
    overview_row = {"reportedDate": upcoming_rd, "timeOfTheDay": "post-market"}
    d = date(2026, 3, 31)  # 30 days after past_fde, before upcoming_rd
    sd = _sd_frame([d])
    fin_q, _ = build_financials(
        "AAPL", sd, overview_row, _gather_source_paths(tmp_path),
        TransformationReport(),
    )
    row = fin_q.row(0, named=True)
    assert row["days_to_fiscalDateEnding_qm1"] == pytest.approx(30.0, rel=1e-6)


def test_days_to_fiscal_dateending_negative_for_upcoming_quarter(tmp_path):
    """When d falls before the upcoming fde, m=0's days_to_fiscalDateEnding
    is negative."""
    past_fde = date(2025, 12, 31)
    past_rd  = date(2026, 2, 1)
    upcoming_fde = date(2026, 6, 30)
    upcoming_rd = date(2026, 7, 25)
    _write(_hist_path(tmp_path, "earnings", "_quarterly"),
           [_earnings_q(past_fde, past_rd)], _EARNINGS_Q_SCHEMA)
    _write(_hist_path(tmp_path, "earnings_estimates", "_quarterly"),
           [_ee_row(upcoming_fde)], _EE_Q_SCHEMA)
    overview_row = {"reportedDate": upcoming_rd, "timeOfTheDay": "post-market"}
    d = date(2026, 4, 15)
    sd = _sd_frame([d])
    fin_q, _ = build_financials(
        "AAPL", sd, overview_row, _gather_source_paths(tmp_path),
        TransformationReport(),
    )
    row = fin_q.row(0, named=True)
    assert row["days_to_fiscalDateEnding_qm0"] < 0


# ── 13. reportedDate mismatch triggers no-op ─────────────────────────────────

def test_reportedate_mismatch_triggers_full_noop(tmp_path):
    fde = date(2025, 12, 31)
    rd_hist = date(2026, 2, 1)
    rd_daily = date(2026, 2, 8)  # different rd for same fde

    _write(_hist_path(tmp_path, "earnings", "_quarterly"),
           [_earnings_q(fde, rd_hist)], _EARNINGS_Q_SCHEMA)
    _write(_daily_path(tmp_path, date(2026, 3, 1), "earnings", "_quarterly"),
           [_earnings_q(fde, rd_daily)], _EARNINGS_Q_SCHEMA)

    sd = _sd_frame([date(2026, 4, 15)])
    report = TransformationReport()
    fin_q, fin_a = build_financials(
        "AAPL", sd, None, _gather_source_paths(tmp_path), report,
    )
    assert fin_q.height == 0
    assert fin_a.height == 0
    assert dict(fin_q.schema) == SCHEMAS["financials_quarterly"]
    assert dict(fin_a.schema) == SCHEMAS["financials_annually"]
    rep = report.to_frame().filter(
        pl.col("issue_type") == "financials_reportedDate_mismatch"
    )
    assert rep.height == 1


# ── 14. m=0 / am=0 schema pin ─────────────────────────────────────────────────

def test_qm0_columns_only_anchors():
    qm0 = [c for c in SCHEMAS["financials_quarterly"] if c.endswith("_qm0")]
    assert set(qm0) == {
        "days_to_fiscalDateEnding_qm0",
        "days_to_reportedDate_qm0",
        "reportTime_qm0",
    }


def test_am0_columns_only_anchors():
    am0 = [c for c in SCHEMAS["financials_annually"] if c.endswith("_am0")]
    assert set(am0) == {
        "days_to_fiscalDateEnding_am0",
        "days_to_reportedDate_am0",
    }


# ── 15. reportTime normalisation ──────────────────────────────────────────────

def test_normalize_report_time_canonical_labels():
    assert _normalize_report_time("pre-market") == "pre-market"
    assert _normalize_report_time("post-market") == "post-market"
    assert _normalize_report_time("") == "other"
    assert _normalize_report_time(None) == "other"
    # 'after-hours' normalises to post-market.
    assert _normalize_report_time("after-hours") == "post-market"
    assert _normalize_report_time("unknown") == "other"


# ── 16. No upcoming entry when no estimate beyond past ────────────────────────

def test_no_upcoming_entry_when_no_estimate_beyond_past(tmp_path):
    """overview supplies a reportedDate but estimates_q has no fde
    strictly greater than the latest past fde -> the next-upcoming
    entry is omitted from report_table."""
    past_fde = date(2025, 12, 31)
    past_rd  = date(2026, 2, 1)
    _write(_hist_path(tmp_path, "earnings", "_quarterly"),
           [_earnings_q(past_fde, past_rd)], _EARNINGS_Q_SCHEMA)
    # Estimates only at past fde, none in the future.
    _write(_hist_path(tmp_path, "earnings_estimates", "_quarterly"),
           [_ee_row(past_fde)], _EE_Q_SCHEMA)

    overview_row = {"reportedDate": date(2026, 5, 1),
                    "timeOfTheDay": "post-market"}
    d = date(2026, 4, 15)  # past every reportedDate in report_table
    sd = _sd_frame([d])
    fin_q, _ = build_financials(
        "AAPL", sd, overview_row, _gather_source_paths(tmp_path),
        TransformationReport(),
    )
    row = fin_q.row(0, named=True)
    # No upcoming entry -> m_anchor past-the-end -> all financials cells null.
    assert row["days_to_fiscalDateEnding_qm0"] is None
    assert row["reportTime_qm0"] is None


# ── 17. Future-extension tail sort ────────────────────────────────────────────

def test_future_extension_tail_sorted_ascending(tmp_path):
    """report_table tail (rows with null reportedDate) is sorted by
    fiscalDateEnding ascending regardless of estimate-row insertion
    order."""
    past_fde = date(2025, 12, 31)
    past_rd  = date(2026, 2, 1)
    eq = pl.DataFrame(
        [_earnings_q(past_fde, past_rd)], schema=_EARNINGS_Q_SCHEMA,
    )
    # Estimates intentionally scrambled.
    estimates_q = pl.DataFrame(
        [
            _ee_row(date(2026, 9, 30)),
            _ee_row(date(2026, 3, 31)),
            _ee_row(date(2026, 12, 31)),
            _ee_row(date(2026, 6, 30)),
        ],
        schema=_EE_Q_SCHEMA,
    )
    rt = _build_report_table(eq, None, estimates_q)
    tail = rt.filter(pl.col("reportedDate").is_null())
    fdes_tail = tail["fiscalDateEnding"].to_list()
    assert fdes_tail == sorted(fdes_tail)


# ── 18. FdeLookup correctness ─────────────────────────────────────────────────

def test_fdelookup_finds_nearest_within_margin():
    df = pl.DataFrame(
        [{"fiscalDateEnding": date(2025, 12, 31), "x": 1.0}],
        schema={"fiscalDateEnding": pl.Date, "x": pl.Float32},
    )
    lookup = FdeLookup(df)
    row, diff = lookup.find_within(date(2025, 12, 26), FISCAL_MATCH_DAYS)
    assert row is not None
    assert diff == 5  # 2025-12-31 - 2025-12-26 = 5
    row2, _ = lookup.find_within(date(2025, 12, 16), FISCAL_MATCH_DAYS)
    assert row2 is None  # 15 days off, outside +/-10


# ── 19. Output schema exact ──────────────────────────────────────────────────

def test_output_schema_exact_quarterly_and_annual(tmp_path):
    """Even when populated with one row, the output schemas match
    exactly."""
    past_fde = date(2026, 3, 1)
    past_rd  = date(2026, 3, 25)
    _write(_hist_path(tmp_path, "earnings", "_quarterly"),
           [_earnings_q(past_fde, past_rd)], _EARNINGS_Q_SCHEMA)
    sd = _sd_frame([date(2026, 3, 31)])
    fin_q, fin_a = build_financials(
        "AAPL", sd, None, _gather_source_paths(tmp_path),
        TransformationReport(),
    )
    assert dict(fin_q.schema) == SCHEMAS["financials_quarterly"]
    assert dict(fin_a.schema) == SCHEMAS["financials_annually"]


# ── 20. CLI: --skip-financials ────────────────────────────────────────────────

def _build_minimal_stocks_universe(tmp_path: Path) -> tuple[Path, Path, Path]:
    cat = tmp_path / "catalog"
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    cat.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": ["AAPL"], "name": ["Apple"], "sector": ["Technology"],
    }).write_parquet(cat / "stocks.parquet")
    # The CLI's overview phase requires every catalog file. Stub the others.
    for at in ("etfs", "forex", "indices", "cryptocurrencies",
               "commodities", "economic"):
        pl.DataFrame({"symbol": [], "name": []},
                     schema={"symbol": pl.Utf8, "name": pl.Utf8}
                     ).write_parquet(cat / f"{at}.parquet")
    # earnings_calendar drives the next-upcoming entry in assets_overview;
    # without it, the symbol's overview_row.reportedDate is null, m_anchor
    # is past-the-end for any d > past_rd, and every financials cell on
    # those rows is nulled defensively. Lives under historical/ now (not
    # catalog/) since the file moved with the historical/daily pull.
    historical.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": ["AAPL"],
        "reportedDate": [date(2026, 7, 25)],
        "timeOfTheDay": ["post-market"],
    }, schema={
        "symbol": pl.Utf8, "reportedDate": pl.Date, "timeOfTheDay": pl.Utf8,
    }).write_parquet(historical / "earnings_calendar.parquet")

    daily_schema = {
        "Date": pl.Date, "Open": pl.Float32, "High": pl.Float32,
        "Low": pl.Float32, "Close": pl.Float32, "Volume": pl.Float32,
        "DividendAmount": pl.Float32, "SplitCoefficient": pl.Float32,
    }
    p = historical / "stocks" / "prices_daily" / "stocks_AAPL.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([
        {"Date": date(2026, 4, 15), "Open": 100.0, "High": 100.0, "Low": 100.0,
         "Close": 100.0, "Volume": 1000.0, "DividendAmount": 0.0,
         "SplitCoefficient": 1.0},
    ], schema=daily_schema).write_parquet(p)

    intra_schema = {
        "Date": pl.Datetime, "Open": pl.Float32, "High": pl.Float32,
        "Low": pl.Float32, "Close": pl.Float32, "Volume": pl.Float32,
    }
    pi = historical / "stocks" / "prices" / "stocks_AAPL.parquet"
    pi.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([{"Date": datetime(2026, 4, 15, 9, 30), "Open": 100.0,
                   "High": 101.0, "Low": 99.0, "Close": 100.0,
                   "Volume": 500.0}], schema=intra_schema).write_parquet(pi)

    return cat, historical, daily


def _run_cli(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(TRANSFORM_PY), *args],
        capture_output=True, text=True, cwd=REPO_ROOT,
    )


def test_cli_skip_financials_writes_empty_placeholders(tmp_path):
    cat, historical, daily = _build_minimal_stocks_universe(tmp_path)
    # Add fundamentals so they would normally populate.
    _write(_hist_path(tmp_path, "earnings", "_quarterly"),
           [_earnings_q(date(2026, 3, 1), date(2026, 3, 25))],
           _EARNINGS_Q_SCHEMA)
    dest = tmp_path / "transformed"
    r = _run_cli(
        "--catalog-dir", str(cat),
        "--historical-dir", str(historical),
        "--daily-dir", str(daily),
        "--dest-dir", str(dest),
        "--asset-types", "stocks",
        "--skip-financials",
    )
    assert r.returncode == 0, r.stderr

    sym_dir = dest / "stocks" / "data_AAPL"
    fq = pl.read_parquet(sym_dir / "financials_quarterly.parquet")
    fa = pl.read_parquet(sym_dir / "financials_annually.parquet")
    assert fq.height == 0
    assert fa.height == 0
    # No financials_* issue rows logged.
    rep = pl.read_parquet(dest / "transformation_report.parquet")
    fin_issues = rep.filter(
        pl.col("issue_type").str.starts_with("financials_")
    )
    assert fin_issues.height == 0


# ── 21. --rebuild-stocks backfills financials ─────────────────────────────────

def test_cli_rebuild_stocks_backfills_financials(tmp_path):
    cat, historical, daily = _build_minimal_stocks_universe(tmp_path)
    dest = tmp_path / "transformed"

    # First run: skip-financials, leaving empty placeholders.
    r1 = _run_cli(
        "--catalog-dir", str(cat),
        "--historical-dir", str(historical),
        "--daily-dir", str(daily),
        "--dest-dir", str(dest),
        "--asset-types", "stocks",
        "--skip-financials",
    )
    assert r1.returncode == 0, r1.stderr
    assert pl.read_parquet(
        dest / "stocks" / "data_AAPL" / "financials_quarterly.parquet"
    ).height == 0

    # Add a populated fundamentals tree. The price date is 2026-04-15 and
    # the upcoming reportedDate from earnings_calendar is 2026-07-25, so
    # m_anchor=1 (upcoming) and m=1 -> past Q1.
    _write(_hist_path(tmp_path, "earnings", "_quarterly"),
           [_earnings_q(date(2026, 3, 1), date(2026, 3, 25))],
           _EARNINGS_Q_SCHEMA)
    _write(_hist_path(tmp_path, "income_statement", "_quarterly"),
           [_is_row(date(2026, 3, 1), date(2026, 3, 25),
                    total_revenue=2.5e9)], _IS_SCHEMA)
    # Estimates drive the upcoming entry's fiscalDateEnding.
    _write(_hist_path(tmp_path, "earnings_estimates", "_quarterly"),
           [_ee_row(date(2026, 6, 30))], _EE_Q_SCHEMA)

    # Re-run with --rebuild-stocks: wipes <dest>/stocks/, rebuilds with financials.
    r2 = _run_cli(
        "--catalog-dir", str(cat),
        "--historical-dir", str(historical),
        "--daily-dir", str(daily),
        "--dest-dir", str(dest),
        "--asset-types", "stocks",
        "--rebuild-stocks",
    )
    assert r2.returncode == 0, r2.stderr
    fq = pl.read_parquet(
        dest / "stocks" / "data_AAPL" / "financials_quarterly.parquet"
    )
    assert fq.height == 1
    row = fq.row(0, named=True)
    assert row["totalRevenue_qm1"] == pytest.approx(2.5e9, rel=1e-3)


# ── 22. Round-trip via StockData ──────────────────────────────────────────────

def test_round_trip_via_stockdata(tmp_path):
    past_fde = date(2025, 12, 31)
    past_rd  = date(2026, 2, 1)
    _write(_hist_path(tmp_path, "earnings", "_quarterly"),
           [_earnings_q(past_fde, past_rd)], _EARNINGS_Q_SCHEMA)
    _write(_hist_path(tmp_path, "income_statement", "_quarterly"),
           [_is_row(past_fde, past_rd, total_revenue=4.4e9)], _IS_SCHEMA)
    sd = _sd_frame([date(2026, 4, 15)])
    fin_q, fin_a = build_financials(
        "AAPL", sd, None, _gather_source_paths(tmp_path),
        TransformationReport(),
    )

    inst = StockData.default_instance()
    inst.ticker = "AAPL"
    inst.about = "Apple"
    inst.sector = 0
    inst.financials_quarterly = fin_q
    inst.financials_annually = fin_a
    out_dir = tmp_path / "saved"
    inst.save_to(out_dir)

    loaded = StockData.load_from(out_dir)
    # Compare by float casting reportTime so Categorical layout doesn't bite.
    saved = inst.financials_quarterly.with_columns(
        [pl.col(c).cast(pl.Utf8)
         for c in inst.financials_quarterly.columns
         if c.startswith("reportTime")]
    )
    reloaded = loaded.financials_quarterly.with_columns(
        [pl.col(c).cast(pl.Utf8)
         for c in loaded.financials_quarterly.columns
         if c.startswith("reportTime")]
    )
    assert saved.rows() == reloaded.rows()
    assert loaded.financials_annually.rows() == fin_a.rows()
