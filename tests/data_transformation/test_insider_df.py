"""Tests for Phase 6a: insider_df for stocks.

Covers concat across historical + multiple daily folders, composite-key
dedup with discrepancy logging, the executive_title -> Executive_role
ordered rule list (including the regression-pinned priority cases),
AcqDis filtering, null-Shares drop, sort order, schema exactness, and
StockData round-trip.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from data_transformation._common import TransformationReport
from data_transformation.AssetData import CANONICAL_INSIDER_ROLES, StockData
from data_transformation.AssetDataService import SCHEMAS
from data_transformation.frames.insider import (
    _INSIDER_ROLE_RULES,
    _normalize_insider_source,
    _role_expr,
    build_insider_df,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

_INSIDER_SOURCE_SCHEMA = {
    "transactionDate": pl.Date,
    "executive": pl.Utf8,
    "executive_title": pl.Utf8,
    "security_type": pl.Utf8,
    "acquisition_or_disposal": pl.Utf8,
    "shares": pl.Float32,
    "share_price": pl.Float32,
}


def _row(
    d: date,
    executive: str = "Jane Doe",
    title: str = "Chief Executive Officer",
    security_type: str = "Common Stock",
    acq: str = "A",
    shares: float | None = 100.0,
    price: float | None = 50.0,
) -> dict:
    return {
        "transactionDate": d,
        "executive": executive,
        "executive_title": title,
        "security_type": security_type,
        "acquisition_or_disposal": acq,
        "shares": shares,
        "share_price": price,
    }


def _write_insider(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, schema=_INSIDER_SOURCE_SCHEMA).write_parquet(path)


def _classify(title: str | None) -> str:
    """Run the role mapper for one title via _role_expr() against a single-row
    frame and return the resulting Executive_role string."""
    df = pl.DataFrame(
        {"executive_title": [title]},
        schema={"executive_title": pl.Utf8},
    )
    return df.with_columns(_role_expr().alias("role"))["role"][0]


# ── 1. Empty inputs ───────────────────────────────────────────────────────────

def test_empty_paths_returns_schema_correct_empty():
    report = TransformationReport()
    out = build_insider_df("AAPL", [], report)
    assert out.height == 0
    assert dict(out.schema) == SCHEMAS["insider_df"]
    assert report.to_frame().height == 0


# ── 2. Concat of historical + multiple daily folders ──────────────────────────

def test_concat_historical_plus_multiple_daily(tmp_path):
    h = tmp_path / "h.parquet"
    d1 = tmp_path / "d1.parquet"
    d2 = tmp_path / "d2.parquet"
    _write_insider(h, [
        _row(date(2026, 4, 10), executive="Alice", title="CEO"),
        _row(date(2026, 4, 11), executive="Bob",   title="CFO"),
    ])
    _write_insider(d1, [
        _row(date(2026, 4, 12), executive="Carol", title="Director"),
    ])
    _write_insider(d2, [
        _row(date(2026, 4, 13), executive="Dan",   title="Vice President"),
    ])
    out = build_insider_df("AAPL", [h, d1, d2], TransformationReport())
    assert out.height == 4
    dates = out["Date"].to_list()
    assert dates == sorted(dates)
    assert dates[0] == date(2026, 4, 10)
    assert dates[-1] == date(2026, 4, 13)


# ── 3. Composite-key dedup with discrepancy logging ───────────────────────────

def test_composite_key_dedup_discrepancy_under_and_over_1pct(tmp_path):
    h = tmp_path / "h.parquet"
    d = tmp_path / "d.parquet"
    _write_insider(h, [
        _row(date(2026, 4, 10), executive="Alice", title="CEO",
             security_type="Common Stock", shares=100.0, price=50.0),
        _row(date(2026, 4, 11), executive="Bob", title="CFO",
             security_type="Common Stock", shares=200.0, price=100.0),
    ])
    _write_insider(d, [
        # 0.5% diff -> under_1pct.
        _row(date(2026, 4, 10), executive="Alice", title="CEO",
             security_type="Common Stock", shares=100.0, price=50.25),
        # 10% diff -> over_1pct.
        _row(date(2026, 4, 11), executive="Bob", title="CFO",
             security_type="Common Stock", shares=200.0, price=110.0),
    ])
    report = TransformationReport()
    out = build_insider_df("AAPL", [h, d], report)
    assert out.height == 2
    rep = report.to_frame()
    issues = set(rep["issue_type"].to_list())
    assert "dedup_value_discrepancy_under_1pct" in issues
    assert "dedup_value_discrepancy_over_1pct" in issues


def test_composite_key_dedup_keeps_most_recent_source(tmp_path):
    h = tmp_path / "h.parquet"
    d = tmp_path / "d.parquet"
    _write_insider(h, [
        _row(date(2026, 4, 10), executive="Alice", title="CEO",
             security_type="Common Stock", shares=100.0, price=50.0),
    ])
    _write_insider(d, [
        _row(date(2026, 4, 10), executive="Alice", title="CEO",
             security_type="Common Stock", shares=200.0, price=55.0),
    ])
    out = build_insider_df("AAPL", [h, d], TransformationReport())
    assert out.height == 1
    assert pytest.approx(200.0, rel=1e-4) == out["Shares"][0]


# ── 4. Composite-key dedup keeps distinct security types ──────────────────────

def test_distinct_security_types_not_collapsed(tmp_path):
    """Same exec, same date, but different security_type: both rows survive."""
    p = tmp_path / "p.parquet"
    _write_insider(p, [
        _row(date(2026, 4, 10), executive="Alice", title="CEO",
             security_type="Common Stock", shares=100.0),
        _row(date(2026, 4, 10), executive="Alice", title="CEO",
             security_type="Stock Option", shares=200.0),
    ])
    out = build_insider_df("AAPL", [p], TransformationReport())
    assert out.height == 2
    assert sorted(out["Shares"].to_list()) == [100.0, 200.0]


# ── 5. Role mapping rule order ────────────────────────────────────────────────

def test_role_mapping_canonical_labels_only():
    """Every label produced by the rule list is in CANONICAL_INSIDER_ROLES."""
    titles = [
        "Chief Accounting Officer",       # CAO
        "Controller",                      # CAO
        "General Counsel & Secretary",    # General Counsel
        "Chief Financial Officer",         # CFO
        "Treasurer",                       # CFO
        "Chief Operating Officer",         # COO
        "Chief Technology Officer",        # CTO_CIO
        "Chief Information Officer",       # CTO_CIO
        "Chief Digital Officer",           # CTO_CIO
        "Senior Vice President",           # VP
        "Executive Vice President",        # VP
        "Chief Executive Officer",         # CEO
        "President",                        # CEO
        "Chief Marketing Officer",         # Other C-Suite (chief catch-all)
        "Chairman of the Board",           # Chairman
        "Director",                         # Director
        "10% Beneficial Owner",            # 10% Owner
        "Officer",                          # Officer
        "Random Role Nobody Knows",        # Other
        "",                                  # Other (empty)
    ]
    for t in titles:
        out = _classify(t)
        assert out in CANONICAL_INSIDER_ROLES, f"{t!r} -> {out!r}"


def test_role_mapping_priority_chief_accounting_hits_cao_not_other_csuite():
    assert _classify("Chief Accounting Officer") == "CAO"


def test_role_mapping_priority_president_and_cfo_hits_cfo_not_ceo():
    assert _classify("President & CFO") == "CFO"


def test_role_mapping_empty_and_null_to_other():
    assert _classify("") == "Other"
    assert _classify(None) == "Other"
    assert _classify("Wholly Unrelated Role") == "Other"


# ── 6. Role mapping priority regression pins ──────────────────────────────────

def test_role_mapping_chief_executive_officer_to_ceo():
    """CEO must precede the 'chief ' catch-all in Other C-Suite."""
    assert _classify("Chief Executive Officer") == "CEO"


def test_role_mapping_vice_president_to_vp():
    """VP must precede CEO; otherwise CEO's 'president' pattern fires."""
    assert _classify("Vice President") == "VP"


def test_role_mapping_director_not_cto_substring():
    """Bare cto pattern must use word boundaries; without them, 'Director'
    contains 'cto' as a substring and would route to CTO_CIO."""
    assert _classify("Director") == "Director"


def test_role_mapping_bare_acronym_titles_match():
    """String-start / string-end count as word boundaries, so a bare
    acronym title still matches its rule."""
    assert _classify("CTO") == "CTO_CIO"
    assert _classify("CIO") == "CTO_CIO"
    assert _classify("CFO") == "CFO"
    assert _classify("COO") == "COO"
    assert _classify("CEO") == "CEO"
    assert _classify("VP") == "VP"


# ── 7. AcqDis verbatim, invalid drops the row ─────────────────────────────────

def test_acqdis_a_and_d_kept_other_dropped(tmp_path):
    p = tmp_path / "p.parquet"
    _write_insider(p, [
        _row(date(2026, 4, 10), executive="A1", acq="A"),
        _row(date(2026, 4, 11), executive="A2", acq="D"),
        _row(date(2026, 4, 12), executive="A3", acq="X"),     # invalid
        _row(date(2026, 4, 13), executive="A4", acq=""),       # invalid
        _row(date(2026, 4, 14), executive="A5", acq=None),     # invalid
    ])
    report = TransformationReport()
    out = build_insider_df("AAPL", [p], report)
    assert out.height == 2
    assert set(out["AcqDis"].to_list()) == {"A", "D"}
    rep = report.to_frame().filter(pl.col("issue_type") == "dedup_dropped_null_row")
    assert rep.height == 1
    assert rep["count"][0] == 3


# ── 8. Null Shares dropped ────────────────────────────────────────────────────

def test_null_shares_dropped(tmp_path):
    p = tmp_path / "p.parquet"
    _write_insider(p, [
        _row(date(2026, 4, 10), executive="A1", shares=100.0),
        _row(date(2026, 4, 11), executive="A2", shares=None),
    ])
    report = TransformationReport()
    out = build_insider_df("AAPL", [p], report)
    assert out.height == 1
    assert out["Shares"][0] == 100.0
    rep = report.to_frame().filter(pl.col("issue_type") == "dedup_dropped_null_row")
    assert rep.height == 1
    assert rep["count"][0] == 1


# ── 9. Output schema exact ────────────────────────────────────────────────────

def test_output_schema_exact(tmp_path):
    p = tmp_path / "p.parquet"
    _write_insider(p, [_row(date(2026, 4, 10))])
    out = build_insider_df("AAPL", [p], TransformationReport())
    assert dict(out.schema) == SCHEMAS["insider_df"]
    assert set(out.columns) == {"Date", "Executive_role", "AcqDis", "Shares"}
    # Categorical role / AcqDis (allow polars to vary in physical layout).
    assert out.schema["Executive_role"].base_type() == pl.Categorical
    assert out.schema["AcqDis"].base_type() == pl.Categorical


# ── 10. Sort order ────────────────────────────────────────────────────────────

def test_output_sorted_by_date(tmp_path):
    """Daily folders contribute interleaved transaction dates; output is
    sorted by Date ascending regardless of source order."""
    h = tmp_path / "h.parquet"
    d1 = tmp_path / "d1.parquet"
    d2 = tmp_path / "d2.parquet"
    _write_insider(h, [_row(date(2026, 4, 15), executive="A1")])
    _write_insider(d1, [_row(date(2026, 4, 10), executive="A2")])
    _write_insider(d2, [_row(date(2026, 4, 12), executive="A3")])
    out = build_insider_df("AAPL", [h, d1, d2], TransformationReport())
    assert out["Date"].to_list() == sorted(out["Date"].to_list())


# ── 11. Round-trip via StockData ──────────────────────────────────────────────

def test_round_trip_via_stockdata(tmp_path):
    p = tmp_path / "p.parquet"
    _write_insider(p, [
        _row(date(2026, 4, 10), executive="Alice", title="CEO",
             shares=100.0, price=50.0),
        _row(date(2026, 4, 12), executive="Bob",   title="CFO",
             shares=200.0, price=100.0, acq="D"),
    ])
    df = build_insider_df("AAPL", [p], TransformationReport())

    inst = StockData.default_instance()
    inst.ticker = "AAPL"
    inst.about = "Apple"
    inst.sector = 0
    inst.insider_df = df
    out_dir = tmp_path / "out"
    inst.save_to(out_dir)

    loaded = StockData.load_from(out_dir)
    assert loaded.insider_df.height == df.height
    # Compare by string projection so Categorical physical-layout drift
    # doesn't break the assertion.
    saved_rows = df.with_columns(
        pl.col("Executive_role").cast(pl.Utf8),
        pl.col("AcqDis").cast(pl.Utf8),
    ).rows()
    loaded_rows = loaded.insider_df.with_columns(
        pl.col("Executive_role").cast(pl.Utf8),
        pl.col("AcqDis").cast(pl.Utf8),
    ).rows()
    assert saved_rows == loaded_rows


# ── 12. Normalize: missing optional columns filled with null ──────────────────

def test_normalize_handles_missing_optional_columns():
    """A source file missing security_type / acquisition_or_disposal / etc
    has those columns synthesised as null Utf8 / null Float32. Required
    transactionDate must be present."""
    src = pl.DataFrame(
        {
            "transactionDate": [date(2026, 4, 10)],
            "executive": ["Alice"],
            "executive_title": ["CEO"],
            "shares": [100.0],
        },
        schema={
            "transactionDate": pl.Date,
            "executive": pl.Utf8,
            "executive_title": pl.Utf8,
            "shares": pl.Float32,
        },
    )
    out = _normalize_insider_source(src)
    # All canonical columns appear with the right dtypes.
    for col in (
        "transactionDate", "executive", "executive_title", "security_type",
        "acquisition_or_disposal", "shares", "share_price",
    ):
        assert col in out.columns
    assert out["security_type"][0] is None
    assert out["share_price"][0] is None
