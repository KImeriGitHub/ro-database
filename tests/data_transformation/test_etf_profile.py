"""Tests for Phase 5: etf_profile for ETFs.

Covers the column rename ``date`` -> ``Date``, the drop of
``inception_date``, the Utf8 -> Categorical cast for ``leveraged``, the
``holdings`` List(Struct) round-trip, concat across historical + multiple
daily folders, the dedup helper firing on duplicate dates, schema
exactness, and the orchestrator's etf-only behaviour.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from data_transformation._common import (
    TransformationReport,
    is_already_transformed,
    symbol_dest_dir,
)
from data_transformation.AssetData import ETFData
from data_transformation.AssetDataService import SCHEMAS
from data_transformation.frames.etf_profile import (
    build_etf_profile,
    _normalize_profile_source,
)
from data_transformation.frames.stocks_etfs import transform_stocks_or_etfs


# ── Helpers ───────────────────────────────────────────────────────────────────

_PROFILE_SOURCE_SCHEMA = {
    "date": pl.Date,
    "information_technology": pl.Float32,
    "communication_services": pl.Float32,
    "consumer_discretionary": pl.Float32,
    "consumer_staples": pl.Float32,
    "healthcare": pl.Float32,
    "industrials": pl.Float32,
    "utilities": pl.Float32,
    "materials": pl.Float32,
    "energy": pl.Float32,
    "financials": pl.Float32,
    "real_estate": pl.Float32,
    "other": pl.Float32,
    "holdings": pl.List(pl.Struct({"symbol": pl.Utf8, "weight": pl.Float32})),
    "net_assets": pl.Float32,
    "net_expense_ratio": pl.Float32,
    "portfolio_turnover": pl.Float32,
    "dividend_yield": pl.Float32,
    "inception_date": pl.Utf8,
    "leveraged": pl.Utf8,
}

_DAILY_SOURCE_SCHEMA = {
    "Date": pl.Date,
    "Open": pl.Float32,
    "High": pl.Float32,
    "Low": pl.Float32,
    "Close": pl.Float32,
    "Volume": pl.Float32,
    "DividendAmount": pl.Float32,
    "SplitCoefficient": pl.Float32,
}


def _profile_row(d: date, leveraged: str = "NO", it: float = 0.30,
                 net_assets: float = 1.0e11,
                 holdings: list[dict] | None = None) -> dict:
    return {
        "date": d,
        "information_technology": it,
        "communication_services": 0.10,
        "consumer_discretionary": 0.10,
        "consumer_staples": 0.05,
        "healthcare": 0.13,
        "industrials": 0.08,
        "utilities": 0.03,
        "materials": 0.02,
        "energy": 0.03,
        "financials": 0.13,
        "real_estate": 0.02,
        "other": 0.01,
        "holdings": holdings or [{"symbol": "AAPL", "weight": 0.07}],
        "net_assets": net_assets,
        "net_expense_ratio": 0.0009,
        "portfolio_turnover": None,
        "dividend_yield": 0.014,
        "inception_date": "1993-01-22",
        "leveraged": leveraged,
    }


def _write_profile(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, schema=_PROFILE_SOURCE_SCHEMA).write_parquet(path)


def _write_daily_source(path: Path, rows: list[tuple]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "Date": [r[0] for r in rows],
            "Open": [r[1] for r in rows],
            "High": [r[2] for r in rows],
            "Low": [r[3] for r in rows],
            "Close": [r[4] for r in rows],
            "Volume": [r[5] for r in rows],
            "DividendAmount": [r[6] for r in rows],
            "SplitCoefficient": [r[7] for r in rows],
        },
        schema=_DAILY_SOURCE_SCHEMA,
    ).write_parquet(path)


def _make_overview(rows: list[tuple[str, str, str, str]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": [r[0] for r in rows],
            "assetType": [r[1] for r in rows],
            "about": [r[2] for r in rows],
            "reportedDate": [None] * len(rows),
            "timeOfTheDay": [""] * len(rows),
            "sector": [r[3] for r in rows],
        },
        schema={
            "symbol": pl.Utf8, "assetType": pl.Utf8, "about": pl.Utf8,
            "reportedDate": pl.Date, "timeOfTheDay": pl.Utf8, "sector": pl.Utf8,
        },
    )


# ── 1. Column rename ──────────────────────────────────────────────────────────

def test_normalize_renames_date_lowercase_to_capitalized():
    src = pl.DataFrame([_profile_row(date(2026, 4, 15))], schema=_PROFILE_SOURCE_SCHEMA)
    out = _normalize_profile_source(src)
    assert "Date" in out.columns
    assert "date" not in out.columns
    assert out.schema["Date"] == pl.Date


# ── 2. Drop inception_date ────────────────────────────────────────────────────

def test_normalize_drops_inception_date():
    src = pl.DataFrame([_profile_row(date(2026, 4, 15))], schema=_PROFILE_SOURCE_SCHEMA)
    out = _normalize_profile_source(src)
    assert "inception_date" not in out.columns


def test_build_drops_inception_date_in_final_schema(tmp_path):
    p = tmp_path / "etfs_SPY.parquet"
    _write_profile(p, [_profile_row(date(2026, 4, 15))])
    out = build_etf_profile("SPY", [p], TransformationReport())
    assert "inception_date" not in out.columns
    assert dict(out.schema) == SCHEMAS["etf_profile"]


# ── 3. Cast leveraged to Categorical ──────────────────────────────────────────

def test_leveraged_castable_via_normalize_then_schema(tmp_path):
    """Normalize keeps leveraged as Utf8 (so concat doesn't trip on
    Categorical merges); the final schema cast in build_etf_profile
    converts to Categorical."""
    p = tmp_path / "etfs_SPY.parquet"
    _write_profile(p, [_profile_row(date(2026, 4, 15), leveraged="YES")])
    out = build_etf_profile("SPY", [p], TransformationReport())
    # Compare via dtype.base_type to be tolerant of polars' Categorical
    # ordering metadata variations across versions.
    assert out.schema["leveraged"].base_type() == pl.Categorical
    assert out["leveraged"].to_list() == ["YES"]


# ── 4. List-of-Struct holdings round-trip ─────────────────────────────────────

def test_holdings_list_struct_roundtrip_via_save_load(tmp_path):
    p = tmp_path / "etfs_SPY.parquet"
    holdings = [
        {"symbol": "AAPL", "weight": 0.07},
        {"symbol": "MSFT", "weight": 0.06},
        {"symbol": "GOOGL", "weight": 0.04},
    ]
    _write_profile(p, [_profile_row(date(2026, 4, 15), holdings=holdings)])
    df = build_etf_profile("SPY", [p], TransformationReport())
    # Inspect via .to_list() of the holdings column.
    got = df["holdings"][0].to_list()
    assert {h["symbol"] for h in got} == {"AAPL", "MSFT", "GOOGL"}
    assert pytest.approx(0.07, rel=1e-3) == next(
        h["weight"] for h in got if h["symbol"] == "AAPL"
    )

    # Round-trip through ETFData.save_to / load_from.
    inst = ETFData.default_instance()
    inst.ticker = "SPY"
    inst.about = "SPDR"
    inst.etf_profile = df
    out_dir = tmp_path / "out"
    inst.save_to(out_dir)
    loaded = ETFData.load_from(out_dir)
    got2 = loaded.etf_profile["holdings"][0].to_list()
    assert {h["symbol"] for h in got2} == {"AAPL", "MSFT", "GOOGL"}


# ── 5. Concat of historical + multiple daily ──────────────────────────────────

def test_concat_one_historical_plus_multiple_daily_no_dups(tmp_path):
    h = tmp_path / "h.parquet"
    d1 = tmp_path / "d1.parquet"
    d2 = tmp_path / "d2.parquet"
    _write_profile(h,  [_profile_row(date(2026, 4, 15), it=0.30)])
    _write_profile(d1, [_profile_row(date(2026, 4, 20), it=0.31)])
    _write_profile(d2, [_profile_row(date(2026, 4, 21), it=0.32)])
    df = build_etf_profile("SPY", [h, d1, d2], TransformationReport())
    assert df.height == 3
    dates = df["Date"].to_list()
    assert dates == sorted(dates)


# ── 6. Duplicate-date dedup (defensive) ───────────────────────────────────────

def test_duplicate_date_triggers_dedup_log(tmp_path):
    """Two source files contribute the same Date with mismatched
    information_technology weights. The shared dedup helper fires."""
    h = tmp_path / "h.parquet"
    d = tmp_path / "d.parquet"
    _write_profile(h, [_profile_row(date(2026, 4, 15), it=0.30, net_assets=1.0e11)])
    _write_profile(d, [_profile_row(date(2026, 4, 15), it=0.40, net_assets=1.0e11)])  # >1pct
    report = TransformationReport()
    df = build_etf_profile("SPY", [h, d], report)
    assert df.height == 1
    # Daily wins.
    assert pytest.approx(0.40, rel=1e-3) == df["information_technology"][0]
    rep = report.to_frame()
    assert rep.filter(pl.col("frame") == "etf_profile").height >= 1


# ── 7. Output schema exact ────────────────────────────────────────────────────

def test_output_schema_exact(tmp_path):
    p = tmp_path / "etfs_SPY.parquet"
    _write_profile(p, [_profile_row(date(2026, 4, 15))])
    df = build_etf_profile("SPY", [p], TransformationReport())
    assert dict(df.schema) == SCHEMAS["etf_profile"]


# ── 8. Empty inputs ───────────────────────────────────────────────────────────

def test_empty_paths():
    df = build_etf_profile("SPY", [], TransformationReport())
    assert df.height == 0
    assert dict(df.schema) == SCHEMAS["etf_profile"]


# ── 9. Orchestrator wiring ────────────────────────────────────────────────────

def test_orchestrator_writes_etfdata_with_profile(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"

    _write_daily_source(
        historical / "etfs" / "prices_daily" / "etfs_SPY.parquet",
        [(date(2026, 4, 15), 300, 300, 300, 300, 100000, 0.0, 1.0)],
    )
    _write_profile(
        historical / "etfs" / "etf_profile" / "etfs_SPY.parquet",
        [_profile_row(date(2026, 4, 15))],
    )
    overview = _make_overview([("SPY", "etfs", "SPDR S&P 500", "")])
    n = transform_stocks_or_etfs(
        "etfs", historical, daily, dest, overview, TransformationReport(),
    )
    assert n == 1

    inst = ETFData.load_from(symbol_dest_dir(dest, "etfs", "SPY"))
    assert inst.etf_profile.height == 1
    assert dict(inst.etf_profile.schema) == SCHEMAS["etf_profile"]


def test_orchestrator_only_etfs_get_etf_profile(tmp_path):
    """Stocks must not produce an etf_profile parquet (StockData has no
    such field). Verify by routing a stocks symbol through the orchestrator
    and asserting the saved instance has no etf_profile artifact."""
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"

    _write_daily_source(
        historical / "stocks" / "prices_daily" / "stocks_AAPL.parquet",
        [(date(2026, 4, 15), 100, 100, 100, 100, 1000, 0.0, 1.0)],
    )
    overview = _make_overview([("AAPL", "stocks", "Apple", "Technology")])
    transform_stocks_or_etfs(
        "stocks", historical, daily, dest, overview, TransformationReport(),
    )
    sym_dir = symbol_dest_dir(dest, "stocks", "AAPL")
    assert not (sym_dir / "etf_profile.parquet").exists()


def test_orchestrator_resume_skips_existing_etf(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"

    _write_daily_source(
        historical / "etfs" / "prices_daily" / "etfs_SPY.parquet",
        [(date(2026, 4, 15), 300, 300, 300, 300, 100000, 0.0, 1.0)],
    )
    _write_profile(
        historical / "etfs" / "etf_profile" / "etfs_SPY.parquet",
        [_profile_row(date(2026, 4, 15), it=0.30)],
    )
    overview = _make_overview([("SPY", "etfs", "SPDR", "")])

    transform_stocks_or_etfs(
        "etfs", historical, daily, dest, overview, TransformationReport(),
    )
    assert is_already_transformed(dest, "etfs", "SPY")

    # Mutate profile source; resume must not overwrite.
    _write_profile(
        historical / "etfs" / "etf_profile" / "etfs_SPY.parquet",
        [_profile_row(date(2026, 4, 15), it=0.99)],
    )
    transform_stocks_or_etfs(
        "etfs", historical, daily, dest, overview, TransformationReport(),
    )
    inst = ETFData.load_from(symbol_dest_dir(dest, "etfs", "SPY"))
    assert pytest.approx(0.30, rel=1e-3) == inst.etf_profile["information_technology"][0]


def test_orchestrator_includes_symbols_with_only_profile(tmp_path):
    """A symbol with an etf_profile but no prices_daily is still
    instantiated (the orchestrator iterates the union of source-index
    keys); shareprice_daily/intraday land empty, etf_profile gets the
    one row."""
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"

    _write_profile(
        historical / "etfs" / "etf_profile" / "etfs_SPY.parquet",
        [_profile_row(date(2026, 4, 15))],
    )
    overview = _make_overview([("SPY", "etfs", "SPDR", "")])
    n = transform_stocks_or_etfs(
        "etfs", historical, daily, dest, overview, TransformationReport(),
    )
    assert n == 1
    inst = ETFData.load_from(symbol_dest_dir(dest, "etfs", "SPY"))
    assert inst.etf_profile.height == 1
    assert inst.shareprice_daily.height == 0
    assert inst.shareprice_intraday.height == 0
