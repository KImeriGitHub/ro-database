"""Tests for Phase 3: shareprice_daily for stocks and etfs.

Covers the single-day ``AdjFactor`` math (CRSP convention, anchored on
the prior close), the schema-level null-row drop, dedup discrepancies
across historical+daily, and the orchestrator entrypoint
``transform_stocks_or_etfs``.
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from data_transformation._common import (
    TransformationReport,
    is_already_transformed,
    symbol_dest_dir,
)
from data_transformation.AssetData import (
    CANONICAL_SECTORS,
    ETFData,
    StockData,
)
from data_transformation.AssetDataService import SCHEMAS
from data_transformation.frames.price_daily import (
    _compute_adj_factor,
    build_shareprice_daily,
)
from data_transformation.frames.stocks_etfs import transform_stocks_or_etfs


# ── helpers ───────────────────────────────────────────────────────────────────

_SD_SOURCE_SCHEMA = {
    "Date": pl.Date,
    "Open": pl.Float32,
    "High": pl.Float32,
    "Low": pl.Float32,
    "Close": pl.Float32,
    "Volume": pl.Float32,
    "DividendAmount": pl.Float32,
    "SplitCoefficient": pl.Float32,
}


def _write_sp_source(path: Path, rows: list[dict]) -> None:
    """rows: list of dicts with the 8 prices_daily columns."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, schema=_SD_SOURCE_SCHEMA).write_parquet(path)


def _row(d, o, h, l, c, v, div=0.0, sc=1.0) -> dict:
    return {
        "Date": d, "Open": o, "High": h, "Low": l, "Close": c,
        "Volume": v, "DividendAmount": div, "SplitCoefficient": sc,
    }


def _make_overview(rows: list[tuple[str, str, str, str]]) -> pl.DataFrame:
    """rows: list of (symbol, assetType, about, sector)."""
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


# ── _compute_adj_factor ──────────────────────────────────────────────────────

def _df_for_factors(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=_SD_SOURCE_SCHEMA)


def test_adj_factor_no_splits_no_divs_are_unity():
    df = _df_for_factors([
        _row(date(2020, 1, 1), 100, 100, 100, 100, 1000),
        _row(date(2020, 1, 2), 100, 100, 100, 100, 1000),
        _row(date(2020, 1, 3), 100, 100, 100, 100, 1000),
    ])
    af = _compute_adj_factor(df)
    assert np.allclose(af, [1.0, 1.0, 1.0])


def test_adj_factor_first_row_is_one_by_convention():
    """No prior close to anchor on -> AdjFactor[0] == 1.0 even if that
    row carries a split or dividend coefficient."""
    df = _df_for_factors([
        _row(date(2020, 1, 1), 100, 100, 100, 100, 1000, div=1.0, sc=2.0),
        _row(date(2020, 1, 2), 100, 100, 100, 100, 1000),
    ])
    af = _compute_adj_factor(df)
    assert af[0] == 1.0


def test_adj_factor_split_only():
    """4-for-1 split on day 2. AdjFactor[1] = SC = 4 (no dividend)."""
    df = _df_for_factors([
        _row(date(2020, 1, 1), 400, 410, 395, 400, 1000),
        _row(date(2020, 1, 2), 100, 105, 95, 100, 4000, sc=4.0),
        _row(date(2020, 1, 3), 100, 105, 95, 105, 4100),
    ])
    af = _compute_adj_factor(df)
    assert np.allclose(af, [1.0, 4.0, 1.0])


def test_adj_factor_dividend_only():
    """A $1 dividend on day 2 with prior close $100 yields
    AdjFactor[1] = 100 / (100 - 1) = 100/99."""
    df = _df_for_factors([
        _row(date(2020, 1, 1), 100, 100, 100, 100, 1000),
        _row(date(2020, 1, 2), 99,  99,  99,  99,  1000, div=1.0),
        _row(date(2020, 1, 3), 99,  99,  99,  99,  1000),
    ])
    af = _compute_adj_factor(df)
    assert af[0] == 1.0
    assert np.isclose(af[1], 100.0 / 99.0)
    assert af[2] == 1.0


def test_adj_factor_split_and_div_compose():
    """Day 2 has both a $1 dividend and a 4-for-1 split, prior close 100.
    AdjFactor[1] = 4 * 100/99."""
    df = _df_for_factors([
        _row(date(2020, 1, 1), 100, 100, 100, 100, 1000),
        _row(date(2020, 1, 2), 24.75, 24.75, 24.75, 24.75, 4000, div=1.0, sc=4.0),
    ])
    af = _compute_adj_factor(df)
    assert np.isclose(af[1], 4.0 * 100.0 / 99.0)


def test_adj_factor_zero_prior_close_safe():
    """A zero/null prior Close must not raise; the dividend ratio falls
    back to 1.0 and the split coefficient still applies."""
    df = _df_for_factors([
        _row(date(2020, 1, 1), 0,   0,   0,   0,   1000),
        _row(date(2020, 1, 2), 100, 100, 100, 100, 1000, div=1.0, sc=2.0),
    ])
    af = _compute_adj_factor(df)
    assert np.all(np.isfinite(af))
    # split coefficient retained, dividend ratio collapsed to 1.0
    assert np.isclose(af[1], 2.0)


def test_adj_factor_dividend_eq_close_safe():
    """If the dividend equals (or exceeds) the prior close the
    denominator collapses; the safety net falls back to SC * 1.0."""
    df = _df_for_factors([
        _row(date(2020, 1, 1), 1.0, 1.0, 1.0, 1.0, 1000),
        _row(date(2020, 1, 2), 0.5, 0.5, 0.5, 0.5, 1000, div=1.0),
    ])
    af = _compute_adj_factor(df)
    assert np.all(np.isfinite(af))
    assert np.isclose(af[1], 1.0)


def test_adj_factor_empty_frame():
    df = pl.DataFrame(schema=_SD_SOURCE_SCHEMA)
    af = _compute_adj_factor(df)
    assert af.shape == (0,)


def test_adj_factor_total_return_identity():
    """Smoke test of the headline identity:
    Close[i] * AdjFactor[i] / Close[i-1] - 1 ≈ total return on day i.

    Construct a sequence where the day-2 ex-div price drop is exactly
    the dividend amount (no real price move): the implied total return
    should be 0.
    """
    df = _df_for_factors([
        _row(date(2020, 1, 1), 100.0, 100.0, 100.0, 100.0, 1000),
        _row(date(2020, 1, 2),  99.0,  99.0,  99.0,  99.0, 1000, div=1.0),
    ])
    af = _compute_adj_factor(df)
    closes = df["Close"].to_numpy().astype(np.float64)
    total_ret = closes[1] * af[1] / closes[0] - 1.0
    assert abs(total_ret) < 1e-9


# ── build_shareprice_daily ───────────────────────────────────────────────────

def test_build_shareprice_daily_empty_paths():
    sp = build_shareprice_daily("stocks", "X", [], TransformationReport())
    assert sp.height == 0
    assert dict(sp.schema) == SCHEMAS["shareprice_daily"]


def test_build_shareprice_daily_simple(tmp_path):
    p = tmp_path / "stocks_AAPL.parquet"
    _write_sp_source(p, [
        _row(date(2020, 1, 1), 100, 100, 100, 100, 1000),
        _row(date(2020, 1, 2), 100, 100, 100, 100, 1000),
    ])
    sp = build_shareprice_daily("stocks", "AAPL", [p], TransformationReport())
    assert sp.height == 2
    assert dict(sp.schema) == SCHEMAS["shareprice_daily"]
    # No splits / divs -> AdjFactor == 1.0 on every row, OHLCV stays raw.
    assert sp["AdjFactor"].to_list() == [1.0, 1.0]
    assert sp["Close"].to_list() == [100.0, 100.0]
    assert sp["Volume"].to_list() == [1000.0, 1000.0]


def test_build_shareprice_daily_dividend_sets_adj_factor(tmp_path):
    p = tmp_path / "stocks_AAPL.parquet"
    _write_sp_source(p, [
        _row(date(2020, 1, 1), 100, 100, 100, 100, 1000),
        _row(date(2020, 1, 2), 99,  99,  99,  99,  1000, div=1.0),
    ])
    sp = build_shareprice_daily("stocks", "AAPL", [p], TransformationReport())
    # AdjFactor[0] = 1.0; AdjFactor[1] = 100 / (100 - 1) = 100/99.
    assert sp["AdjFactor"][0] == pytest.approx(1.0, rel=1e-6)
    assert sp["AdjFactor"][1] == pytest.approx(100.0 / 99.0, rel=1e-6)
    # OHLCV stays raw; no AdjClose / AdjVolume column.
    assert "AdjClose" not in sp.columns
    assert "AdjVolume" not in sp.columns
    assert sp["Close"].to_list() == [100.0, 99.0]


def test_build_shareprice_daily_split_sets_adj_factor(tmp_path):
    p = tmp_path / "stocks_AAPL.parquet"
    _write_sp_source(p, [
        _row(date(2020, 1, 1), 400, 400, 400, 400, 1000),
        _row(date(2020, 1, 2), 100, 100, 100, 100, 4000, sc=4.0),
    ])
    sp = build_shareprice_daily("stocks", "AAPL", [p], TransformationReport())
    # AdjFactor[1] = SplitCoefficient * Close[0] / (Close[0] - 0) = 4 * 1 = 4.
    assert sp["AdjFactor"].to_list() == [1.0, 4.0]
    # OHLCV raw, post-split values intact.
    assert sp["Volume"].to_list() == [1000.0, 4000.0]


def test_null_dropped_row_logged(tmp_path):
    p = tmp_path / "stocks_AAPL.parquet"
    _write_sp_source(p, [
        _row(date(2020, 1, 1), 100, 100, 100, 100, 1000),
        _row(date(2020, 1, 2), None, 100, 100, 100, 1000),
        _row(date(2020, 1, 3), 100, 100, 100, 100, None),  # Volume null -> drop
    ])
    report = TransformationReport()
    sp = build_shareprice_daily("stocks", "AAPL", [p], report)
    assert sp.height == 1
    rep = report.to_frame().filter(pl.col("issue_type") == "dedup_dropped_null_row")
    assert rep.height == 1
    assert rep["count"][0] == 2


def test_dedup_under_1pct_logged(tmp_path):
    """Two source files overlap on a date with Close differing by <1%.

    Historic extends past the daily-overlap date so the overlap is on an
    *interior* date -- the boundary-suppression rule for price frames
    silences discrepancies on max(historic Date) only.
    """
    h = tmp_path / "historical" / "stocks_AAPL.parquet"
    d = tmp_path / "daily" / "stocks_AAPL.parquet"
    _write_sp_source(h, [
        _row(date(2020, 1, 1), 100, 100, 100, 100, 1000),
        _row(date(2020, 1, 2), 101, 101, 101, 101, 1000),
    ])
    _write_sp_source(d, [_row(date(2020, 1, 1), 100, 100, 100, 100.5, 1000)])
    report = TransformationReport()
    sp = build_shareprice_daily("stocks", "AAPL", [h, d], report)
    assert sp.height == 2
    # daily snapshot wins on the overlap.
    jan1 = sp.filter(pl.col("Date") == date(2020, 1, 1))
    assert pytest.approx(100.5, rel=1e-4) == jan1["Close"][0]
    rep = report.to_frame().filter(
        pl.col("issue_type") == "dedup_value_discrepancy_under_1pct"
    )
    assert rep.height == 1


# ── transform_stocks_or_etfs (orchestrator entrypoint) ─────────

def test_orchestrator_writes_stockdata_with_sector(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"
    p = historical / "stocks" / "prices_daily" / "stocks_AAPL.parquet"
    _write_sp_source(p, [_row(date(2020, 1, 1), 100, 100, 100, 100, 1000)])

    overview = _make_overview([("AAPL", "stocks", "Apple Inc", "Technology")])
    report = TransformationReport()
    n = transform_stocks_or_etfs(
        "stocks", historical, daily, dest, overview, report,
    )
    assert n == 1
    inst = StockData.load_from(symbol_dest_dir(dest, "stocks", "AAPL"))
    assert inst.ticker == "AAPL"
    assert inst.about == "Apple Inc"
    assert inst.sector == CANONICAL_SECTORS.index("Technology")
    assert inst.shareprice_daily.height == 1
    assert dict(inst.shareprice_daily.schema) == SCHEMAS["shareprice_daily"]
    # Phase 4/5 not run yet -> these stay empty but schema-correct.
    assert inst.shareprice_intraday.height == 0
    assert dict(inst.shareprice_intraday.schema) == SCHEMAS["shareprice_intraday"]


def test_orchestrator_writes_etfdata_no_sector(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"
    p = historical / "etfs" / "prices_daily" / "etfs_SPY.parquet"
    _write_sp_source(p, [_row(date(2020, 1, 1), 300, 300, 300, 300, 100000)])

    overview = _make_overview([("SPY", "etfs", "SPDR S&P 500", "")])
    n = transform_stocks_or_etfs(
        "etfs", historical, daily, dest, overview, TransformationReport(),
    )
    assert n == 1
    inst = ETFData.load_from(symbol_dest_dir(dest, "etfs", "SPY"))
    assert inst.ticker == "SPY"
    assert inst.about == "SPDR S&P 500"
    assert inst.shareprice_daily.height == 1


def test_orchestrator_unknown_sector_falls_back_to_other(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"
    p = historical / "stocks" / "prices_daily" / "stocks_AAPL.parquet"
    _write_sp_source(p, [_row(date(2020, 1, 1), 100, 100, 100, 100, 1000)])
    overview = _make_overview([("AAPL", "stocks", "Apple", "Made up sector")])
    transform_stocks_or_etfs(
        "stocks", historical, daily, dest, overview, TransformationReport(),
    )
    inst = StockData.load_from(symbol_dest_dir(dest, "stocks", "AAPL"))
    assert inst.sector == CANONICAL_SECTORS.index("Other")


def test_orchestrator_concat_historical_plus_daily_folders(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"
    _write_sp_source(
        historical / "stocks" / "prices_daily" / "stocks_AAPL.parquet",
        [_row(date(2020, 1, 1), 100, 100, 100, 100, 1000)],
    )
    _write_sp_source(
        daily / "2026-04-01" / "stocks" / "prices_daily" / "stocks_AAPL.parquet",
        [_row(date(2026, 4, 1), 200, 200, 200, 200, 2000)],
    )
    overview = _make_overview([("AAPL", "stocks", "Apple", "Technology")])
    transform_stocks_or_etfs(
        "stocks", historical, daily, dest, overview, TransformationReport(),
    )
    inst = StockData.load_from(symbol_dest_dir(dest, "stocks", "AAPL"))
    assert inst.shareprice_daily.height == 2
    assert inst.shareprice_daily["Date"].to_list() == [date(2020, 1, 1), date(2026, 4, 1)]


def test_orchestrator_resume_skips_existing(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"
    p = historical / "stocks" / "prices_daily" / "stocks_AAPL.parquet"
    _write_sp_source(p, [_row(date(2020, 1, 1), 100, 100, 100, 100, 1000)])
    overview = _make_overview([("AAPL", "stocks", "Apple", "Technology")])

    transform_stocks_or_etfs(
        "stocks", historical, daily, dest, overview, TransformationReport(),
    )
    assert is_already_transformed(dest, "stocks", "AAPL")
    # Mutate source.
    _write_sp_source(p, [_row(date(2020, 1, 1), 999, 999, 999, 999, 1)])
    transform_stocks_or_etfs(
        "stocks", historical, daily, dest, overview, TransformationReport(),
    )
    inst = StockData.load_from(symbol_dest_dir(dest, "stocks", "AAPL"))
    assert inst.shareprice_daily["Close"][0] == 100.0  # unchanged


def test_orchestrator_symbols_filter(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"
    for sym in ("AAPL", "MSFT"):
        _write_sp_source(
            historical / "stocks" / "prices_daily" / f"stocks_{sym}.parquet",
            [_row(date(2020, 1, 1), 100, 100, 100, 100, 1000)],
        )
    overview = _make_overview([
        ("AAPL", "stocks", "Apple", "Technology"),
        ("MSFT", "stocks", "Microsoft", "Technology"),
    ])
    n = transform_stocks_or_etfs(
        "stocks", historical, daily, dest, overview, TransformationReport(),
        symbols_filter={"AAPL"},
    )
    assert n == 1
    assert is_already_transformed(dest, "stocks", "AAPL")
    assert not is_already_transformed(dest, "stocks", "MSFT")


def test_orchestrator_metadata_json_carries_sector_index(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"
    p = historical / "stocks" / "prices_daily" / "stocks_AAPL.parquet"
    _write_sp_source(p, [_row(date(2020, 1, 1), 100, 100, 100, 100, 1000)])
    overview = _make_overview([("AAPL", "stocks", "Apple", "Healthcare")])
    transform_stocks_or_etfs(
        "stocks", historical, daily, dest, overview, TransformationReport(),
    )
    md = json.loads((symbol_dest_dir(dest, "stocks", "AAPL") / "metadata.json").read_text())
    assert md["_asset_type"] == "StockData"
    assert md["sector"] == CANONICAL_SECTORS.index("Healthcare")


def test_orchestrator_unsupported_asset_type_rejected(tmp_path):
    overview = _make_overview([])
    with pytest.raises(ValueError, match="does not handle"):
        transform_stocks_or_etfs(
            "forex", tmp_path, tmp_path, tmp_path, overview,
            TransformationReport(),
        )
