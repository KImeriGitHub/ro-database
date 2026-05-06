"""Tests for Phase 3: shareprice_daily for stocks and etfs.

Covers the AdjClose/AdjVolume math, the in-memory factor frame returned for
Phase 4, the schema-level null-row drop, dedup discrepancies across
historical+daily, and the orchestrator entrypoint
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
    FACTOR_FRAME_SCHEMA,
    _compute_adjustment_factors,
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


# ── _compute_adjustment_factors ──────────────────────────────────────────────

def _df_for_factors(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=_SD_SOURCE_SCHEMA)


def test_factors_no_splits_no_divs_are_unity():
    df = _df_for_factors([
        _row(date(2020, 1, 1), 100, 100, 100, 100, 1000),
        _row(date(2020, 1, 2), 100, 100, 100, 100, 1000),
        _row(date(2020, 1, 3), 100, 100, 100, 100, 1000),
    ])
    cs, dv = _compute_adjustment_factors(df)
    assert np.allclose(cs, [1.0, 1.0, 1.0])
    assert np.allclose(dv, [1.0, 1.0, 1.0])


def test_factors_single_split_pre_split_volumes_inflated():
    """4-for-1 split on day 2. Pre-split day must carry cum_split=4; the
    split day itself and after are 1.0 (already in post-split units)."""
    df = _df_for_factors([
        _row(date(2020, 1, 1), 400, 410, 395, 400, 1000),
        _row(date(2020, 1, 2), 100, 105, 95, 100, 4000, sc=4.0),
        _row(date(2020, 1, 3), 100, 105, 95, 105, 4100),
    ])
    cs, dv = _compute_adjustment_factors(df)
    assert np.allclose(cs, [4.0, 1.0, 1.0])
    # No dividends, so div_factor stays unity.
    assert np.allclose(dv, [1.0, 1.0, 1.0])


def test_factors_single_dividend_pre_div_close_discounted():
    """A $1 dividend on day 2 with day-1 close of $100 yields a 0.99 step
    factor for day-1 (and earlier); ex-div day and after are 1.0."""
    df = _df_for_factors([
        _row(date(2020, 1, 1), 100, 100, 100, 100, 1000),
        _row(date(2020, 1, 2), 99,  99,  99,  99,  1000, div=1.0),
        _row(date(2020, 1, 3), 99,  99,  99,  99,  1000),
    ])
    cs, dv = _compute_adjustment_factors(df)
    assert np.allclose(cs, [1.0, 1.0, 1.0])
    assert np.allclose(dv, [0.99, 1.0, 1.0])


def test_factors_split_and_div_compose():
    """Day 1 close=100. Day 2: $1 div (pre-split price). Day 3: 4-for-1
    split. cum_split[1]=cum_split[2]=4, cum_split[3]=1. div_factor
    accumulates the day-2 div for day-1 only."""
    df = _df_for_factors([
        _row(date(2020, 1, 1), 100, 100, 100, 100, 1000),
        _row(date(2020, 1, 2), 99,  99,  99,  99,  1000, div=1.0),
        _row(date(2020, 1, 3), 24.75, 24.75, 24.75, 24.75, 4000, sc=4.0),
    ])
    cs, dv = _compute_adjustment_factors(df)
    assert np.allclose(cs, [4.0, 4.0, 1.0])
    # day-1 div factor = (100 - 1) / 100 = 0.99; day-2 has no future div
    assert np.allclose(dv, [0.99, 1.0, 1.0])


def test_factors_zero_close_safe():
    """A zero/null Close must not raise; that step's div factor falls back
    to 1.0 (no adjustment for that step)."""
    df = _df_for_factors([
        _row(date(2020, 1, 1), 0,   0,   0,   0,   1000),
        _row(date(2020, 1, 2), 100, 100, 100, 100, 1000, div=1.0),
    ])
    cs, dv = _compute_adjustment_factors(df)
    assert np.all(np.isfinite(dv))
    # day-1 stepping forward to day-2 has Close=0 -> step=1.0
    assert np.allclose(dv, [1.0, 1.0])


def test_factors_empty_frame():
    df = pl.DataFrame(schema=_SD_SOURCE_SCHEMA)
    cs, dv = _compute_adjustment_factors(df)
    assert cs.shape == (0,)
    assert dv.shape == (0,)


# ── build_shareprice_daily ───────────────────────────────────────────────────

def test_build_shareprice_daily_empty_paths():
    sp, factor = build_shareprice_daily("stocks", "X", [], TransformationReport())
    assert sp.height == 0
    assert dict(sp.schema) == SCHEMAS["shareprice_daily"]
    assert factor.height == 0
    assert dict(factor.schema) == FACTOR_FRAME_SCHEMA


def test_build_shareprice_daily_simple(tmp_path):
    p = tmp_path / "stocks_AAPL.parquet"
    _write_sp_source(p, [
        _row(date(2020, 1, 1), 100, 100, 100, 100, 1000),
        _row(date(2020, 1, 2), 100, 100, 100, 100, 1000),
    ])
    sp, factor = build_shareprice_daily("stocks", "AAPL", [p], TransformationReport())
    assert sp.height == 2
    assert dict(sp.schema) == SCHEMAS["shareprice_daily"]
    assert dict(factor.schema) == FACTOR_FRAME_SCHEMA
    # No splits / divs -> AdjClose==Close, AdjVolume==Volume.
    assert sp["AdjClose"].to_list() == [100.0, 100.0]
    assert sp["AdjVolume"].to_list() == [1000.0, 1000.0]


def test_build_shareprice_daily_dividend_adjusts_pre_div_close(tmp_path):
    p = tmp_path / "stocks_AAPL.parquet"
    _write_sp_source(p, [
        _row(date(2020, 1, 1), 100, 100, 100, 100, 1000),
        _row(date(2020, 1, 2), 99,  99,  99,  99,  1000, div=1.0),
    ])
    sp, _ = build_shareprice_daily("stocks", "AAPL", [p], TransformationReport())
    # Day-1 AdjClose = 100 * 0.99 = 99 (matches day-2 close, continuous).
    assert pytest.approx(99.0, rel=1e-3) == sp["AdjClose"][0]
    assert pytest.approx(99.0, rel=1e-3) == sp["AdjClose"][1]
    # Volume not affected by dividends.
    assert sp["AdjVolume"].to_list() == [1000.0, 1000.0]


def test_build_shareprice_daily_split_inflates_pre_split_volume(tmp_path):
    p = tmp_path / "stocks_AAPL.parquet"
    _write_sp_source(p, [
        _row(date(2020, 1, 1), 400, 400, 400, 400, 1000),
        _row(date(2020, 1, 2), 100, 100, 100, 100, 4000, sc=4.0),
    ])
    sp, _ = build_shareprice_daily("stocks", "AAPL", [p], TransformationReport())
    # Day-1 Volume * 4, day-2 unchanged.
    assert sp["AdjVolume"].to_list() == [4000.0, 4000.0]
    # AdjClose folds in both splits and dividends (CRSP convention):
    # day-1 pre-split $400 / 4 = $100, continuous with day-2 post-split $100.
    assert sp["AdjClose"].to_list() == [100.0, 100.0]


def test_factor_frame_aligns_with_surviving_dates(tmp_path):
    """A row dropped due to null Open must NOT appear in the factor frame."""
    p = tmp_path / "stocks_AAPL.parquet"
    _write_sp_source(p, [
        _row(date(2020, 1, 1), 100,  100, 100, 100, 1000),
        _row(date(2020, 1, 2), None, 100, 100, 100, 1000),  # Open null -> drop
        _row(date(2020, 1, 3), 100,  100, 100, 100, 1000),
    ])
    sp, factor = build_shareprice_daily("stocks", "AAPL", [p], TransformationReport())
    assert sp.height == 2
    assert factor.height == 2
    assert factor["Date"].to_list() == sp["Date"].to_list()


def test_null_dropped_row_logged(tmp_path):
    p = tmp_path / "stocks_AAPL.parquet"
    _write_sp_source(p, [
        _row(date(2020, 1, 1), 100, 100, 100, 100, 1000),
        _row(date(2020, 1, 2), None, 100, 100, 100, 1000),
        _row(date(2020, 1, 3), 100, 100, 100, 100, None),  # Volume null -> drop
    ])
    report = TransformationReport()
    sp, _ = build_shareprice_daily("stocks", "AAPL", [p], report)
    assert sp.height == 1
    rep = report.to_frame().filter(pl.col("issue_type") == "dedup_dropped_null_row")
    assert rep.height == 1
    assert rep["count"][0] == 2


def test_dedup_under_1pct_logged(tmp_path):
    """Two source files overlap on a date with Close differing by <1%."""
    h = tmp_path / "historical" / "stocks_AAPL.parquet"
    d = tmp_path / "daily" / "stocks_AAPL.parquet"
    _write_sp_source(h, [_row(date(2020, 1, 1), 100, 100, 100, 100, 1000)])
    _write_sp_source(d, [_row(date(2020, 1, 1), 100, 100, 100, 100.5, 1000)])
    report = TransformationReport()
    sp, _ = build_shareprice_daily("stocks", "AAPL", [h, d], report)
    assert sp.height == 1
    # daily snapshot wins.
    assert pytest.approx(100.5, rel=1e-4) == sp["Close"][0]
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
