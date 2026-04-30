"""Tests for data_transformation/frames/overview.py."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from data_transformation.frames.overview import (
    OVERVIEW_SCHEMA,
    build_assets_overview,
    write_assets_overview,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def catalog_dir(tmp_path: Path) -> Path:
    """A complete catalog/ tree with two stocks, one ETF, one forex pair, one
    commodity, one economic indicator, plus an earnings_calendar.
    """
    cat = tmp_path / "catalog"
    cat.mkdir()

    pl.DataFrame({
        "symbol": ["AAPL", "MSFT"],
        "name": ["Apple Inc", "Microsoft Corp"],
        "sector": ["Technology", "Technology"],
    }).write_parquet(cat / "stocks.parquet")

    pl.DataFrame({
        "symbol": ["SPY"],
        "name": ["SPDR S&P 500 ETF"],
    }).write_parquet(cat / "etfs.parquet")

    pl.DataFrame({
        "symbol": ["EURUSD"],
        "name": ["Euro / US Dollar"],
    }).write_parquet(cat / "forex.parquet")

    pl.DataFrame({
        "symbol": ["SPX"],
        "name": ["S&P 500 Index"],
    }).write_parquet(cat / "indices.parquet")

    pl.DataFrame({
        "symbol": ["BTC"],
        "name": ["Bitcoin"],
    }).write_parquet(cat / "cryptocurrencies.parquet")

    pl.DataFrame({
        "symbol": ["WTI"],
        "name": ["WTI Crude Oil"],
    }).write_parquet(cat / "commodities.parquet")

    pl.DataFrame({
        "symbol": ["CPI"],
        "name": ["Consumer Price Index"],
    }).write_parquet(cat / "economic.parquet")

    pl.DataFrame({
        "symbol": ["AAPL", "AAPL", "MSFT"],
        "name": ["Apple Inc", "Apple Inc", "Microsoft Corp"],
        "reportedDate": [
            date(2026, 5, 1),   # next AAPL earnings
            date(2026, 8, 1),   # later AAPL earnings (should be ignored)
            date(2026, 4, 25),  # MSFT earnings already past relative to today=2026-04-28
        ],
        "timeOfTheDay": ["pre-market", "post-market", "post-market"],
    }, schema={
        "symbol": pl.Utf8, "name": pl.Utf8, "reportedDate": pl.Date,
        "timeOfTheDay": pl.Utf8,
    }).write_parquet(cat / "earnings_calendar.parquet")

    return cat


# ── Schema and shape ──────────────────────────────────────────────────────────

def test_overview_schema_exact(catalog_dir):
    df = build_assets_overview(catalog_dir, today=date(2026, 4, 28))
    assert dict(df.schema) == OVERVIEW_SCHEMA


def test_overview_one_row_per_catalog_symbol(catalog_dir):
    df = build_assets_overview(catalog_dir, today=date(2026, 4, 28))
    # 2 stocks + 1 etf + 1 forex + 1 indices + 1 crypto + 1 commodity + 1 economic = 8
    assert df.height == 8
    assert set(df["assetType"].unique().to_list()) == {
        "stocks", "etfs", "forex", "indices",
        "cryptocurrencies", "commodities", "economic",
    }


def test_overview_sorted_by_assettype_then_symbol(catalog_dir):
    df = build_assets_overview(catalog_dir, today=date(2026, 4, 28))
    pairs = list(zip(df["assetType"].to_list(), df["symbol"].to_list()))
    assert pairs == sorted(pairs)


# ── about / sector ────────────────────────────────────────────────────────────

def test_about_populated_from_name(catalog_dir):
    df = build_assets_overview(catalog_dir, today=date(2026, 4, 28))
    aapl = df.filter(pl.col("symbol") == "AAPL").row(0, named=True)
    assert aapl["about"] == "Apple Inc"


def test_sector_only_for_stocks(catalog_dir):
    df = build_assets_overview(catalog_dir, today=date(2026, 4, 28))
    for row in df.iter_rows(named=True):
        if row["assetType"] == "stocks":
            assert row["sector"] != ""
        else:
            assert row["sector"] == ""


# ── reportedDate / timeOfTheDay ─────────────────────────────────────────────────

def test_next_upcoming_earnings_picked(catalog_dir):
    """AAPL has two future earnings; the earliest (2026-05-01) wins."""
    df = build_assets_overview(catalog_dir, today=date(2026, 4, 28))
    aapl = df.filter(pl.col("symbol") == "AAPL").row(0, named=True)
    assert aapl["reportedDate"] == date(2026, 5, 1)
    assert aapl["timeOfTheDay"] == "pre-market"


def test_past_earnings_dropped(catalog_dir):
    """MSFT's only earnings row is on 2026-04-25, before today=2026-04-28; it
    must not be assigned (reportedDate stays null, timeOfTheDay stays "")."""
    df = build_assets_overview(catalog_dir, today=date(2026, 4, 28))
    msft = df.filter(pl.col("symbol") == "MSFT").row(0, named=True)
    assert msft["reportedDate"] is None
    assert msft["timeOfTheDay"] == ""


def test_symbol_with_no_earnings_entry(catalog_dir):
    df = build_assets_overview(catalog_dir, today=date(2026, 4, 28))
    spy = df.filter(pl.col("symbol") == "SPY").row(0, named=True)
    assert spy["reportedDate"] is None
    assert spy["timeOfTheDay"] == ""


# ── Nulls -> empty strings ────────────────────────────────────────────────────

def test_utf8_nulls_become_empty_strings(catalog_dir):
    df = build_assets_overview(catalog_dir, today=date(2026, 4, 28))
    # No null Utf8 cell in the result.
    for col in ("about", "timeOfTheDay", "sector"):
        assert df[col].null_count() == 0
    # reportedDate may be null and that is intentional.
    assert df["reportedDate"].null_count() > 0


# ── Robustness ────────────────────────────────────────────────────────────────

def test_missing_earnings_calendar(catalog_dir):
    (catalog_dir / "earnings_calendar.parquet").unlink()
    df = build_assets_overview(catalog_dir, today=date(2026, 4, 28))
    assert df.height == 8
    assert df["reportedDate"].null_count() == df.height
    assert (df["timeOfTheDay"] == "").all()


def test_missing_stocks_catalog_means_no_sector(catalog_dir):
    (catalog_dir / "stocks.parquet").unlink()
    df = build_assets_overview(catalog_dir, today=date(2026, 4, 28))
    assert (df["sector"] == "").all()


def test_empty_catalog_dir_returns_empty_frame(tmp_path):
    df = build_assets_overview(tmp_path / "empty", today=date(2026, 4, 28))
    assert df.height == 0
    assert dict(df.schema) == OVERVIEW_SCHEMA


# ── write_assets_overview ─────────────────────────────────────────────────────

def test_write_assets_overview_roundtrip(catalog_dir, tmp_path):
    dest = tmp_path / "transformed"
    out = write_assets_overview(catalog_dir, dest, today=date(2026, 4, 28))
    assert out == dest / "assets_overview.parquet"
    reloaded = pl.read_parquet(out)
    assert dict(reloaded.schema) == OVERVIEW_SCHEMA
    assert reloaded.height == 8
