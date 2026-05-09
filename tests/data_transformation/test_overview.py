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
    commodity, one economic indicator. ``earnings_calendar.parquet`` lives
    in ``historical_dir`` (see fixture below), not under catalog/.
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

    return cat


@pytest.fixture
def historical_dir(catalog_dir: Path) -> Path:
    """Mirror of the production ``historical/`` folder, sitting alongside
    ``catalog_dir``. Holds ``earnings_calendar.parquet`` (which moved out of
    catalog/ when it became part of every data-pull folder)."""
    hist = catalog_dir.parent / "historical"
    hist.mkdir()

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
    }).write_parquet(hist / "earnings_calendar.parquet")

    return hist


# ── Schema and shape ──────────────────────────────────────────────────────────

def test_overview_schema_exact(catalog_dir, historical_dir):
    df = build_assets_overview(
        catalog_dir, today=date(2026, 4, 28), historical_dir=historical_dir,
    )
    assert dict(df.schema) == OVERVIEW_SCHEMA


def test_overview_one_row_per_catalog_symbol(catalog_dir, historical_dir):
    df = build_assets_overview(
        catalog_dir, today=date(2026, 4, 28), historical_dir=historical_dir,
    )
    # 2 stocks + 1 etf + 1 forex + 1 indices + 1 crypto + 1 commodity + 1 economic = 8
    assert df.height == 8
    assert set(df["assetType"].unique().to_list()) == {
        "stocks", "etfs", "forex", "indices",
        "cryptocurrencies", "commodities", "economic",
    }


def test_overview_sorted_by_assettype_then_symbol(catalog_dir, historical_dir):
    df = build_assets_overview(
        catalog_dir, today=date(2026, 4, 28), historical_dir=historical_dir,
    )
    pairs = list(zip(df["assetType"].to_list(), df["symbol"].to_list()))
    assert pairs == sorted(pairs)


# ── about / sector ────────────────────────────────────────────────────────────

def test_about_populated_from_name(catalog_dir, historical_dir):
    df = build_assets_overview(
        catalog_dir, today=date(2026, 4, 28), historical_dir=historical_dir,
    )
    aapl = df.filter(pl.col("symbol") == "AAPL").row(0, named=True)
    assert aapl["about"] == "Apple Inc"


def test_sector_only_for_stocks(catalog_dir, historical_dir):
    df = build_assets_overview(
        catalog_dir, today=date(2026, 4, 28), historical_dir=historical_dir,
    )
    for row in df.iter_rows(named=True):
        if row["assetType"] == "stocks":
            assert row["sector"] != ""
        else:
            assert row["sector"] == ""


# ── reportedDate / timeOfTheDay ─────────────────────────────────────────────────

def test_next_upcoming_earnings_picked(catalog_dir, historical_dir):
    """AAPL has two future earnings; the earliest (2026-05-01) wins."""
    df = build_assets_overview(
        catalog_dir, today=date(2026, 4, 28), historical_dir=historical_dir,
    )
    aapl = df.filter(pl.col("symbol") == "AAPL").row(0, named=True)
    assert aapl["reportedDate"] == date(2026, 5, 1)
    assert aapl["timeOfTheDay"] == "pre-market"


def test_past_earnings_dropped(catalog_dir, historical_dir):
    """MSFT's only earnings row is on 2026-04-25, before today=2026-04-28; it
    must not be assigned (reportedDate stays null, timeOfTheDay stays "")."""
    df = build_assets_overview(
        catalog_dir, today=date(2026, 4, 28), historical_dir=historical_dir,
    )
    msft = df.filter(pl.col("symbol") == "MSFT").row(0, named=True)
    assert msft["reportedDate"] is None
    assert msft["timeOfTheDay"] == ""


def test_symbol_with_no_earnings_entry(catalog_dir, historical_dir):
    df = build_assets_overview(
        catalog_dir, today=date(2026, 4, 28), historical_dir=historical_dir,
    )
    spy = df.filter(pl.col("symbol") == "SPY").row(0, named=True)
    assert spy["reportedDate"] is None
    assert spy["timeOfTheDay"] == ""


# ── Nulls -> empty strings ────────────────────────────────────────────────────

def test_utf8_nulls_become_empty_strings(catalog_dir, historical_dir):
    df = build_assets_overview(
        catalog_dir, today=date(2026, 4, 28), historical_dir=historical_dir,
    )
    # No null Utf8 cell in the result.
    for col in ("about", "timeOfTheDay", "sector"):
        assert df[col].null_count() == 0
    # reportedDate may be null and that is intentional.
    assert df["reportedDate"].null_count() > 0


# ── Robustness ────────────────────────────────────────────────────────────────

def test_missing_earnings_calendar(catalog_dir, historical_dir):
    (historical_dir / "earnings_calendar.parquet").unlink()
    df = build_assets_overview(
        catalog_dir, today=date(2026, 4, 28), historical_dir=historical_dir,
    )
    assert df.height == 8
    assert df["reportedDate"].null_count() == df.height
    assert (df["timeOfTheDay"] == "").all()


def test_daily_folder_takes_precedence_over_historical(
    catalog_dir, historical_dir, tmp_path,
):
    """A newer ``daily/<date>/earnings_calendar.parquet`` overrides the
    historical copy. Only the latest date folder is read."""
    daily = tmp_path / "daily"
    older = daily / "2026-04-20"
    newer = daily / "2026-04-27"
    older.mkdir(parents=True)
    newer.mkdir(parents=True)

    # Older copy has the AAPL row at 2026-05-01; newer copy moves it to 2026-06-15.
    pl.DataFrame({
        "symbol": ["AAPL"],
        "name": ["Apple Inc"],
        "reportedDate": [date(2026, 5, 1)],
        "timeOfTheDay": ["pre-market"],
    }, schema={
        "symbol": pl.Utf8, "name": pl.Utf8, "reportedDate": pl.Date,
        "timeOfTheDay": pl.Utf8,
    }).write_parquet(older / "earnings_calendar.parquet")
    pl.DataFrame({
        "symbol": ["AAPL"],
        "name": ["Apple Inc"],
        "reportedDate": [date(2026, 6, 15)],
        "timeOfTheDay": ["post-market"],
    }, schema={
        "symbol": pl.Utf8, "name": pl.Utf8, "reportedDate": pl.Date,
        "timeOfTheDay": pl.Utf8,
    }).write_parquet(newer / "earnings_calendar.parquet")

    df = build_assets_overview(
        catalog_dir,
        today=date(2026, 4, 28),
        daily_dir=daily,
        historical_dir=historical_dir,
    )
    aapl = df.filter(pl.col("symbol") == "AAPL").row(0, named=True)
    assert aapl["reportedDate"] == date(2026, 6, 15)
    assert aapl["timeOfTheDay"] == "post-market"


def test_missing_stocks_catalog_means_no_sector(catalog_dir, historical_dir):
    (catalog_dir / "stocks.parquet").unlink()
    df = build_assets_overview(
        catalog_dir, today=date(2026, 4, 28), historical_dir=historical_dir,
    )
    assert (df["sector"] == "").all()


def test_empty_catalog_dir_returns_empty_frame(tmp_path):
    df = build_assets_overview(tmp_path / "empty", today=date(2026, 4, 28))
    assert df.height == 0
    assert dict(df.schema) == OVERVIEW_SCHEMA


# ── write_assets_overview ─────────────────────────────────────────────────────

def test_write_assets_overview_roundtrip(catalog_dir, historical_dir, tmp_path):
    dest = tmp_path / "transformed"
    out = write_assets_overview(
        catalog_dir, dest, today=date(2026, 4, 28),
        historical_dir=historical_dir,
    )
    assert out == dest / "assets_overview.parquet"
    reloaded = pl.read_parquet(out)
    assert dict(reloaded.schema) == OVERVIEW_SCHEMA
    assert reloaded.height == 8
