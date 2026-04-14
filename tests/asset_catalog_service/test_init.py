"""Tests for initial catalog creation (no parquet files exist yet)."""

import shutil
import sys
from datetime import date
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from asset_catalog_service.updates import (
    update_stocks_etfs,
    update_indices,
    update_forex,
    update_cryptocurrencies,
    update_commodities,
    update_economic,
    update_yield_status,
    update_earnings_calendar,
)

MOCK_DIR = Path(__file__).parent / "mock_catalog"

# ── Fixtures ──────────────────────────────────────────────────────────

LISTING_ACTIVE_CSV = (
    "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
    "AAPL,Apple Inc,NASDAQ,Stock,1980-12-12,null,Active\n"
    "MSFT,Microsoft Corp,NASDAQ,Stock,1986-03-13,null,Active\n"
    "SPY,SPDR S&P 500 ETF,NYSE,ETF,1993-01-29,null,Active\n"
)

LISTING_DELISTED_CSV = (
    "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
    "OLD,Old Corp,NYSE,Stock,2000-01-01,2020-06-15,Delisted\n"
)

INDEX_CATALOG_JSON = {"SPX": "S&P 500", "DJI": "Dow Jones Industrial Average"}

FOREX_CSV = (
    "currency code,currency name\n"
    "EUR,Euro\n"
    "JPY,Japanese Yen\n"
    "GBP,British Pound\n"
)

CRYPTO_CSV = (
    "from_currency,to_currency\n"
    "BTC,USD\n"
    "ETH,USD\n"
    "BTC,EUR\n"
)

EARNINGS_CSV = (
    "symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\n"
    "AAPL,Apple Inc,2026-04-25,2026-03-31,1.62,USD,AMC\n"
    "MSFT,Microsoft,2026-04-22,2026-03-31,3.22,USD,AMC\n"
    "BAD,Bad Corp,not-a-date,2026-03-31,xyz,USD,BMS\n"
)


@pytest.fixture(autouse=True)
def clean_mock_dir():
    """Wipe mock_catalog before and after each test."""
    if MOCK_DIR.exists():
        shutil.rmtree(MOCK_DIR)
    MOCK_DIR.mkdir(parents=True, exist_ok=True)
    yield
    shutil.rmtree(MOCK_DIR)
    MOCK_DIR.mkdir(parents=True, exist_ok=True)


# ── Tests ─────────────────────────────────────────────────────────────


@patch("asset_catalog_service.updates.stocks_etfs.fetch_text")
def test_init_stocks_etfs(mock_fetch):
    mock_fetch.side_effect = [LISTING_ACTIVE_CSV, LISTING_DELISTED_CSV]

    update_stocks_etfs("fake-key", MOCK_DIR)

    stocks = pl.read_parquet(MOCK_DIR / "stocks.parquet")
    etfs = pl.read_parquet(MOCK_DIR / "etfs.parquet")

    assert stocks.height == 3  # AAPL, MSFT, OLD
    assert etfs.height == 1  # SPY
    assert set(stocks["symbol"].to_list()) == {"AAPL", "MSFT", "OLD"}
    assert set(etfs["symbol"].to_list()) == {"SPY"}
    assert set(stocks.columns) == {
        "symbol", "name", "exchange", "assetType",
        "ipoDate", "delistingDate", "status",
    }


@patch("asset_catalog_service.updates.indices.fetch_json")
def test_init_indices(mock_fetch):
    mock_fetch.return_value = INDEX_CATALOG_JSON

    update_indices("fake-key", MOCK_DIR)

    df = pl.read_parquet(MOCK_DIR / "indices.parquet")
    assert df.height == 2
    assert set(df["symbol"].to_list()) == {"SPX", "DJI"}
    assert df["ipoDate"].null_count() == 2
    assert df["delistingDate"].null_count() == 2
    assert df["status"].null_count() == 2


@patch("asset_catalog_service.updates.forex.fetch_text")
def test_init_forex(mock_fetch):
    mock_fetch.return_value = FOREX_CSV

    update_forex(MOCK_DIR)

    df = pl.read_parquet(MOCK_DIR / "forex.parquet")
    assert df.height == 3
    assert set(df["symbol"].to_list()) == {"EURUSD", "JPYUSD", "GBPUSD"}


@patch("asset_catalog_service.updates.cryptocurrencies.fetch_text")
def test_init_cryptocurrencies(mock_fetch):
    mock_fetch.return_value = CRYPTO_CSV

    update_cryptocurrencies(MOCK_DIR)

    df = pl.read_parquet(MOCK_DIR / "cryptocurrencies.parquet")
    # Only USD pairs kept (BTC/USD, ETH/USD), BTC/EUR filtered out
    assert df.height == 2
    assert set(df["symbol"].to_list()) == {"BTC", "ETH"}
    assert "Cryptocurrency BTC for Market USD" in df["name"].to_list()


def test_init_commodities():
    update_commodities(MOCK_DIR)

    df = pl.read_parquet(MOCK_DIR / "commodities.parquet")
    assert df.height == 13
    assert "XAU" in df["symbol"].to_list()
    assert all(s == "Active" for s in df["status"].to_list())


def test_init_economic():
    update_economic(MOCK_DIR)

    df = pl.read_parquet(MOCK_DIR / "economic.parquet")
    assert df.height == 10
    assert "REAL_GDP" in df["symbol"].to_list()
    assert all(s == "Active" for s in df["status"].to_list())


@patch("asset_catalog_service.updates.stocks_etfs.fetch_text")
def test_init_yield_status(mock_fetch):
    # yield_status needs stocks.parquet first
    mock_fetch.side_effect = [LISTING_ACTIVE_CSV, LISTING_DELISTED_CSV]
    update_stocks_etfs("fake-key", MOCK_DIR)

    update_yield_status(MOCK_DIR)

    df = pl.read_parquet(MOCK_DIR / "yield_status.parquet")
    assert df.height == 3  # 3 stocks (AAPL, MSFT, OLD)
    assert "prices" in df.columns
    assert "prices_daily" in df.columns
    assert "sentiment" in df.columns
    assert df["prices"].null_count() == 3
    assert df["prices_daily"].null_count() == 3
    assert df["date"].to_list() == [date.today()] * 3


def test_yield_status_skips_without_stocks():
    # No stocks.parquet -> should return without error
    update_yield_status(MOCK_DIR)
    assert not (MOCK_DIR / "yield_status.parquet").exists()


@patch("asset_catalog_service.updates.earnings_calendar.fetch_text")
def test_init_earnings_calendar(mock_fetch):
    mock_fetch.return_value = EARNINGS_CSV

    update_earnings_calendar("fake-key", MOCK_DIR)

    df = pl.read_parquet(MOCK_DIR / "earnings_calendar.parquet")
    assert df.height == 3
    assert set(df.columns) == {
        "symbol", "name", "reportDate", "fiscalDateEnding",
        "estimate", "currency", "timeOfTheDay", "cast_issues",
    }
    # BAD row should have cast issues
    bad_row = df.filter(pl.col("symbol") == "BAD")
    assert bad_row["cast_issues"].to_list()[0] is not None
    assert "reportDate" in bad_row["cast_issues"].to_list()[0]
    assert "estimate" in bad_row["cast_issues"].to_list()[0]

    # Good rows should have no cast issues
    good = df.filter(pl.col("symbol") == "AAPL")
    assert good["cast_issues"].to_list()[0] is None
    assert good["estimate"].to_list()[0] == pytest.approx(1.62)
