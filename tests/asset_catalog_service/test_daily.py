"""Tests for daily catalog updates (parquet files already exist)."""

import shutil
import sys
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

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

# ── Helpers ───────────────────────────────────────────────────────────


def _seed_simple_catalog(filename: str, symbols: list[str], names: list[str]):
    """Write a minimal catalog parquet with the standard 5-col schema."""
    df = pl.DataFrame({
        "symbol": symbols,
        "name": names,
        "ipoDate": [None] * len(symbols),
        "delistingDate": [None] * len(symbols),
        "status": [None] * len(symbols),
    }).cast({
        "ipoDate": pl.Date,
        "delistingDate": pl.Date,
        "status": pl.Utf8,
    })
    df.write_parquet(MOCK_DIR / filename, compression="zstd")


def _seed_listing(filename: str, rows: list[dict]):
    """Write a stocks/etfs parquet with the 6-col schema."""
    df = pl.DataFrame(rows, schema={
        "symbol": pl.Utf8,
        "name": pl.Utf8,
        "exchange": pl.Utf8,
        "ipoDate": pl.Date,
        "delistingDate": pl.Date,
        "status": pl.Utf8,
    })
    df.write_parquet(MOCK_DIR / filename, compression="zstd")


@pytest.fixture(autouse=True)
def clean_mock_dir():
    if MOCK_DIR.exists():
        shutil.rmtree(MOCK_DIR)
    MOCK_DIR.mkdir(parents=True, exist_ok=True)
    yield
    shutil.rmtree(MOCK_DIR)
    MOCK_DIR.mkdir(parents=True, exist_ok=True)


# ── stocks & ETFs daily ──────────────────────────────────────────────

STOCKS_SEED = [
    {"symbol": "AAPL", "name": "Apple Inc", "exchange": "NASDAQ",
     "ipoDate": date(1980, 12, 12), "delistingDate": None, "status": "Active"},
    {"symbol": "MSFT", "name": "Microsoft Corp", "exchange": "NASDAQ",
     "ipoDate": date(1986, 3, 13), "delistingDate": None, "status": "Active"},
]

ETFS_SEED = [
    {"symbol": "SPY", "name": "SPDR S&P 500 ETF", "exchange": "NYSE",
     "ipoDate": date(1993, 1, 29), "delistingDate": None, "status": "Active"},
]

# Fresh data: MSFT vanished, GOOG added, AAPL ipoDate changed
DAILY_ACTIVE_CSV = (
    "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
    "AAPL,Apple Inc,NASDAQ,Stock,1999-01-01,null,Active\n"
    "GOOG,Alphabet Inc,NASDAQ,Stock,2004-08-19,null,Active\n"
    "SPY,SPDR S&P 500 ETF,NYSE,ETF,1993-01-29,null,Active\n"
)
DAILY_DELISTED_CSV = (
    "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
)


@patch("asset_catalog_service.updates.stocks_etfs.fetch_text")
def test_daily_stocks_new_and_vanished(mock_fetch):
    _seed_listing("stocks.parquet", STOCKS_SEED)
    _seed_listing("etfs.parquet", ETFS_SEED)

    mock_fetch.side_effect = [DAILY_ACTIVE_CSV, DAILY_DELISTED_CSV]
    update_stocks_etfs("fake-key", MOCK_DIR)

    stocks = pl.read_parquet(MOCK_DIR / "stocks.parquet")
    symbols = set(stocks["symbol"].to_list())

    # GOOG added
    assert "GOOG" in symbols
    # MSFT vanished -> Corrupted
    msft = stocks.filter(pl.col("symbol") == "MSFT")
    assert msft["status"].to_list()[0] == "Corrupted"
    # AAPL ipoDate changed -> Corrupted
    aapl = stocks.filter(pl.col("symbol") == "AAPL")
    assert aapl["status"].to_list()[0] == "Corrupted"


@patch("asset_catalog_service.updates.stocks_etfs.fetch_text")
def test_daily_stocks_delisting_date_change(mock_fetch):
    _seed_listing("stocks.parquet", STOCKS_SEED)
    _seed_listing("etfs.parquet", ETFS_SEED)

    # Fresh: AAPL now has a delistingDate
    csv = (
        "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
        "AAPL,Apple Inc,NASDAQ,Stock,1980-12-12,2026-04-01,Active\n"
        "MSFT,Microsoft Corp,NASDAQ,Stock,1986-03-13,null,Active\n"
        "SPY,SPDR S&P 500 ETF,NYSE,ETF,1993-01-29,null,Active\n"
    )
    mock_fetch.side_effect = [csv, DAILY_DELISTED_CSV]
    update_stocks_etfs("fake-key", MOCK_DIR)

    stocks = pl.read_parquet(MOCK_DIR / "stocks.parquet")
    aapl = stocks.filter(pl.col("symbol") == "AAPL")
    assert aapl["delistingDate"].to_list()[0] == date(2026, 4, 1)


# ── indices daily ─────────────────────────────────────────────────────


@patch("asset_catalog_service.updates.indices.fetch_json")
def test_daily_indices_new_symbol(mock_fetch):
    _seed_simple_catalog("indices.parquet", ["SPX", "DJI"],
                         ["S&P 500", "Dow Jones Industrial Average"])

    # Fresh: added IXIC, removed DJI
    mock_fetch.return_value = {"SPX": "S&P 500", "IXIC": "NASDAQ Composite"}
    update_indices("fake-key", MOCK_DIR)

    df = pl.read_parquet(MOCK_DIR / "indices.parquet")
    symbols = set(df["symbol"].to_list())
    assert "IXIC" in symbols
    # DJI missing -> Corrupted with delistingDate=today
    dji = df.filter(pl.col("symbol") == "DJI")
    assert dji["status"].to_list()[0] == "Corrupted"
    assert dji["delistingDate"].to_list()[0] == date.today()


@patch("asset_catalog_service.updates.indices.fetch_json")
def test_daily_indices_delisted_after_30_days(mock_fetch):
    """Symbol missing for >30 days gets promoted from Corrupted to Delisted."""
    old_date = date.today() - timedelta(days=45)
    df = pl.DataFrame({
        "symbol": ["SPX", "GONE"],
        "name": ["S&P 500", "Gone Index"],
        "ipoDate": [None, None],
        "delistingDate": [None, old_date],
        "status": [None, "Corrupted"],
    }).cast({"ipoDate": pl.Date, "delistingDate": pl.Date, "status": pl.Utf8})
    df.write_parquet(MOCK_DIR / "indices.parquet", compression="zstd")

    mock_fetch.return_value = {"SPX": "S&P 500"}
    update_indices("fake-key", MOCK_DIR)

    result = pl.read_parquet(MOCK_DIR / "indices.parquet")
    gone = result.filter(pl.col("symbol") == "GONE")
    assert gone["status"].to_list()[0] == "Delisted"


# ── forex daily ───────────────────────────────────────────────────────


@patch("asset_catalog_service.updates.forex.fetch_text")
def test_daily_forex_new_and_missing(mock_fetch):
    _seed_simple_catalog("forex.parquet", ["EURUSD", "JPYUSD"],
                         ["Euro", "Japanese Yen"])

    # Fresh: EUR still there, JPY gone, GBP new
    csv = "currency code,currency name\nEUR,Euro\nGBP,British Pound\n"
    mock_fetch.return_value = csv
    update_forex(MOCK_DIR)

    df = pl.read_parquet(MOCK_DIR / "forex.parquet")
    symbols = set(df["symbol"].to_list())
    assert "GBPUSD" in symbols
    jpy = df.filter(pl.col("symbol") == "JPYUSD")
    assert jpy["status"].to_list()[0] == "Corrupted"


# ── crypto daily ──────────────────────────────────────────────────────


@patch("asset_catalog_service.updates.cryptocurrencies.fetch_text")
def test_daily_crypto_new_and_missing(mock_fetch):
    _seed_simple_catalog("cryptocurrencies.parquet", ["BTC", "ETH"],
                         ["Cryptocurrency BTC for Market USD",
                          "Cryptocurrency ETH for Market USD"])

    # Fresh: BTC still there, ETH gone, SOL new
    csv = "from_currency,to_currency\nBTC,USD\nSOL,USD\n"
    mock_fetch.return_value = csv
    update_cryptocurrencies(MOCK_DIR)

    df = pl.read_parquet(MOCK_DIR / "cryptocurrencies.parquet")
    symbols = set(df["symbol"].to_list())
    assert "SOL" in symbols
    eth = df.filter(pl.col("symbol") == "ETH")
    assert eth["status"].to_list()[0] == "Corrupted"


# ── static catalogs are no-ops on second run ──────────────────────────


def test_daily_commodities_noop():
    update_commodities(MOCK_DIR)
    df1 = pl.read_parquet(MOCK_DIR / "commodities.parquet")

    update_commodities(MOCK_DIR)  # second run
    df2 = pl.read_parquet(MOCK_DIR / "commodities.parquet")

    assert df1.equals(df2)


def test_daily_economic_noop():
    update_economic(MOCK_DIR)
    df1 = pl.read_parquet(MOCK_DIR / "economic.parquet")

    update_economic(MOCK_DIR)
    df2 = pl.read_parquet(MOCK_DIR / "economic.parquet")

    assert df1.equals(df2)


# ── yield_status is no-op on second run ───────────────────────────────


@patch("asset_catalog_service.updates.stocks_etfs.fetch_text")
def test_daily_yield_status_noop(mock_fetch):
    seed_csv_active = (
        "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
        "AAPL,Apple Inc,NASDAQ,Stock,1980-12-12,null,Active\n"
    )
    seed_csv_delisted = (
        "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
    )
    mock_fetch.side_effect = [seed_csv_active, seed_csv_delisted]
    update_stocks_etfs("fake-key", MOCK_DIR)
    update_yield_status(MOCK_DIR)
    df1 = pl.read_parquet(MOCK_DIR / "yield_status.parquet")

    update_yield_status(MOCK_DIR)  # second run - should be no-op
    df2 = pl.read_parquet(MOCK_DIR / "yield_status.parquet")

    assert df1.equals(df2)


# ── earnings_calendar always overwrites ───────────────────────────────


@patch("asset_catalog_service.updates.earnings_calendar.fetch_text")
def test_daily_earnings_calendar_overwrites(mock_fetch):
    csv1 = (
        "symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\n"
        "AAPL,Apple Inc,2026-04-25,2026-03-31,1.62,USD,AMC\n"
    )
    csv2 = (
        "symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\n"
        "MSFT,Microsoft,2026-04-22,2026-03-31,3.22,USD,AMC\n"
    )

    mock_fetch.return_value = csv1
    update_earnings_calendar("fake-key", MOCK_DIR)
    df1 = pl.read_parquet(MOCK_DIR / "earnings_calendar.parquet")
    assert df1.height == 1
    assert df1["symbol"].to_list() == ["AAPL"]

    mock_fetch.return_value = csv2
    update_earnings_calendar("fake-key", MOCK_DIR)
    df2 = pl.read_parquet(MOCK_DIR / "earnings_calendar.parquet")
    assert df2.height == 1
    assert df2["symbol"].to_list() == ["MSFT"]  # fully replaced
