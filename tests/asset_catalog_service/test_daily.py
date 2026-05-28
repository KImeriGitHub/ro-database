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


def _seed_stocks(rows: list[dict]):
    """Write a stocks parquet with the 6-col schema (includes sector)."""
    df = pl.DataFrame(rows, schema={
        "symbol": pl.Utf8,
        "name": pl.Utf8,
        "sector": pl.Utf8,
        "ipoDate": pl.Date,
        "delistingDate": pl.Date,
        "status": pl.Utf8,
    })
    df.write_parquet(MOCK_DIR / "stocks.parquet", compression="zstd")


def _seed_etfs(rows: list[dict]):
    """Write an etfs parquet with the 5-col schema (no exchange, no sector)."""
    df = pl.DataFrame(rows, schema={
        "symbol": pl.Utf8,
        "name": pl.Utf8,
        "ipoDate": pl.Date,
        "delistingDate": pl.Date,
        "status": pl.Utf8,
    })
    df.write_parquet(MOCK_DIR / "etfs.parquet", compression="zstd")


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
    {"symbol": "AAPL", "name": "Apple Inc", "sector": "Technology",
     "ipoDate": date(1980, 12, 12), "delistingDate": None, "status": "Active"},
    {"symbol": "MSFT", "name": "Microsoft Corp", "sector": "Technology",
     "ipoDate": date(1986, 3, 13), "delistingDate": None, "status": "Active"},
]

ETFS_SEED = [
    {"symbol": "SPY", "name": "SPDR S&P 500 ETF",
     "ipoDate": date(1993, 1, 29), "delistingDate": None, "status": "Active"},
]

# Fresh data: MSFT vanished, GOOG added, AAPL ipoDate moved earlier
DAILY_ACTIVE_CSV = (
    "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
    "AAPL,Apple Inc,NASDAQ,Stock,1979-01-01,null,Active\n"
    "GOOG,Alphabet Inc,NASDAQ,Stock,2004-08-19,null,Active\n"
    "SPY,SPDR S&P 500 ETF,NYSE,ETF,1993-01-29,null,Active\n"
)
DAILY_DELISTED_CSV = (
    "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
)


@patch("asset_catalog_service.updates.stocks_etfs._fetch_sector")
@patch("asset_catalog_service.updates.stocks_etfs.fetch_text")
def test_daily_stocks_new_and_vanished(mock_fetch_text, mock_fetch_sector):
    _seed_stocks(STOCKS_SEED)
    _seed_etfs(ETFS_SEED)

    mock_fetch_text.side_effect = [DAILY_ACTIVE_CSV, DAILY_DELISTED_CSV]
    mock_fetch_sector.return_value = "Communication Services"

    update_stocks_etfs("fake-key", MOCK_DIR)

    stocks = pl.read_parquet(MOCK_DIR / "stocks.parquet")
    symbols = set(stocks["symbol"].to_list())

    # GOOG added with sector from OVERVIEW
    assert "GOOG" in symbols
    goog = stocks.filter(pl.col("symbol") == "GOOG")
    assert goog["sector"].to_list()[0] == "Communication Services"

    # MSFT vanished -> Corrupted
    msft = stocks.filter(pl.col("symbol") == "MSFT")
    assert msft["status"].to_list()[0] == "Corrupted"

    # AAPL ipoDate moved earlier -> date updated, status preserved
    aapl = stocks.filter(pl.col("symbol") == "AAPL")
    assert aapl["status"].to_list()[0] == "Active"
    assert aapl["ipoDate"].to_list()[0] == date(1979, 1, 1)


@patch("asset_catalog_service.updates.stocks_etfs.fetch_text")
def test_daily_stocks_delisting_date_change(mock_fetch_text):
    _seed_stocks(STOCKS_SEED)
    _seed_etfs(ETFS_SEED)

    # Fresh: AAPL now has a delistingDate
    csv = (
        "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
        "AAPL,Apple Inc,NASDAQ,Stock,1980-12-12,2026-04-01,Active\n"
        "MSFT,Microsoft Corp,NASDAQ,Stock,1986-03-13,null,Active\n"
        "SPY,SPDR S&P 500 ETF,NYSE,ETF,1993-01-29,null,Active\n"
    )
    mock_fetch_text.side_effect = [csv, DAILY_DELISTED_CSV]
    update_stocks_etfs("fake-key", MOCK_DIR)

    stocks = pl.read_parquet(MOCK_DIR / "stocks.parquet")
    aapl = stocks.filter(pl.col("symbol") == "AAPL")
    assert aapl["delistingDate"].to_list()[0] == date(2026, 4, 1)


@patch("asset_catalog_service.updates.stocks_etfs._fetch_sector")
@patch("asset_catalog_service.updates.stocks_etfs.fetch_text")
def test_daily_new_stock_gets_sector(mock_fetch_text, mock_fetch_sector):
    """New stock symbol during daily update gets its sector from OVERVIEW."""
    _seed_stocks(STOCKS_SEED)
    _seed_etfs(ETFS_SEED)

    csv_active = (
        "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
        "AAPL,Apple Inc,NASDAQ,Stock,1980-12-12,null,Active\n"
        "MSFT,Microsoft Corp,NASDAQ,Stock,1986-03-13,null,Active\n"
        "NVDA,NVIDIA Corp,NASDAQ,Stock,1999-01-22,null,Active\n"
        "SPY,SPDR S&P 500 ETF,NYSE,ETF,1993-01-29,null,Active\n"
    )
    mock_fetch_text.side_effect = [csv_active, DAILY_DELISTED_CSV]
    mock_fetch_sector.return_value = "Technology"

    update_stocks_etfs("fake-key", MOCK_DIR)

    stocks = pl.read_parquet(MOCK_DIR / "stocks.parquet")
    nvda = stocks.filter(pl.col("symbol") == "NVDA")
    assert nvda.height == 1
    assert nvda["sector"].to_list()[0] == "Technology"

    # Verify OVERVIEW was called for NVDA
    mock_fetch_sector.assert_called_once_with("fake-key", "NVDA")


@patch("asset_catalog_service.updates.stocks_etfs.fetch_text")
def test_daily_stocks_ipo_null_to_value(mock_fetch_text):
    """ipoDate going from null to a value should update it, not mark Corrupted."""
    _seed_stocks([
        {"symbol": "AAPL", "name": "Apple Inc", "sector": "Technology",
         "ipoDate": None, "delistingDate": None, "status": "Active"},
    ])
    _seed_etfs(ETFS_SEED)

    csv = (
        "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
        "AAPL,Apple Inc,NASDAQ,Stock,1980-12-12,null,Active\n"
        "SPY,SPDR S&P 500 ETF,NYSE,ETF,1993-01-29,null,Active\n"
    )
    mock_fetch_text.side_effect = [csv, DAILY_DELISTED_CSV]
    update_stocks_etfs("fake-key", MOCK_DIR)

    stocks = pl.read_parquet(MOCK_DIR / "stocks.parquet")
    aapl = stocks.filter(pl.col("symbol") == "AAPL")
    assert aapl["ipoDate"].to_list()[0] == date(1980, 12, 12)
    assert aapl["status"].to_list()[0] == "Active"  # not Corrupted


@patch("asset_catalog_service.updates.stocks_etfs.fetch_text")
def test_daily_stocks_ipo_later_is_ignored(mock_fetch_text):
    """Fresh ipoDate later than existing must not flip status or rewrite the date.

    Without this guard, AV occasionally reporting a later ipoDate (e.g.
    delisted-row info disappearing) would re-mark the ticker Corrupted on
    every run.
    """
    _seed_stocks([
        {"symbol": "OSG", "name": "OSG Corp", "sector": "Industrials",
         "ipoDate": date(2013, 5, 1), "delistingDate": None, "status": "Active"},
    ])
    _seed_etfs(ETFS_SEED)

    csv = (
        "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
        "OSG,OSG Corp,NYSE,Stock,2015-12-01,null,Active\n"
        "SPY,SPDR S&P 500 ETF,NYSE,ETF,1993-01-29,null,Active\n"
    )
    mock_fetch_text.side_effect = [csv, DAILY_DELISTED_CSV]
    update_stocks_etfs("fake-key", MOCK_DIR)

    stocks = pl.read_parquet(MOCK_DIR / "stocks.parquet")
    osg = stocks.filter(pl.col("symbol") == "OSG")
    assert osg["ipoDate"].to_list()[0] == date(2013, 5, 1)
    assert osg["status"].to_list()[0] == "Active"


@patch("asset_catalog_service.updates.stocks_etfs.fetch_text")
def test_daily_reissued_ticker_keeps_min_ipo(mock_fetch_text):
    """When the same symbol appears in both active and delisted lists, the
    catalog row should carry the earliest ipoDate across the two."""
    _seed_stocks([
        {"symbol": "GRML", "name": "Gorilla Inc", "sector": "Technology",
         "ipoDate": date(2026, 3, 11), "delistingDate": None, "status": "Active"},
    ])
    _seed_etfs(ETFS_SEED)

    active = (
        "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
        "GRML,Gorilla Inc,NASDAQ,Stock,2026-03-11,null,Active\n"
        "SPY,SPDR S&P 500 ETF,NYSE,ETF,1993-01-29,null,Active\n"
    )
    delisted = (
        "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
        "GRML,Gorilla Old Inc,NASDAQ,Stock,2022-04-29,2024-08-15,Delisted\n"
    )
    mock_fetch_text.side_effect = [active, delisted]
    update_stocks_etfs("fake-key", MOCK_DIR)

    stocks = pl.read_parquet(MOCK_DIR / "stocks.parquet")
    grml = stocks.filter(pl.col("symbol") == "GRML")
    assert grml.height == 1
    assert grml["ipoDate"].to_list()[0] == date(2022, 4, 29)
    assert grml["status"].to_list()[0] == "Active"

    # Second run with identical input must be a no-op (no re-detect).
    mock_fetch_text.side_effect = [active, delisted]
    update_stocks_etfs("fake-key", MOCK_DIR)
    stocks2 = pl.read_parquet(MOCK_DIR / "stocks.parquet")
    grml2 = stocks2.filter(pl.col("symbol") == "GRML")
    assert grml2["ipoDate"].to_list()[0] == date(2022, 4, 29)


@patch("asset_catalog_service.updates.stocks_etfs.fetch_text")
def test_daily_update_missing_parquets(mock_fetch_text):
    """update_stocks_etfs raises if parquets don't exist."""
    with pytest.raises(FileNotFoundError, match="Run init_catalog.py first"):
        update_stocks_etfs("fake-key", MOCK_DIR)


@patch("asset_catalog_service.updates.stocks_etfs.fetch_text")
def test_daily_stocks_relisted_resets_to_active(mock_fetch_text):
    """A Corrupted/Delisted symbol that AV reports back in active LISTING_STATUS
    gets its status reset to Active and its stale delistingDate cleared."""
    old_date = date.today() - timedelta(days=45)
    _seed_stocks([
        {"symbol": "AAPL", "name": "Apple Inc", "sector": "Technology",
         "ipoDate": date(1980, 12, 12), "delistingDate": None, "status": "Active"},
        # Vanished previously, marked Delisted after the 30-day grace.
        {"symbol": "REVIVE", "name": "Revive Corp", "sector": "Technology",
         "ipoDate": date(2010, 1, 1), "delistingDate": old_date, "status": "Delisted"},
        # Vanished recently, marked Corrupted (delistingDate set by block 2a).
        {"symbol": "CORRUPT", "name": "Corrupt Inc", "sector": "Technology",
         "ipoDate": date(2015, 1, 1), "delistingDate": date.today() - timedelta(days=2),
         "status": "Corrupted"},
    ])
    _seed_etfs(ETFS_SEED)

    active_csv = (
        "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
        "AAPL,Apple Inc,NASDAQ,Stock,1980-12-12,null,Active\n"
        "REVIVE,Revive Corp,NASDAQ,Stock,2010-01-01,null,Active\n"
        "CORRUPT,Corrupt Inc,NASDAQ,Stock,2015-01-01,null,Active\n"
        "SPY,SPDR S&P 500 ETF,NYSE,ETF,1993-01-29,null,Active\n"
    )
    mock_fetch_text.side_effect = [active_csv, DAILY_DELISTED_CSV]
    update_stocks_etfs("fake-key", MOCK_DIR)

    stocks = pl.read_parquet(MOCK_DIR / "stocks.parquet")
    revive = stocks.filter(pl.col("symbol") == "REVIVE")
    assert revive["status"].to_list()[0] == "Active"
    assert revive["delistingDate"].to_list()[0] is None
    corrupt = stocks.filter(pl.col("symbol") == "CORRUPT")
    assert corrupt["status"].to_list()[0] == "Active"
    assert corrupt["delistingDate"].to_list()[0] is None


@patch("asset_catalog_service.updates.stocks_etfs.fetch_text")
def test_daily_stocks_active_to_delisted(mock_fetch_text):
    """An Active symbol that AV moves into delisted LISTING_STATUS is promoted
    to Delisted on the same run, without waiting for the 30-day Corrupted timer."""
    _seed_stocks([
        {"symbol": "AAPL", "name": "Apple Inc", "sector": "Technology",
         "ipoDate": date(1980, 12, 12), "delistingDate": None, "status": "Active"},
        {"symbol": "GOODBYE", "name": "Goodbye Corp", "sector": "Technology",
         "ipoDate": date(2010, 1, 1), "delistingDate": None, "status": "Active"},
    ])
    _seed_etfs(ETFS_SEED)

    # GOODBYE is now in the delisted CSV with an explicit delistingDate.
    active_csv = (
        "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
        "AAPL,Apple Inc,NASDAQ,Stock,1980-12-12,null,Active\n"
        "SPY,SPDR S&P 500 ETF,NYSE,ETF,1993-01-29,null,Active\n"
    )
    delisted_csv = (
        "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
        "GOODBYE,Goodbye Corp,NASDAQ,Stock,2010-01-01,2026-05-10,Delisted\n"
    )
    mock_fetch_text.side_effect = [active_csv, delisted_csv]
    update_stocks_etfs("fake-key", MOCK_DIR)

    stocks = pl.read_parquet(MOCK_DIR / "stocks.parquet")
    bye = stocks.filter(pl.col("symbol") == "GOODBYE")
    assert bye["status"].to_list()[0] == "Delisted"
    assert bye["delistingDate"].to_list()[0] == date(2026, 5, 10)


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


@patch("asset_catalog_service.updates.indices.fetch_json")
def test_daily_indices_relisted_resets_to_active(mock_fetch):
    """A Corrupted/Delisted index that re-appears in the source list is reset
    to Active and its delistingDate is cleared."""
    old_date = date.today() - timedelta(days=45)
    recent = date.today() - timedelta(days=2)
    df = pl.DataFrame({
        "symbol": ["SPX", "REVIVE", "CORRUPT"],
        "name": ["S&P 500", "Revive Index", "Corrupt Index"],
        "ipoDate": [None, None, None],
        "delistingDate": [None, old_date, recent],
        "status": [None, "Delisted", "Corrupted"],
    }).cast({"ipoDate": pl.Date, "delistingDate": pl.Date, "status": pl.Utf8})
    df.write_parquet(MOCK_DIR / "indices.parquet", compression="zstd")

    mock_fetch.return_value = {
        "SPX": "S&P 500",
        "REVIVE": "Revive Index",
        "CORRUPT": "Corrupt Index",
    }
    update_indices("fake-key", MOCK_DIR)

    result = pl.read_parquet(MOCK_DIR / "indices.parquet")
    revive = result.filter(pl.col("symbol") == "REVIVE")
    assert revive["status"].to_list()[0] == "Active"
    assert revive["delistingDate"].to_list()[0] is None
    corrupt = result.filter(pl.col("symbol") == "CORRUPT")
    assert corrupt["status"].to_list()[0] == "Active"
    assert corrupt["delistingDate"].to_list()[0] is None


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


@patch("asset_catalog_service.updates.stocks_etfs.fetch_json")
@patch("asset_catalog_service.updates.stocks_etfs.fetch_text")
def test_daily_yield_status_noop(mock_fetch_text, mock_fetch_json):
    seed_csv_active = (
        "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
        "AAPL,Apple Inc,NASDAQ,Stock,1980-12-12,null,Active\n"
    )
    seed_csv_delisted = (
        "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
    )
    mock_fetch_text.side_effect = [seed_csv_active, seed_csv_delisted]
    mock_fetch_json.return_value = {"Sector": "TECHNOLOGY"}

    from asset_catalog_service.updates import init_stocks_etfs
    init_stocks_etfs("fake-key", MOCK_DIR)
    update_yield_status(MOCK_DIR)
    df1 = pl.read_parquet(MOCK_DIR / "yield_status.parquet")

    update_yield_status(MOCK_DIR)  # second run - should be no-op
    df2 = pl.read_parquet(MOCK_DIR / "yield_status.parquet")

    assert df1.equals(df2)


