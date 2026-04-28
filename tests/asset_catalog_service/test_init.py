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
    init_stocks_etfs,
    validate_firstrate_csvs,
    update_indices,
    update_forex,
    update_cryptocurrencies,
    update_commodities,
    update_economic,
    update_yield_status,
    update_earnings_calendar,
)
from asset_catalog_service.updates._common import normalize_sector

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
    "symbol,name,reportedDate,fiscalDateEnding,estimate,currency,timeOfTheDay\n"
    "AAPL,Apple Inc,2026-04-25,2026-03-31,1.62,USD,AMC\n"
    "MSFT,Microsoft,2026-04-22,2026-03-31,3.22,USD,AMC\n"
    "BAD,Bad Corp,not-a-date,2026-03-31,xyz,USD,BMS\n"
)

# OVERVIEW responses keyed by symbol
OVERVIEW_RESPONSES = {
    "AAPL": {"Sector": "TECHNOLOGY"},
    "MSFT": {"Sector": "TECHNOLOGY"},
    "OLD": {"Sector": "INDUSTRIALS"},
}

# FirstRate CSV content
FIRSTRATE_STOCKS_CSV = (
    "Ticker,Company Name,Sector,IPO Date,Delisting Date,Status\n"
    "AAPL,Apple Inc,Technology,1980-12-12,,Active\n"
    "MSFT,Microsoft Corp,Technology,1986-03-13,,Active\n"
    "DEAD,Dead Corp,Energy,1995-01-01,2018-05-01,Delisted\n"
)

FIRSTRATE_ETFS_CSV = (
    "Ticker,Name,IPO Date,Delisting Date,Status\n"
    "SPY,SPDR S&P 500 ETF,1993-01-29,,Active\n"
    "GONE,Gone ETF,2005-03-01,2019-12-31,Delisted\n"
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


# ── Sector normalization ─────────────────────────────────────────────


def test_normalize_sector_canonical():
    assert normalize_sector("Technology") == "Technology"
    assert normalize_sector("TECHNOLOGY") == "Technology"


def test_normalize_sector_aliases():
    assert normalize_sector("CONSUMER STAPLES") == "Consumer Defensive"
    assert normalize_sector("FINANCIALS") == "Financial Services"


def test_normalize_sector_none_empty_unknown():
    assert normalize_sector(None) == "Other"
    assert normalize_sector("") == "Other"
    assert normalize_sector("  ") == "Other"
    assert normalize_sector("NONE") == "Other"
    assert normalize_sector("OTHER") == "Other"
    assert normalize_sector("Basket Weaving") == "Other"


# ── FirstRate CSV validation ─────────────────────────────────────────


def test_validate_firstrate_no_dirs():
    """No dirs provided -> no error."""
    validate_firstrate_csvs(None, None)


def test_validate_firstrate_missing_csv():
    """Dir exists but CSV not found -> ValueError."""
    with pytest.raises(ValueError, match="catalog_stocks.csv not found"):
        validate_firstrate_csvs(MOCK_DIR, None)


def test_validate_firstrate_missing_headers():
    """CSV exists but missing required columns -> ValueError."""
    csv_path = MOCK_DIR / "catalog_stocks.csv"
    csv_path.write_text("Ticker,Company Name\nAAPL,Apple\n")
    with pytest.raises(ValueError, match="missing required headers"):
        validate_firstrate_csvs(MOCK_DIR, None)


def test_validate_firstrate_both_fail():
    """Both dirs invalid -> ValueError mentions both."""
    etf_dir = MOCK_DIR / "etfs"
    etf_dir.mkdir()
    with pytest.raises(ValueError) as exc_info:
        validate_firstrate_csvs(MOCK_DIR, etf_dir)
    msg = str(exc_info.value)
    assert "catalog_stocks.csv" in msg
    assert "catalog_etfs.csv" in msg


def test_validate_firstrate_valid():
    """Valid CSVs pass validation."""
    stocks_dir = MOCK_DIR / "stocks"
    stocks_dir.mkdir()
    (stocks_dir / "catalog_stocks.csv").write_text(FIRSTRATE_STOCKS_CSV)

    etfs_dir = MOCK_DIR / "etfs"
    etfs_dir.mkdir()
    (etfs_dir / "catalog_etfs.csv").write_text(FIRSTRATE_ETFS_CSV)

    validate_firstrate_csvs(stocks_dir, etfs_dir)  # should not raise


# ── Init stocks & ETFs (AV only) ────────────────────────────────────


@patch("asset_catalog_service.updates.stocks_etfs.fetch_json")
@patch("asset_catalog_service.updates.stocks_etfs.fetch_text")
def test_init_stocks_etfs_av_only(mock_fetch_text, mock_fetch_json):
    mock_fetch_text.side_effect = [LISTING_ACTIVE_CSV, LISTING_DELISTED_CSV]
    mock_fetch_json.side_effect = lambda url: OVERVIEW_RESPONSES.get(
        url.split("symbol=")[1].split("&")[0], {"Sector": None}
    )

    init_stocks_etfs("fake-key", MOCK_DIR)

    stocks = pl.read_parquet(MOCK_DIR / "stocks.parquet")
    etfs = pl.read_parquet(MOCK_DIR / "etfs.parquet")

    assert stocks.height == 3  # AAPL, MSFT, OLD
    assert etfs.height == 1  # SPY
    assert set(stocks["symbol"].to_list()) == {"AAPL", "MSFT", "OLD"}
    assert set(etfs["symbol"].to_list()) == {"SPY"}

    # Schema: stocks have sector, no exchange
    assert set(stocks.columns) == {
        "symbol", "name", "sector",
        "ipoDate", "delistingDate", "status",
    }
    # Schema: etfs have no exchange, no sector
    assert set(etfs.columns) == {
        "symbol", "name",
        "ipoDate", "delistingDate", "status",
    }

    # Sectors populated via OVERVIEW
    aapl = stocks.filter(pl.col("symbol") == "AAPL")
    assert aapl["sector"].to_list()[0] == "Technology"

    old = stocks.filter(pl.col("symbol") == "OLD")
    assert old["sector"].to_list()[0] == "Industrials"


# ── Init stocks & ETFs with FirstRate ────────────────────────────────


@patch("asset_catalog_service.updates.stocks_etfs.fetch_json")
@patch("asset_catalog_service.updates.stocks_etfs.fetch_text")
def test_init_stocks_with_firstrate(mock_fetch_text, mock_fetch_json):
    # Set up FirstRate dirs
    stocks_dir = MOCK_DIR / "fr_stocks"
    stocks_dir.mkdir()
    (stocks_dir / "catalog_stocks.csv").write_text(FIRSTRATE_STOCKS_CSV)

    etfs_dir = MOCK_DIR / "fr_etfs"
    etfs_dir.mkdir()
    (etfs_dir / "catalog_etfs.csv").write_text(FIRSTRATE_ETFS_CSV)

    mock_fetch_text.side_effect = [LISTING_ACTIVE_CSV, LISTING_DELISTED_CSV]
    # OVERVIEW only called for OLD (in AV but not in FirstRate, needs sector)
    mock_fetch_json.return_value = {"Sector": "INDUSTRIALS"}

    init_stocks_etfs("fake-key", MOCK_DIR, stocks_dir, etfs_dir)

    stocks = pl.read_parquet(MOCK_DIR / "stocks.parquet")
    etfs = pl.read_parquet(MOCK_DIR / "etfs.parquet")

    # DEAD from FirstRate + AAPL, MSFT, OLD
    assert "DEAD" in stocks["symbol"].to_list()
    assert stocks.height == 4

    # GONE from FirstRate + SPY
    assert "GONE" in etfs["symbol"].to_list()
    assert etfs.height == 2

    # FirstRate sectors preserved
    aapl = stocks.filter(pl.col("symbol") == "AAPL")
    assert aapl["sector"].to_list()[0] == "Technology"

    dead = stocks.filter(pl.col("symbol") == "DEAD")
    assert dead["sector"].to_list()[0] == "Energy"

    # OLD only in AV -> sector from OVERVIEW
    old = stocks.filter(pl.col("symbol") == "OLD")
    assert old["sector"].to_list()[0] == "Industrials"


@patch("asset_catalog_service.updates.stocks_etfs.fetch_json")
@patch("asset_catalog_service.updates.stocks_etfs.fetch_text")
def test_init_firstrate_sector_normalization(mock_fetch_text, mock_fetch_json):
    """Verify sector mapping: CSV value and AV OVERVIEW value both normalize."""
    csv_content = (
        "Ticker,Company Name,Sector,IPO Date,Status\n"
        "A,Corp A,Consumer Defensive,2000-01-01,Active\n"
        "B,Corp B,,2000-01-01,Active\n"
    )
    stocks_dir = MOCK_DIR / "fr_stocks"
    stocks_dir.mkdir()
    (stocks_dir / "catalog_stocks.csv").write_text(csv_content)

    # AV has B and C
    av_csv_active = (
        "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
        "B,Corp B,NYSE,Stock,2000-01-01,null,Active\n"
        "C,Corp C,NYSE,Stock,2000-01-01,null,Active\n"
    )
    av_csv_delisted = (
        "symbol,name,exchange,assetType,ipoDate,delistingDate,status\n"
    )
    mock_fetch_text.side_effect = [av_csv_active, av_csv_delisted]

    # OVERVIEW: B -> CONSUMER STAPLES, C -> FINANCIALS
    def overview_side_effect(url):
        if "symbol=B" in url:
            return {"Sector": "CONSUMER STAPLES"}
        if "symbol=C" in url:
            return {"Sector": "FINANCIALS"}
        return {"Sector": None}

    mock_fetch_json.side_effect = overview_side_effect

    init_stocks_etfs("fake-key", MOCK_DIR, stocks_dir)

    stocks = pl.read_parquet(MOCK_DIR / "stocks.parquet")

    a = stocks.filter(pl.col("symbol") == "A")
    assert a["sector"].to_list()[0] == "Consumer Defensive"

    # B: CSV has empty sector -> OVERVIEW returns CONSUMER STAPLES -> maps to Consumer Defensive
    b = stocks.filter(pl.col("symbol") == "B")
    assert b["sector"].to_list()[0] == "Consumer Defensive"

    # C: AV only -> OVERVIEW returns FINANCIALS -> maps to Financial Services
    c = stocks.filter(pl.col("symbol") == "C")
    assert c["sector"].to_list()[0] == "Financial Services"


# ── Other catalogs (unchanged) ───────────────────────────────────────


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
    assert df.height == 15
    assert "REAL_GDP" in df["symbol"].to_list()
    assert all(s == "Active" for s in df["status"].to_list())


@patch("asset_catalog_service.updates.stocks_etfs.fetch_json")
@patch("asset_catalog_service.updates.stocks_etfs.fetch_text")
def test_init_yield_status(mock_fetch_text, mock_fetch_json):
    # yield_status needs stocks.parquet first
    mock_fetch_text.side_effect = [LISTING_ACTIVE_CSV, LISTING_DELISTED_CSV]
    mock_fetch_json.side_effect = lambda url: OVERVIEW_RESPONSES.get(
        url.split("symbol=")[1].split("&")[0], {"Sector": None}
    )
    init_stocks_etfs("fake-key", MOCK_DIR)

    update_yield_status(MOCK_DIR)

    df = pl.read_parquet(MOCK_DIR / "yield_status.parquet")
    assert df.height == 4  # 3 stocks (AAPL, MSFT, OLD) + 1 ETF (SPY)
    assert "prices" in df.columns
    assert "prices_daily" in df.columns
    assert "sentiment" in df.columns
    assert "etf_profile" in df.columns
    assert "direct" in df.columns
    assert df["prices"].null_count() == 4
    assert df["prices_daily"].null_count() == 4
    assert df["date"].to_list() == [date.today()] * 4


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
        "symbol", "name", "reportedDate", "fiscalDateEnding",
        "estimate", "currency", "timeOfTheDay", "cast_issues",
    }
    # BAD row should have cast issues
    bad_row = df.filter(pl.col("symbol") == "BAD")
    assert bad_row["cast_issues"].to_list()[0] is not None
    assert "reportedDate" in bad_row["cast_issues"].to_list()[0]
    assert "estimate" in bad_row["cast_issues"].to_list()[0]

    # Good rows should have no cast issues
    good = df.filter(pl.col("symbol") == "AAPL")
    assert good["cast_issues"].to_list()[0] is None
    assert good["estimate"].to_list()[0] == pytest.approx(1.62)
