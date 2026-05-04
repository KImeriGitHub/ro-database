"""Unit tests for ``historical_data_setup.endpoints.prices_daily``.

Two paths are exercised independently: the FirstRate Data CSV path (which
derives ``DividendAmount`` and ``SplitCoefficient`` from three CSVs and never
calls the network) and the Alpha Vantage path (single ``TIME_SERIES_DAILY_ADJUSTED``
JSON, mocked through a scripted session).
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from historical_data_setup._common import IssueTracker, RateLimiter
from historical_data_setup.endpoints import prices_daily as ep


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run(coro):
    return asyncio.run(coro)


def _make_catalog(catalog_dir: Path, asset_type: str, symbols: list[str]) -> None:
    """Write a minimal stocks/etfs catalog parquet for ``read_catalog_symbols``."""
    catalog_dir.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame({
        "symbol": symbols,
        "name": [s for s in symbols],
        "ipoDate": [None] * len(symbols),
        "delistingDate": [None] * len(symbols),
        "status": ["Active"] * len(symbols),
    }).cast({"ipoDate": pl.Date, "delistingDate": pl.Date, "status": pl.Utf8})
    df.write_parquet(catalog_dir / f"{asset_type}.parquet", compression="zstd")


_AV_PAYLOAD_AAPL = {
    "Meta Data": {
        "1. Information": "Daily Adjusted",
        "2. Symbol": "AAPL",
        "5. Time Zone": "US/Eastern",
    },
    "Time Series (Daily)": {
        "2024-01-04": {
            "1. open": "100.0", "2. high": "101.0", "3. low": "99.0",
            "4. close": "100.5", "5. adjusted close": "100.5",
            "6. volume": "1000", "7. dividend amount": "0.0",
            "8. split coefficient": "1.0",
        },
        "2024-01-03": {
            "1. open": "99.0", "2. high": "100.0", "3. low": "98.0",
            "4. close": "99.5", "5. adjusted close": "99.5",
            "6. volume": "2000", "7. dividend amount": "0.25",
            "8. split coefficient": "1.0",
        },
    },
}


@pytest.fixture
def fast_limiter():
    """RateLimiter that never blocks (irrelevant to the assertions)."""
    return RateLimiter(calls_per_minute=10000.0, window=1.0, min_gap=0.0)


# ---------------------------------------------------------------------------
# Alpha Vantage path
# ---------------------------------------------------------------------------


def test_av_path_writes_parquet_with_expected_schema(tmp_path, fast_limiter):
    """Happy path: one symbol, AV returns two bars, parquet has Date+OHLCV+
    DividendAmount+SplitCoefficient sorted ascending and cast to Float32."""
    catalog = tmp_path / "catalog"
    historical = tmp_path / "historical"
    _make_catalog(catalog, "stocks", ["AAPL"])

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        return _AV_PAYLOAD_AAPL

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_daily_prices(
            catalog_dir=catalog,
            historical_dir=historical,
            api_key="fake",
            session=None,
            rate_limiter=fast_limiter,
            issue_tracker=tracker,
            asset_type="stocks",
            frd_dir=None,
        ))

    out = historical / "stocks" / "prices_daily" / "stocks_AAPL.parquet"
    assert out.exists()
    df = pl.read_parquet(out)
    assert df.columns == [
        "Date", "Open", "High", "Low", "Close", "Volume",
        "DividendAmount", "SplitCoefficient",
    ]
    assert df.schema["Date"] == pl.Date
    for c in ("Open", "High", "Low", "Close", "Volume",
              "DividendAmount", "SplitCoefficient"):
        assert df.schema[c] == pl.Float32, c
    assert df["Date"].to_list() == [
        __import__("datetime").date(2024, 1, 3),
        __import__("datetime").date(2024, 1, 4),
    ]
    assert tracker.count == 0


def test_av_path_skip_existing_file(tmp_path, fast_limiter):
    """If the per-symbol parquet already exists, the endpoint must NOT fetch
    or overwrite -- this is what makes historical setup resumable."""
    catalog = tmp_path / "catalog"
    historical = tmp_path / "historical"
    _make_catalog(catalog, "stocks", ["AAPL"])
    out = historical / "stocks" / "prices_daily" / "stocks_AAPL.parquet"
    out.parent.mkdir(parents=True)
    out.write_bytes(b"sentinel")

    fetch_calls: list[str] = []

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        fetch_calls.append(url)
        return _AV_PAYLOAD_AAPL

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_daily_prices(
            catalog_dir=catalog,
            historical_dir=historical,
            api_key="fake",
            session=None,
            rate_limiter=fast_limiter,
            issue_tracker=tracker,
            asset_type="stocks",
            frd_dir=None,
        ))

    assert fetch_calls == []
    assert out.read_bytes() == b"sentinel"


def test_av_path_missing_time_series_records_structure_error(tmp_path, fast_limiter):
    """Missing the ``Time Series (Daily)`` key is a structural failure --
    no parquet, ``structure_error`` issue."""
    catalog = tmp_path / "catalog"
    historical = tmp_path / "historical"
    _make_catalog(catalog, "stocks", ["AAPL"])

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        return {"Meta Data": {"5. Time Zone": "US/Eastern"}}

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_daily_prices(
            catalog_dir=catalog, historical_dir=historical, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks", frd_dir=None,
        ))

    out = historical / "stocks" / "prices_daily" / "stocks_AAPL.parquet"
    assert not out.exists()
    issues = [r for r in tracker._rows if r["issue_type"] == "structure_error"]
    assert len(issues) == 1
    assert "Time Series (Daily)" in issues[0]["detail"]


def test_av_path_empty_time_series_records_empty_content(tmp_path, fast_limiter):
    """An empty time series object is data-quality, not structural."""
    catalog = tmp_path / "catalog"
    historical = tmp_path / "historical"
    _make_catalog(catalog, "stocks", ["AAPL"])

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        return {
            "Meta Data": {"5. Time Zone": "US/Eastern"},
            "Time Series (Daily)": {},
        }

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_daily_prices(
            catalog_dir=catalog, historical_dir=historical, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks", frd_dir=None,
        ))

    issues = [r for r in tracker._rows if r["issue_type"] == "empty_content"]
    assert len(issues) == 1


def test_av_path_av_throttle_logs_and_continues(tmp_path, fast_limiter):
    """AVResponseError -> ``av_throttle`` issue, no parquet, but the loop
    moves on to the next symbol."""
    catalog = tmp_path / "catalog"
    historical = tmp_path / "historical"
    _make_catalog(catalog, "stocks", ["AAPL", "MSFT"])

    from historical_data_setup._common import AVResponseError

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        if "AAPL" in url:
            raise AVResponseError("rate limited")
        return _AV_PAYLOAD_AAPL

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_daily_prices(
            catalog_dir=catalog, historical_dir=historical, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks", frd_dir=None,
        ))

    # AAPL throttled, no file. MSFT succeeded.
    assert not (historical / "stocks" / "prices_daily" / "stocks_AAPL.parquet").exists()
    assert (historical / "stocks" / "prices_daily" / "stocks_MSFT.parquet").exists()
    throttled = [r for r in tracker._rows if r["issue_type"] == "av_throttle"]
    assert [r["symbol"] for r in throttled] == ["AAPL"]


def test_av_path_per_bar_cast_failure_is_recorded(tmp_path, fast_limiter):
    """A non-numeric OHLCV value records ``cast_failure`` for that one bar
    but the surviving bars are still written."""
    catalog = tmp_path / "catalog"
    historical = tmp_path / "historical"
    _make_catalog(catalog, "stocks", ["AAPL"])

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        return {
            "Meta Data": {"5. Time Zone": "US/Eastern"},
            "Time Series (Daily)": {
                "2024-01-04": {
                    "1. open": "100", "2. high": "101", "3. low": "99",
                    "4. close": "100.5", "6. volume": "1000",
                    "7. dividend amount": "0.0", "8. split coefficient": "1.0",
                },
                "2024-01-05": {
                    "1. open": "ack", "2. high": "101", "3. low": "99",
                    "4. close": "100.5", "6. volume": "1000",
                    "7. dividend amount": "0.0", "8. split coefficient": "1.0",
                },
            },
        }

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_daily_prices(
            catalog_dir=catalog, historical_dir=historical, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks", frd_dir=None,
        ))

    df = pl.read_parquet(historical / "stocks" / "prices_daily" / "stocks_AAPL.parquet")
    assert df.height == 1
    cast = [r for r in tracker._rows if r["issue_type"] == "cast_failure"]
    assert len(cast) == 1
    assert "2024-01-05" in cast[0]["detail"]


# ---------------------------------------------------------------------------
# FRD path
# ---------------------------------------------------------------------------


def _write_frd_csvs(frd_dir: Path, symbol: str, rows: list[dict]) -> None:
    """Write three FRD CSVs (unadjusted / split-adjusted / split+div-adjusted).

    *rows* is a list of dicts with keys: timestamp, open, high, low,
    {unadj_close, sa_close, sda_close}, volume.
    """
    frd_dir.mkdir(parents=True, exist_ok=True)

    def _write(suffix: str, close_key: str):
        df = pl.DataFrame([
            {
                "timestamp": r["timestamp"],
                "open": r["open"],
                "high": r["high"],
                "low": r["low"],
                "close": r[close_key],
                "volume": r["volume"],
            } for r in rows
        ])
        df.write_csv(frd_dir / f"{symbol}_{suffix}.csv")

    _write("1day_unadjusted", "unadj_close")
    _write("1day_splitadjusted", "sa_close")
    _write("1day_splitdivadjusted", "sda_close")


def test_frd_path_takes_precedence_over_av(tmp_path, fast_limiter):
    """When all three FRD CSVs exist for a symbol, the AV branch must not
    be entered (no fetch call). The output parquet is built from the CSVs."""
    catalog = tmp_path / "catalog"
    historical = tmp_path / "historical"
    frd = tmp_path / "frd"
    _make_catalog(catalog, "stocks", ["AAPL"])

    # Three flat days, no splits or dividends -> SplitCoefficient=1, Dividend=0.
    _write_frd_csvs(frd, "AAPL", [
        {"timestamp": "2024-01-03", "open": "1", "high": "1.1", "low": "0.9",
         "unadj_close": "1.0", "sa_close": "1.0", "sda_close": "1.0",
         "volume": "100"},
        {"timestamp": "2024-01-04", "open": "1", "high": "1.1", "low": "0.9",
         "unadj_close": "1.0", "sa_close": "1.0", "sda_close": "1.0",
         "volume": "200"},
    ])

    fetch_calls: list[str] = []

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        fetch_calls.append(url)
        return {}

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_daily_prices(
            catalog_dir=catalog, historical_dir=historical, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks", frd_dir=frd,
        ))

    assert fetch_calls == [], "AV must not be called when FRD covers the symbol"
    df = pl.read_parquet(historical / "stocks" / "prices_daily" / "stocks_AAPL.parquet")
    assert df.height == 2
    assert df["SplitCoefficient"].to_list() == pytest.approx([1.0, 1.0])
    assert df["DividendAmount"].to_list() == pytest.approx([0.0, 0.0])
