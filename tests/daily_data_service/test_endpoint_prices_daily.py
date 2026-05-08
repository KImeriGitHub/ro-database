"""Unit tests for ``daily_data_service.endpoints.prices_daily``.

This is the daily counterpart of the historical fetcher. The defining
behaviour is the ``(previous_date, folder_date]`` window applied client-side
after AV returns the trailing ~100 bars in ``compact`` mode. Bars outside
the window must be dropped; all other paths (skip-existing, throttle, missing
key) mirror the historical version.
"""

from __future__ import annotations

import asyncio
import sys
from datetime import date
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from historical_data_setup._common import IssueTracker, RateLimiter
from daily_data_service.endpoints import prices_daily as ep


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def fast_limiter():
    return RateLimiter(calls_per_minute=10000.0, window=1.0, min_gap=0.0)


def _make_catalog(catalog_dir: Path, asset_type: str, symbols: list[str]) -> None:
    catalog_dir.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame({
        "symbol": symbols,
        "name": symbols,
        "ipoDate": [None] * len(symbols),
        "delistingDate": [None] * len(symbols),
        "status": ["Active"] * len(symbols),
    }).cast({"ipoDate": pl.Date, "delistingDate": pl.Date, "status": pl.Utf8})
    df.write_parquet(catalog_dir / f"{asset_type}.parquet", compression="zstd")


def _bar(date_str: str, close: float = 100.0) -> dict:
    return {
        "1. open": str(close), "2. high": str(close + 1),
        "3. low": str(close - 1), "4. close": str(close),
        "5. adjusted close": str(close), "6. volume": "1000",
        "7. dividend amount": "0.0", "8. split coefficient": "1.0",
    }


# ---------------------------------------------------------------------------
# Window truncation
# ---------------------------------------------------------------------------


def test_truncates_to_seven_day_window_through_folder_date(
    tmp_path, fast_limiter,
):
    """The window is ``(min(previous_date, folder_date - 7d), folder_date]``.
    With ``previous_date=2026-04-14`` and ``folder_date=2026-04-17``, the
    floor pushes the lower bound back to ``2026-04-10`` (7 days before
    folder), so bars 04-11..04-17 are kept and only ``<= 04-10`` / ``> 04-17``
    are dropped."""
    catalog = tmp_path / "catalog"
    daily = tmp_path / "daily"
    _make_catalog(catalog, "stocks", ["AAPL"])

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        return {
            "Meta Data": {"5. Time Zone": "US/Eastern"},
            "Time Series (Daily)": {
                "2026-04-09": _bar("2026-04-09"),  # < window_lower, dropped
                "2026-04-10": _bar("2026-04-10"),  # == window_lower, dropped (strict <)
                "2026-04-11": _bar("2026-04-11"),  # in window (within 7d floor)
                "2026-04-13": _bar("2026-04-13"),  # in window
                "2026-04-14": _bar("2026-04-14"),  # in window (no longer == lower)
                "2026-04-15": _bar("2026-04-15"),  # in window
                "2026-04-16": _bar("2026-04-16"),  # in window
                "2026-04-17": _bar("2026-04-17"),  # == folder, kept
                "2026-04-18": _bar("2026-04-18"),  # > folder, dropped
            },
        }

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_daily_prices(
            catalog_dir=catalog, daily_dir=daily, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks",
            folder_date=date(2026, 4, 17),
            previous_date=date(2026, 4, 14),
        ))

    df = pl.read_parquet(daily / "stocks" / "prices_daily" / "stocks_AAPL.parquet")
    assert df["Date"].to_list() == [
        date(2026, 4, 11),
        date(2026, 4, 13),
        date(2026, 4, 14),
        date(2026, 4, 15),
        date(2026, 4, 16),
        date(2026, 4, 17),
    ]


def test_truncates_uses_previous_date_when_older_than_seven_day_floor(
    tmp_path, fast_limiter,
):
    """When ``previous_date`` is older than ``folder_date - 7d``, the window
    keeps using ``previous_date`` so a long outage's worth of bars is
    captured (the floor only widens the window, never narrows it)."""
    catalog = tmp_path / "catalog"
    daily = tmp_path / "daily"
    _make_catalog(catalog, "stocks", ["AAPL"])

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        return {
            "Meta Data": {"5. Time Zone": "US/Eastern"},
            "Time Series (Daily)": {
                "2026-03-31": _bar("2026-03-31"),  # == previous, dropped (strict <)
                "2026-04-01": _bar("2026-04-01"),  # in window
                "2026-04-09": _bar("2026-04-09"),  # in window
                "2026-04-17": _bar("2026-04-17"),  # == folder, kept
                "2026-04-18": _bar("2026-04-18"),  # > folder, dropped
            },
        }

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_daily_prices(
            catalog_dir=catalog, daily_dir=daily, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks",
            folder_date=date(2026, 4, 17),
            previous_date=date(2026, 3, 31),  # 17 days back, older than 7d floor
        ))

    df = pl.read_parquet(daily / "stocks" / "prices_daily" / "stocks_AAPL.parquet")
    assert df["Date"].to_list() == [
        date(2026, 4, 1), date(2026, 4, 9), date(2026, 4, 17),
    ]


def test_window_yields_empty_frame_when_no_bars_fall_inside(tmp_path, fast_limiter):
    """If no bars match the window, an EMPTY parquet is written (with the
    correct schema). This is by design: downstream tooling expects a file
    to exist for every (symbol, day) the loop dispatched."""
    catalog = tmp_path / "catalog"
    daily = tmp_path / "daily"
    _make_catalog(catalog, "stocks", ["AAPL"])

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        return {
            "Meta Data": {"5. Time Zone": "US/Eastern"},
            "Time Series (Daily)": {"2025-01-01": _bar("2025-01-01")},
        }

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_daily_prices(
            catalog_dir=catalog, daily_dir=daily, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks",
            folder_date=date(2026, 4, 17),
            previous_date=date(2026, 4, 14),
        ))

    out = daily / "stocks" / "prices_daily" / "stocks_AAPL.parquet"
    assert out.exists()
    df = pl.read_parquet(out)
    assert df.height == 0
    # Schema preserved.
    assert df.schema["Date"] == pl.Date
    assert df.schema["Close"] == pl.Float32


# ---------------------------------------------------------------------------
# Issue paths
# ---------------------------------------------------------------------------


def test_missing_time_series_key_records_structure_error(tmp_path, fast_limiter):
    catalog = tmp_path / "catalog"
    daily = tmp_path / "daily"
    _make_catalog(catalog, "stocks", ["AAPL"])

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        return {"Meta Data": {"5. Time Zone": "US/Eastern"}}

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_daily_prices(
            catalog_dir=catalog, daily_dir=daily, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks",
            folder_date=date(2026, 4, 17),
            previous_date=date(2026, 4, 14),
        ))

    assert any(r["issue_type"] == "structure_error" for r in tracker._rows)
    assert not (daily / "stocks" / "prices_daily" / "stocks_AAPL.parquet").exists()


def test_av_throttle_continues_to_next_symbol(tmp_path, fast_limiter):
    catalog = tmp_path / "catalog"
    daily = tmp_path / "daily"
    _make_catalog(catalog, "stocks", ["AAPL", "MSFT"])

    from historical_data_setup._common import AVResponseError

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        if "AAPL" in url:
            raise AVResponseError("rate limited")
        return {
            "Meta Data": {"5. Time Zone": "US/Eastern"},
            "Time Series (Daily)": {"2026-04-15": _bar("2026-04-15")},
        }

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_daily_prices(
            catalog_dir=catalog, daily_dir=daily, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks",
            folder_date=date(2026, 4, 17),
            previous_date=date(2026, 4, 14),
        ))

    assert not (daily / "stocks" / "prices_daily" / "stocks_AAPL.parquet").exists()
    assert (daily / "stocks" / "prices_daily" / "stocks_MSFT.parquet").exists()
    assert any(
        r["symbol"] == "AAPL" and r["issue_type"] == "av_throttle"
        for r in tracker._rows
    )


# ---------------------------------------------------------------------------
# symbols_filter
# ---------------------------------------------------------------------------


def test_symbols_filter_restricts_iteration(tmp_path, fast_limiter):
    """Weekend retry mode: ``symbols_filter`` must subset the catalog before
    iteration. Symbols not in the filter are not fetched, even if their
    parquet does not yet exist."""
    catalog = tmp_path / "catalog"
    daily = tmp_path / "daily"
    _make_catalog(catalog, "stocks", ["AAPL", "MSFT", "GOOG"])

    fetched_symbols: list[str] = []

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        # AV URL contains ?symbol=<X>&...
        for s in ("AAPL", "MSFT", "GOOG"):
            if f"symbol={s}&" in url:
                fetched_symbols.append(s)
                break
        return {
            "Meta Data": {"5. Time Zone": "US/Eastern"},
            "Time Series (Daily)": {"2026-04-15": _bar("2026-04-15")},
        }

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_daily_prices(
            catalog_dir=catalog, daily_dir=daily, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks",
            folder_date=date(2026, 4, 17),
            previous_date=date(2026, 4, 14),
            symbols_filter={"AAPL", "GOOG"},
        ))

    assert sorted(fetched_symbols) == ["AAPL", "GOOG"]
    assert (daily / "stocks" / "prices_daily" / "stocks_AAPL.parquet").exists()
    assert (daily / "stocks" / "prices_daily" / "stocks_GOOG.parquet").exists()
    assert not (daily / "stocks" / "prices_daily" / "stocks_MSFT.parquet").exists()
