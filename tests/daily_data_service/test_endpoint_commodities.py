"""Unit tests for ``daily_data_service.endpoints.commodities``.

The endpoint dispatches three flavours by symbol:

  - Daily-interval (WTI, BRENT, NATURAL_GAS): hits the per-symbol AV function
    with ``interval=daily``, truncated to ``(previous_date, folder_date]``.
  - Monthly-interval (COPPER, ALUMINUM, WHEAT, ...): same shape but uses
    ``interval=monthly`` and a ``Date >= folder_date - 1y`` cutoff.
  - Gold/Silver (XAU, XAG): hits ``GOLD_SILVER_HISTORY`` with the AV symbol
    name (GOLD, SILVER), reading ``price`` instead of ``value``.

Symbols outside these three groups must be reported as a structure error.
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
from daily_data_service.endpoints import commodities as ep


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def fast_limiter():
    return RateLimiter(calls_per_minute=10000.0, window=1.0, min_gap=0.0)


def _make_commodities_catalog(catalog_dir: Path, symbols: list[str]) -> None:
    catalog_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "symbol": symbols,
        "name": symbols,
    }).write_parquet(catalog_dir / "commodities.parquet")


# ---------------------------------------------------------------------------
# Dispatch by symbol type
# ---------------------------------------------------------------------------


def test_daily_symbol_uses_interval_daily_and_window_truncation(tmp_path, fast_limiter):
    """WTI -> ``function=WTI&interval=daily``. Window is
    ``(previous_date, folder_date]``."""
    catalog = tmp_path / "catalog"
    daily = tmp_path / "daily"
    _make_commodities_catalog(catalog, ["WTI"])

    captured_urls: list[str] = []

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        captured_urls.append(url)
        return {
            "name": "Crude Oil",
            "interval": "daily",
            "unit": "dollars per barrel",
            "data": [
                {"date": "2026-04-13", "value": "80.0"},  # < prev, dropped
                {"date": "2026-04-15", "value": "81.0"},  # in window
                {"date": "2026-04-17", "value": "82.0"},  # == folder, kept
                {"date": "2026-04-20", "value": "83.0"},  # > folder, dropped
            ],
        }

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_commodities(
            catalog_dir=catalog, daily_dir=daily, api_key="k",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="commodities",
            folder_date=date(2026, 4, 17),
            previous_date=date(2026, 4, 14),
        ))

    assert "function=WTI" in captured_urls[0]
    assert "interval=daily" in captured_urls[0]
    df = pl.read_parquet(daily / "commodities" / "commodities_WTI.parquet")
    assert df["Date"].to_list() == [date(2026, 4, 15), date(2026, 4, 17)]
    assert df["value"].to_list() == pytest.approx([81.0, 82.0])
    assert df["unit"].to_list() == ["dollars per barrel"] * 2


def test_monthly_symbol_uses_interval_monthly_and_one_year_cutoff(tmp_path, fast_limiter):
    """COPPER -> monthly. Cutoff is ``folder_date - 1y`` inclusive
    (``2026-04-17 - 1y = 2025-04-17``). Rows older than that are dropped."""
    catalog = tmp_path / "catalog"
    daily = tmp_path / "daily"
    _make_commodities_catalog(catalog, ["COPPER"])

    captured_urls: list[str] = []

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        captured_urls.append(url)
        return {
            "name": "Copper",
            "unit": "dollars per pound",
            "data": [
                {"date": "2024-04-01", "value": "4.0"},   # before cutoff
                {"date": "2025-05-01", "value": "4.5"},   # after cutoff
                {"date": "2026-03-01", "value": "5.0"},
            ],
        }

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_commodities(
            catalog_dir=catalog, daily_dir=daily, api_key="k",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="commodities",
            folder_date=date(2026, 4, 17),
            previous_date=date(2026, 4, 14),
        ))

    assert "function=COPPER" in captured_urls[0]
    assert "interval=monthly" in captured_urls[0]
    df = pl.read_parquet(daily / "commodities" / "commodities_COPPER.parquet")
    assert df["Date"].to_list() == [date(2025, 5, 1), date(2026, 3, 1)]


def test_xau_routes_to_gold_silver_endpoint_and_reads_price_field(tmp_path, fast_limiter):
    """XAU is mapped to GOLD on the ``GOLD_SILVER_HISTORY`` endpoint.
    The ``price`` field is renamed to ``value``; the ``unit`` is fixed."""
    catalog = tmp_path / "catalog"
    daily = tmp_path / "daily"
    _make_commodities_catalog(catalog, ["XAU"])

    captured_urls: list[str] = []

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        captured_urls.append(url)
        return {
            "data": [
                {"date": "2026-04-15", "price": "2400.0"},
                {"date": "2026-04-17", "price": "2410.0"},
            ],
        }

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_commodities(
            catalog_dir=catalog, daily_dir=daily, api_key="k",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="commodities",
            folder_date=date(2026, 4, 17),
            previous_date=date(2026, 4, 14),
        ))

    assert "function=GOLD_SILVER_HISTORY" in captured_urls[0]
    assert "symbol=GOLD" in captured_urls[0]
    df = pl.read_parquet(daily / "commodities" / "commodities_XAU.parquet")
    assert df["value"].to_list() == pytest.approx([2400.0, 2410.0])
    assert df["unit"].to_list() == ["dollars per troy ounce"] * 2


def test_unknown_commodity_symbol_records_structure_error(tmp_path, fast_limiter):
    """A symbol not in any of the three groups -> structure_error issue, no
    HTTP call, no parquet."""
    catalog = tmp_path / "catalog"
    daily = tmp_path / "daily"
    _make_commodities_catalog(catalog, ["TIN"])

    fetch_calls: list[str] = []

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        fetch_calls.append(url)
        return {"data": []}

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_commodities(
            catalog_dir=catalog, daily_dir=daily, api_key="k",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="commodities",
            folder_date=date(2026, 4, 17),
            previous_date=date(2026, 4, 14),
        ))

    assert fetch_calls == []
    issues = [r for r in tracker._rows if r["issue_type"] == "structure_error"]
    assert len(issues) == 1
    assert "TIN" in issues[0]["detail"]


# ---------------------------------------------------------------------------
# Issue paths shared across all branches
# ---------------------------------------------------------------------------


def test_missing_data_key_records_structure_error(tmp_path, fast_limiter):
    catalog = tmp_path / "catalog"
    daily = tmp_path / "daily"
    _make_commodities_catalog(catalog, ["WTI"])

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        return {"name": "Crude Oil"}  # no 'data'

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_commodities(
            catalog_dir=catalog, daily_dir=daily, api_key="k",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="commodities",
            folder_date=date(2026, 4, 17),
            previous_date=date(2026, 4, 14),
        ))

    assert any(r["issue_type"] == "structure_error" for r in tracker._rows)
    assert not (daily / "commodities" / "commodities_WTI.parquet").exists()


def test_empty_data_records_empty_content(tmp_path, fast_limiter):
    catalog = tmp_path / "catalog"
    daily = tmp_path / "daily"
    _make_commodities_catalog(catalog, ["WTI"])

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        return {"name": "Crude Oil", "unit": "$/bbl", "data": []}

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_commodities(
            catalog_dir=catalog, daily_dir=daily, api_key="k",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="commodities",
            folder_date=date(2026, 4, 17),
            previous_date=date(2026, 4, 14),
        ))

    assert any(r["issue_type"] == "empty_content" for r in tracker._rows)
    assert not (daily / "commodities" / "commodities_WTI.parquet").exists()


def test_null_sentinel_value_is_recorded_as_null_not_cast_failure(tmp_path, fast_limiter):
    """The AV null sentinels {None, "None", "", "."} are *not* cast failures
    -- they are stored as a null Float32 cell and the row survives."""
    catalog = tmp_path / "catalog"
    daily = tmp_path / "daily"
    _make_commodities_catalog(catalog, ["WTI"])

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        return {
            "name": "Crude Oil",
            "unit": "dollars per barrel",
            "data": [
                {"date": "2026-04-15", "value": "."},
                {"date": "2026-04-17", "value": "82.0"},
            ],
        }

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_commodities(
            catalog_dir=catalog, daily_dir=daily, api_key="k",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="commodities",
            folder_date=date(2026, 4, 17),
            previous_date=date(2026, 4, 14),
        ))

    df = pl.read_parquet(daily / "commodities" / "commodities_WTI.parquet")
    assert df.height == 2
    # First row's value is null after the null-sentinel handling.
    assert df["value"].to_list()[0] is None
    assert df["value"].to_list()[1] == pytest.approx(82.0)
    assert not any(r["issue_type"] == "cast_failure" for r in tracker._rows)
