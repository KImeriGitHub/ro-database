"""Pins the ``status in {"Active", "Corrupted"}`` catalog filter.

Two tests:

1. Direct ``polars`` predicate test: guards against future polars releases
   changing ``Expr.is_in`` semantics (which has bitten us before).
2. End-to-end via ``fetch_daily_prices``: confirms ``active_only=True`` keeps
   Active + Corrupted symbols, drops Delisted; ``active_only=False`` keeps
   every catalog row.
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


def _make_mixed_catalog(catalog_dir: Path, rows: list[tuple[str, str]]) -> None:
    """Write a stocks.parquet with ``(symbol, status)`` pairs."""
    catalog_dir.mkdir(parents=True, exist_ok=True)
    symbols = [r[0] for r in rows]
    statuses = [r[1] for r in rows]
    df = pl.DataFrame({
        "symbol": symbols,
        "name": symbols,
        "sector": ["Technology"] * len(symbols),
        "ipoDate": [None] * len(symbols),
        "delistingDate": [None] * len(symbols),
        "status": statuses,
    }).cast({"ipoDate": pl.Date, "delistingDate": pl.Date, "status": pl.Utf8})
    df.write_parquet(catalog_dir / "stocks.parquet", compression="zstd")


def _bar(close: float = 100.0) -> dict:
    return {
        "1. open": str(close), "2. high": str(close + 1),
        "3. low": str(close - 1), "4. close": str(close),
        "5. adjusted close": str(close), "6. volume": "1000",
        "7. dividend amount": "0.0", "8. split coefficient": "1.0",
    }


# ---------------------------------------------------------------------------
# 1. Polars predicate behaviour
# ---------------------------------------------------------------------------


def test_polars_is_in_keeps_active_and_corrupted_only():
    """``pl.col("status").is_in(["Active", "Corrupted"])`` must keep exactly
    those two statuses and drop Delisted and null. This is the predicate the
    daily endpoints rely on; a polars upgrade that changes ``is_in`` (e.g.
    by treating null as a member of the list) would silently broaden or
    narrow the daily query set."""
    df = pl.DataFrame({
        "symbol": ["A_ACTIVE", "B_CORRUPT", "C_DELIST", "D_NULL"],
        "status": ["Active", "Corrupted", "Delisted", None],
    }, schema={"symbol": pl.Utf8, "status": pl.Utf8})

    kept = df.filter(
        pl.col("status").is_in(["Active", "Corrupted"])
    )["symbol"].to_list()

    assert kept == ["A_ACTIVE", "B_CORRUPT"]


# ---------------------------------------------------------------------------
# 2. End-to-end: prices_daily honours the filter
# ---------------------------------------------------------------------------


def test_active_only_true_queries_active_and_corrupted_skips_delisted(
    tmp_path, fast_limiter,
):
    """Default daily-run behaviour: ``active_only=True`` queries Active and
    Corrupted symbols but not Delisted."""
    catalog = tmp_path / "catalog"
    daily = tmp_path / "daily"
    _make_mixed_catalog(catalog, [
        ("AAPL", "Active"),
        ("CORR", "Corrupted"),
        ("DEAD", "Delisted"),
    ])

    queried: list[str] = []

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        queried.append(url.split("symbol=")[1].split("&")[0])
        return {
            "Meta Data": {"5. Time Zone": "US/Eastern"},
            "Time Series (Daily)": {"2026-04-17": _bar()},
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

    assert sorted(queried) == ["AAPL", "CORR"]
    out_dir = daily / "stocks" / "prices_daily"
    assert (out_dir / "stocks_AAPL.parquet").exists()
    assert (out_dir / "stocks_CORR.parquet").exists()
    assert not (out_dir / "stocks_DEAD.parquet").exists()


def test_active_only_false_includes_delisted(tmp_path, fast_limiter):
    """Weekend retry behaviour: ``active_only=False`` widens the query set
    to include Delisted symbols too."""
    catalog = tmp_path / "catalog"
    daily = tmp_path / "daily"
    _make_mixed_catalog(catalog, [
        ("AAPL", "Active"),
        ("CORR", "Corrupted"),
        ("DEAD", "Delisted"),
    ])

    queried: list[str] = []

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        queried.append(url.split("symbol=")[1].split("&")[0])
        return {
            "Meta Data": {"5. Time Zone": "US/Eastern"},
            "Time Series (Daily)": {"2026-04-17": _bar()},
        }

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_daily_prices(
            catalog_dir=catalog, daily_dir=daily, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks",
            folder_date=date(2026, 4, 17),
            previous_date=date(2026, 4, 14),
            active_only=False,
        ))

    assert sorted(queried) == ["AAPL", "CORR", "DEAD"]
