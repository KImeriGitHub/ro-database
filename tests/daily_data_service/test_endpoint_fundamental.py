"""Unit tests for ``daily_data_service.endpoints._fundamental``.

The shared dispatcher backs ``income_statement``, ``balance_sheet``,
``cash_flow``, ``earnings``, and ``earnings_estimates`` for the daily pull.
The defining behaviour beyond the historical version is:

  - 5-year truncation: rows with ``fiscalDateEnding < folder_date - 5y`` are
    dropped before write.
  - ``skip_empty_yield``: when True, symbols flagged False in
    ``yield_status.parquet`` are skipped with an ``empty_content`` issue
    instead of an HTTP fetch.
  - ``symbols_filter``: restricts iteration to a subset (used by the weekend
    retry path).
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
from daily_data_service.endpoints import income_statement as ep
from daily_data_service.endpoints import _fundamental as fund


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def fast_limiter():
    return RateLimiter(calls_per_minute=10000.0, window=1.0, min_gap=0.0)


def _make_catalog(catalog_dir: Path, symbols: list[str]) -> None:
    catalog_dir.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame({
        "symbol": symbols,
        "name": symbols,
        "ipoDate": [None] * len(symbols),
        "delistingDate": [None] * len(symbols),
        "status": ["Active"] * len(symbols),
    }).cast({"ipoDate": pl.Date, "delistingDate": pl.Date, "status": pl.Utf8})
    df.write_parquet(catalog_dir / "stocks.parquet", compression="zstd")


def _make_yield_status(catalog_dir: Path, symbols: list[str], cell: dict[str, bool | None]) -> None:
    """Write a minimal ``yield_status.parquet`` with the columns the tests need."""
    catalog_dir.mkdir(parents=True, exist_ok=True)
    cols = {"symbol": symbols, "date": [date(2026, 4, 14)] * len(symbols)}
    for endpoint, value in cell.items():
        cols[endpoint] = [value] * len(symbols)
    pl.DataFrame(cols).write_parquet(catalog_dir / "yield_status.parquet")


_RESPONSE_WITH_TWO_DECADES = {
    "symbol": "AAPL",
    "annualReports": [
        {"fiscalDateEnding": "2024-09-30", "reportedCurrency": "USD",
         "totalRevenue": "100"},
        {"fiscalDateEnding": "2019-09-30", "reportedCurrency": "USD",
         "totalRevenue": "70"},   # within 5y cutoff at folder=2026-04-17
        {"fiscalDateEnding": "2014-09-30", "reportedCurrency": "USD",
         "totalRevenue": "50"},   # before cutoff
        {"fiscalDateEnding": "2010-09-30", "reportedCurrency": "USD",
         "totalRevenue": "30"},   # before cutoff
    ],
    "quarterlyReports": [
        {"fiscalDateEnding": "2024-09-30", "reportedCurrency": "USD",
         "totalRevenue": "26"},
    ],
}


# ---------------------------------------------------------------------------
# 5-year truncation
# ---------------------------------------------------------------------------


def test_truncates_annual_to_five_years_before_folder_date(tmp_path, fast_limiter):
    """folder_date = 2026-04-17, cutoff = 2021-04-17. 2024 row is kept,
    everything older than 2021 is dropped. Empty quarter frames still get
    written (with full schema) -- callers rely on file presence."""
    catalog = tmp_path / "catalog"
    daily = tmp_path / "daily"
    _make_catalog(catalog, ["AAPL"])

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        return _RESPONSE_WITH_TWO_DECADES

    tracker = IssueTracker()
    # Patch _fundamental's own fetch_av_json import (NOT historical's).
    with patch.object(fund, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_income_statement(
            catalog_dir=catalog, daily_dir=daily, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks",
            folder_date=date(2026, 4, 17),
            previous_date=date(2026, 4, 14),
        ))

    annual = pl.read_parquet(
        daily / "stocks" / "income_statement" / "stocks_AAPL_annual.parquet"
    )
    assert annual["fiscalDateEnding"].to_list() == [
        date(2024, 9, 30),
    ]


# ---------------------------------------------------------------------------
# skip_empty_yield
# ---------------------------------------------------------------------------


def test_skip_empty_yield_records_empty_content_without_fetching(tmp_path, fast_limiter):
    """When ``yield_status[income_statement] == False`` for a symbol, no HTTP
    call is issued and an ``empty_content`` issue is recorded so the next
    finalize keeps the cell False."""
    catalog = tmp_path / "catalog"
    daily = tmp_path / "daily"
    _make_catalog(catalog, ["AAPL", "MSFT"])
    _make_yield_status(catalog, ["AAPL", "MSFT"],
                       {"income_statement": False})

    fetch_calls: list[str] = []

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        fetch_calls.append(url)
        return _RESPONSE_WITH_TWO_DECADES

    tracker = IssueTracker()
    with patch.object(fund, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_income_statement(
            catalog_dir=catalog, daily_dir=daily, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks",
            folder_date=date(2026, 4, 17),
            previous_date=date(2026, 4, 14),
            skip_empty_yield=True,
        ))

    assert fetch_calls == []
    skipped = [r for r in tracker._rows if r["issue_type"] == "empty_content"]
    assert {r["symbol"] for r in skipped} == {"AAPL", "MSFT"}
    assert all("revalidate on weekend" in r["detail"] for r in skipped)


def test_skip_empty_yield_does_not_skip_null_or_true_cells(tmp_path, fast_limiter):
    """Only explicit False values qualify for skipping. Null cells (new
    symbols) and True cells (last finalize confirmed yield) are still
    queried."""
    catalog = tmp_path / "catalog"
    daily = tmp_path / "daily"
    _make_catalog(catalog, ["AAPL", "MSFT", "GOOG"])
    pl.DataFrame({
        "symbol": ["AAPL", "MSFT", "GOOG"],
        "date": [date(2026, 4, 14)] * 3,
        "income_statement": [None, True, False],
    }).write_parquet(catalog / "yield_status.parquet")

    fetched: list[str] = []

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        for s in ("AAPL", "MSFT", "GOOG"):
            if f"symbol={s}&" in url:
                fetched.append(s)
        return _RESPONSE_WITH_TWO_DECADES

    tracker = IssueTracker()
    with patch.object(fund, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_income_statement(
            catalog_dir=catalog, daily_dir=daily, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks",
            folder_date=date(2026, 4, 17),
            previous_date=date(2026, 4, 14),
            skip_empty_yield=True,
        ))

    assert sorted(fetched) == ["AAPL", "MSFT"]


# ---------------------------------------------------------------------------
# symbols_filter
# ---------------------------------------------------------------------------


def test_symbols_filter_restricts_iteration(tmp_path, fast_limiter):
    """The weekend retry path passes ``symbols_filter`` so only the affected
    (symbol, endpoint) pairs are re-queried."""
    catalog = tmp_path / "catalog"
    daily = tmp_path / "daily"
    _make_catalog(catalog, ["AAPL", "MSFT", "GOOG"])

    fetched: list[str] = []

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        for s in ("AAPL", "MSFT", "GOOG"):
            if f"symbol={s}&" in url:
                fetched.append(s)
        return _RESPONSE_WITH_TWO_DECADES

    tracker = IssueTracker()
    with patch.object(fund, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_income_statement(
            catalog_dir=catalog, daily_dir=daily, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks",
            folder_date=date(2026, 4, 17),
            previous_date=date(2026, 4, 14),
            symbols_filter={"AAPL"},
        ))

    assert fetched == ["AAPL"]


# ---------------------------------------------------------------------------
# Skip when both files exist
# ---------------------------------------------------------------------------


def test_skip_when_both_files_exist(tmp_path, fast_limiter):
    """Resume guarantee for partial re-runs: if both annual and quarterly
    parquets exist, do not refetch."""
    catalog = tmp_path / "catalog"
    daily = tmp_path / "daily"
    _make_catalog(catalog, ["AAPL"])
    out = daily / "stocks" / "income_statement"
    out.mkdir(parents=True)
    (out / "stocks_AAPL_annual.parquet").write_bytes(b"a")
    (out / "stocks_AAPL_quarterly.parquet").write_bytes(b"q")

    fetch_calls: list[str] = []

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        fetch_calls.append(url)
        return _RESPONSE_WITH_TWO_DECADES

    tracker = IssueTracker()
    with patch.object(fund, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_income_statement(
            catalog_dir=catalog, daily_dir=daily, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks",
            folder_date=date(2026, 4, 17),
            previous_date=date(2026, 4, 14),
        ))

    assert fetch_calls == []
    assert (out / "stocks_AAPL_annual.parquet").read_bytes() == b"a"
