"""Unit tests for ``historical_data_setup._common.fetch_fundamental_endpoint``
through one of its thin wrappers (``income_statement``).

This single shared driver backs ``income_statement``, ``balance_sheet``,
``cash_flow``, ``earnings``, and ``earnings_estimates``, so coverage on it is
high-leverage. Tests focus on the per-symbol output split (annual vs
quarterly), structural / empty / throttle issue handling, and skip-on-existing
behaviour.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from historical_data_setup import _common as hc
from historical_data_setup._common import IssueTracker, RateLimiter
from historical_data_setup.endpoints import income_statement as ep


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


_OK_RESPONSE = {
    "symbol": "AAPL",
    "annualReports": [
        {"fiscalDateEnding": "2024-09-30", "reportedCurrency": "USD",
         "totalRevenue": "100"},
        {"fiscalDateEnding": "2023-09-30", "reportedCurrency": "USD",
         "totalRevenue": "90"},
    ],
    "quarterlyReports": [
        {"fiscalDateEnding": "2024-09-30", "reportedCurrency": "USD",
         "totalRevenue": "26"},
    ],
}


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


def test_writes_annual_and_quarterly_per_symbol(tmp_path, fast_limiter):
    """Two parquets per symbol: ``stocks_AAPL_annual.parquet`` and
    ``stocks_AAPL_quarterly.parquet``. Schema includes a Date
    fiscalDateEnding and Float32 totalRevenue."""
    catalog = tmp_path / "catalog"
    historical = tmp_path / "historical"
    _make_catalog(catalog, ["AAPL"])

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        return _OK_RESPONSE

    tracker = IssueTracker()
    with patch.object(hc, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_income_statement(
            catalog_dir=catalog, historical_dir=historical, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks",
        ))

    out = historical / "stocks" / "income_statement"
    assert (out / "stocks_AAPL_annual.parquet").exists()
    assert (out / "stocks_AAPL_quarterly.parquet").exists()
    annual = pl.read_parquet(out / "stocks_AAPL_annual.parquet")
    assert annual.height == 2
    assert annual.schema["fiscalDateEnding"] == pl.Date
    assert annual.schema["totalRevenue"] == pl.Float32
    # Sorted ascending by fiscalDateEnding.
    assert annual["totalRevenue"].to_list() == [90.0, 100.0]


def test_skip_when_both_files_exist(tmp_path, fast_limiter):
    """If BOTH per-symbol parquets exist, the symbol is skipped and no fetch
    is issued. (Resume guarantee for partial historical setups.)"""
    catalog = tmp_path / "catalog"
    historical = tmp_path / "historical"
    _make_catalog(catalog, ["AAPL"])
    out = historical / "stocks" / "income_statement"
    out.mkdir(parents=True)
    (out / "stocks_AAPL_annual.parquet").write_bytes(b"a")
    (out / "stocks_AAPL_quarterly.parquet").write_bytes(b"q")

    fetch_calls: list[str] = []

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        fetch_calls.append(url)
        return _OK_RESPONSE

    tracker = IssueTracker()
    with patch.object(hc, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_income_statement(
            catalog_dir=catalog, historical_dir=historical, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks",
        ))

    assert fetch_calls == []
    assert (out / "stocks_AAPL_annual.parquet").read_bytes() == b"a"


# ---------------------------------------------------------------------------
# Issue paths
# ---------------------------------------------------------------------------


def test_missing_top_level_keys_records_structure_error(tmp_path, fast_limiter):
    """Response missing ``annualReports`` or ``quarterlyReports`` -> structure
    error, no parquet written."""
    catalog = tmp_path / "catalog"
    historical = tmp_path / "historical"
    _make_catalog(catalog, ["AAPL"])

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        return {"symbol": "AAPL", "annualReports": []}  # missing quarterlyReports

    tracker = IssueTracker()
    with patch.object(hc, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_income_statement(
            catalog_dir=catalog, historical_dir=historical, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks",
        ))

    issues = [r for r in tracker._rows if r["issue_type"] == "structure_error"]
    assert len(issues) == 1
    assert "quarterlyReports" in issues[0]["detail"]
    assert not any((historical / "stocks" / "income_statement").rglob("*.parquet"))


def test_empty_reports_record_empty_content_per_period(tmp_path, fast_limiter):
    """Empty ``annualReports`` and ``quarterlyReports`` each get their own
    ``empty_content`` issue. No parquet written for either period."""
    catalog = tmp_path / "catalog"
    historical = tmp_path / "historical"
    _make_catalog(catalog, ["AAPL"])

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        return {"symbol": "AAPL", "annualReports": [], "quarterlyReports": []}

    tracker = IssueTracker()
    with patch.object(hc, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_income_statement(
            catalog_dir=catalog, historical_dir=historical, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks",
        ))

    empties = [r for r in tracker._rows if r["issue_type"] == "empty_content"]
    assert len(empties) == 2
    assert {r["detail"] for r in empties} == {
        "empty annualReports", "empty quarterlyReports",
    }


def test_av_throttle_continues_to_next_symbol(tmp_path, fast_limiter):
    """An ``AVResponseError`` records ``av_throttle`` for that symbol but
    does not abort the loop -- the next symbol must still be processed."""
    catalog = tmp_path / "catalog"
    historical = tmp_path / "historical"
    _make_catalog(catalog, ["AAPL", "MSFT"])

    from historical_data_setup._common import AVResponseError

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        if "AAPL" in url:
            raise AVResponseError("rate limited")
        return {**_OK_RESPONSE, "symbol": "MSFT"}

    tracker = IssueTracker()
    with patch.object(hc, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_income_statement(
            catalog_dir=catalog, historical_dir=historical, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks",
        ))

    out = historical / "stocks" / "income_statement"
    assert not (out / "stocks_AAPL_annual.parquet").exists()
    assert (out / "stocks_MSFT_annual.parquet").exists()
    throttles = [r for r in tracker._rows if r["issue_type"] == "av_throttle"]
    assert [r["symbol"] for r in throttles] == ["AAPL"]
