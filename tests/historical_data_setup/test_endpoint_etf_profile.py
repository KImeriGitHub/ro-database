"""Unit tests for ``historical_data_setup.endpoints.etf_profile``.

Covers the sector pivot (canonical names, unmapped sectors aggregated into
``other``), holdings filtering (null sentinels dropped), scalar field casting
(``inception_date``/``leveraged`` stay String, others go Float32), the
non-ETF early-return guard, and the structural / throttle / cast-failure
issue paths.
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
from historical_data_setup.endpoints import etf_profile as ep
from historical_data_setup.endpoints.etf_profile import SECTOR_COLUMNS


def _run(coro):
    return asyncio.run(coro)


def _make_etf_catalog(catalog_dir: Path, symbols: list[str]) -> None:
    catalog_dir.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame({
        "symbol": symbols,
        "name": symbols,
        "ipoDate": [None] * len(symbols),
        "delistingDate": [None] * len(symbols),
        "status": ["Active"] * len(symbols),
    }).cast({"ipoDate": pl.Date, "delistingDate": pl.Date, "status": pl.Utf8})
    df.write_parquet(catalog_dir / "etfs.parquet", compression="zstd")


_VALID_RESPONSE = {
    "net_assets": "100000000",
    "net_expense_ratio": "0.0009",
    "portfolio_turnover": "0.05",
    "dividend_yield": "0.013",
    "inception_date": "2010-01-01",
    "leveraged": "NO",
    "sectors": [
        {"sector": "INFORMATION TECHNOLOGY", "weight": "0.30"},
        {"sector": "HEALTHCARE", "weight": "0.20"},
        {"sector": "MUNICIPAL BONDS", "weight": "0.05"},  # unmapped -> other
        {"sector": "GOVERNMENT BONDS", "weight": "0.04"},  # unmapped -> other
    ],
    "holdings": [
        {"symbol": "AAPL", "weight": "0.07"},
        {"symbol": "None", "weight": "0.01"},  # sentinel dropped
        {"symbol": "MSFT", "weight": "0.06"},
    ],
}


@pytest.fixture
def fast_limiter():
    return RateLimiter(calls_per_minute=10000.0, window=1.0, min_gap=0.0)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_build_sector_values_pivots_known_sectors_into_canonical_columns():
    """INFORMATION TECHNOLOGY -> information_technology, etc. Mapping is
    case-sensitive against the AV upper-case names; weights are floats."""
    tracker = IssueTracker()
    out = ep._build_sector_values(
        [
            {"sector": "INFORMATION TECHNOLOGY", "weight": "0.3"},
            {"sector": "FINANCIALS", "weight": "0.1"},
        ],
        "SPY", "etfs", tracker,
    )
    assert out["information_technology"] == pytest.approx(0.3)
    assert out["financials"] == pytest.approx(0.1)
    # Unmentioned sectors stay None.
    assert out["healthcare"] is None
    assert out["other"] is None
    assert tracker.count == 0


def test_build_sector_values_aggregates_unmapped_into_other():
    """Anything not in ``_SECTOR_MAP`` is summed into the ``other`` bucket."""
    tracker = IssueTracker()
    out = ep._build_sector_values(
        [
            {"sector": "MUNICIPAL BONDS", "weight": "0.04"},
            {"sector": "GOVERNMENT BONDS", "weight": "0.06"},
        ],
        "AGG", "etfs", tracker,
    )
    assert out["other"] == pytest.approx(0.10)


def test_build_sector_values_records_cast_failure_for_bad_weight():
    """A non-numeric weight records a cast_failure issue and yields None."""
    tracker = IssueTracker()
    out = ep._build_sector_values(
        [{"sector": "ENERGY", "weight": "not-a-number"}],
        "XLE", "etfs", tracker,
    )
    assert out["energy"] is None
    assert any(r["issue_type"] == "cast_failure" for r in tracker._rows)


def test_build_holdings_drops_null_sentinel_symbols():
    """Holdings with symbol in {None, "None", "", "."} are filtered out."""
    tracker = IssueTracker()
    out = ep._build_holdings(
        [
            {"symbol": "AAPL", "weight": "0.07"},
            {"symbol": "None", "weight": "0.01"},
            {"symbol": "", "weight": "0.005"},
            {"symbol": "MSFT", "weight": "0.06"},
        ],
        "QQQ", "etfs", tracker,
    )
    assert out is not None
    syms = [h["symbol"] for h in out]
    assert syms == ["AAPL", "MSFT"]


def test_build_holdings_returns_none_when_all_filtered():
    """If every holding is filtered out, return None so the caller can write
    a typed null into the column rather than an empty list."""
    tracker = IssueTracker()
    out = ep._build_holdings(
        [{"symbol": "None", "weight": "1.0"}],
        "EMPTY", "etfs", tracker,
    )
    assert out is None


# ---------------------------------------------------------------------------
# fetch_etf_profile end-to-end
# ---------------------------------------------------------------------------


def test_fetch_etf_profile_writes_single_row_with_full_schema(tmp_path, fast_limiter):
    """One row per symbol with date + all sector columns + holdings list +
    scalar fields. Holdings is a List(Struct{symbol, weight})."""
    catalog = tmp_path / "catalog"
    historical = tmp_path / "historical"
    _make_etf_catalog(catalog, ["SPY"])

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        return _VALID_RESPONSE

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_etf_profile(
            catalog_dir=catalog, historical_dir=historical, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="etfs",
        ))

    out = historical / "etfs" / "etf_profile" / "etfs_SPY.parquet"
    assert out.exists()
    df = pl.read_parquet(out)
    assert df.height == 1
    # Schema
    assert df.schema["date"] == pl.Date
    for col in SECTOR_COLUMNS:
        assert df.schema[col] == pl.Float32, col
    assert df.schema["inception_date"] == pl.Utf8
    assert df.schema["leveraged"] == pl.Utf8
    assert df.schema["net_assets"] == pl.Float32
    # Sector pivot
    row = df.row(0, named=True)
    assert row["information_technology"] == pytest.approx(0.30, rel=1e-3)
    assert row["healthcare"] == pytest.approx(0.20, rel=1e-3)
    assert row["other"] == pytest.approx(0.05 + 0.04, rel=1e-3)
    # Holdings
    holdings = row["holdings"]
    assert {h["symbol"] for h in holdings} == {"AAPL", "MSFT"}
    # Scalars
    assert row["inception_date"] == "2010-01-01"
    assert row["leveraged"] == "NO"


def test_fetch_etf_profile_returns_immediately_for_non_etfs(tmp_path, fast_limiter):
    """The endpoint guards against being invoked with asset_type != 'etfs'.
    No catalog read, no fetch, no output dir."""
    catalog = tmp_path / "catalog"
    historical = tmp_path / "historical"

    fetch_calls: list[str] = []

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        fetch_calls.append(url)
        return _VALID_RESPONSE

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_etf_profile(
            catalog_dir=catalog, historical_dir=historical, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks",
        ))

    assert fetch_calls == []
    assert not (historical / "etfs").exists()


def test_fetch_etf_profile_missing_required_keys_records_structure_error(
    tmp_path, fast_limiter,
):
    """Any subset of REQUIRED_KEYS missing from the response is structural."""
    catalog = tmp_path / "catalog"
    historical = tmp_path / "historical"
    _make_etf_catalog(catalog, ["SPY"])

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        # Missing 'holdings' and 'sectors'
        return {
            "net_assets": "1", "net_expense_ratio": "1",
            "portfolio_turnover": "1", "dividend_yield": "1",
            "inception_date": "2000-01-01", "leveraged": "NO",
        }

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_etf_profile(
            catalog_dir=catalog, historical_dir=historical, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="etfs",
        ))

    assert not (historical / "etfs" / "etf_profile" / "etfs_SPY.parquet").exists()
    issues = [r for r in tracker._rows if r["issue_type"] == "structure_error"]
    assert len(issues) == 1
    assert "holdings" in issues[0]["detail"] or "sectors" in issues[0]["detail"]


def test_fetch_etf_profile_av_throttle_records_issue(tmp_path, fast_limiter):
    """AVResponseError -> ``av_throttle`` issue, no parquet written."""
    catalog = tmp_path / "catalog"
    historical = tmp_path / "historical"
    _make_etf_catalog(catalog, ["SPY"])

    from historical_data_setup._common import AVResponseError

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        raise AVResponseError("rate limited")

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_etf_profile(
            catalog_dir=catalog, historical_dir=historical, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="etfs",
        ))

    assert not (historical / "etfs" / "etf_profile" / "etfs_SPY.parquet").exists()
    assert any(r["issue_type"] == "av_throttle" for r in tracker._rows)


def test_fetch_etf_profile_skip_existing_file(tmp_path, fast_limiter):
    """Existing per-symbol parquet -> no fetch, no overwrite."""
    catalog = tmp_path / "catalog"
    historical = tmp_path / "historical"
    _make_etf_catalog(catalog, ["SPY"])
    out = historical / "etfs" / "etf_profile" / "etfs_SPY.parquet"
    out.parent.mkdir(parents=True)
    out.write_bytes(b"sentinel")

    fetch_calls: list[str] = []

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        fetch_calls.append(url)
        return _VALID_RESPONSE

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_etf_profile(
            catalog_dir=catalog, historical_dir=historical, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="etfs",
        ))

    assert fetch_calls == []
    assert out.read_bytes() == b"sentinel"
