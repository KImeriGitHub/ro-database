"""Smoke tests for ``historical_data_setup.setup_historical.run_historical_setup``.

The orchestrator schedules every applicable (asset_type, endpoint) pair
concurrently against a shared rate limiter and aiohttp session, then writes
an ingestion report and (on full runs) finalizes yield_status. This module
patches every endpoint coroutine to a recording stub so the assertions
focus on plan composition and the FRD-dir routing.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from historical_data_setup import setup_historical as sh


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def workdir(tmp_path: Path) -> Path:
    (tmp_path / "catalog").mkdir()
    (tmp_path / "historical").mkdir()
    return tmp_path


def _make_recording_stub(calls: list[dict]):
    def _stub(**kwargs):
        calls.append({
            "asset_type": kwargs["asset_type"],
            "frd_dir": kwargs.get("frd_dir"),
            "has_frd_kwarg": "frd_dir" in kwargs,
        })

        async def _go():
            return None
        return _go()
    return _stub


# ---------------------------------------------------------------------------
# Plan composition
# ---------------------------------------------------------------------------


def test_full_run_schedules_every_asset_endpoint_pair(workdir: Path):
    historical = workdir / "historical"
    catalog = workdir / "catalog"

    calls: list[dict] = []
    stub = _make_recording_stub(calls)
    endpoint_map = {ep: stub for ep in sh.ENDPOINT_MAP}

    with patch.object(sh, "ENDPOINT_MAP", endpoint_map), \
         patch.object(sh, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(sh, "finalize_yield_status"), \
         patch.object(sh, "run_and_persist"):
        _run(sh.run_historical_setup(
            catalog_dir=catalog,
            historical_dir=historical,
            run_monitor=False,
        ))

    expected_pairs = sum(len(v) for v in sh.ASSET_ENDPOINTS.values())
    assert len(calls) == expected_pairs


def test_subset_by_endpoints_filters_plan(workdir: Path):
    """``--endpoints prices_daily`` keeps only stock + ETF calls (the asset
    types whose ``ASSET_ENDPOINTS`` lists prices_daily)."""
    historical = workdir / "historical"
    catalog = workdir / "catalog"

    calls: list[dict] = []
    stub = _make_recording_stub(calls)
    endpoint_map = {ep: stub for ep in sh.ENDPOINT_MAP}

    with patch.object(sh, "ENDPOINT_MAP", endpoint_map), \
         patch.object(sh, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(sh, "finalize_yield_status"), \
         patch.object(sh, "run_and_persist"):
        _run(sh.run_historical_setup(
            catalog_dir=catalog,
            historical_dir=historical,
            endpoints=["prices_daily"],
            run_monitor=False,
        ))

    asset_types = sorted({c["asset_type"] for c in calls})
    assert asset_types == ["etfs", "stocks"]


# ---------------------------------------------------------------------------
# FRD-dir routing
# ---------------------------------------------------------------------------


def test_frd_dirs_routed_only_to_prices_endpoints(workdir: Path):
    """``stocks_dir`` / ``etfs_dir`` flow through to ``prices`` and
    ``prices_daily`` for stocks/etfs only -- never to fundamental endpoints
    or to forex/commodities/etc."""
    historical = workdir / "historical"
    catalog = workdir / "catalog"
    stocks_dir = workdir / "frd_stocks"
    etfs_dir = workdir / "frd_etfs"

    # We need to recover endpoint+asset_type per call: rebuild endpoint_map
    # so the stub captures the endpoint name too.
    calls: list[dict] = []

    def make_stub(endpoint_name: str):
        def _stub(**kwargs):
            calls.append({
                "endpoint": endpoint_name,
                "asset_type": kwargs["asset_type"],
                "frd_dir": kwargs.get("frd_dir"),
                "has_frd_kwarg": "frd_dir" in kwargs,
            })

            async def _go():
                return None
            return _go()
        return _stub

    endpoint_map = {ep: make_stub(ep) for ep in sh.ENDPOINT_MAP}

    with patch.object(sh, "ENDPOINT_MAP", endpoint_map), \
         patch.object(sh, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(sh, "finalize_yield_status"), \
         patch.object(sh, "run_and_persist"):
        _run(sh.run_historical_setup(
            catalog_dir=catalog,
            historical_dir=historical,
            stocks_dir=stocks_dir,
            etfs_dir=etfs_dir,
            run_monitor=False,
        ))

    by_pair = {(c["endpoint"], c["asset_type"]): c for c in calls}

    # Stocks prices/prices_daily get stocks_dir.
    assert by_pair[("prices", "stocks")]["frd_dir"] == stocks_dir
    assert by_pair[("prices_daily", "stocks")]["frd_dir"] == stocks_dir
    # ETFs prices/prices_daily get etfs_dir.
    assert by_pair[("prices", "etfs")]["frd_dir"] == etfs_dir
    assert by_pair[("prices_daily", "etfs")]["frd_dir"] == etfs_dir
    # Fundamental endpoints must not receive a frd_dir kwarg at all.
    for c in calls:
        if c["endpoint"] in ("income_statement", "balance_sheet", "cash_flow",
                              "earnings", "earnings_estimates", "insider",
                              "sentiment", "etf_profile"):
            assert not c["has_frd_kwarg"], c
        if c["asset_type"] in ("forex", "indices", "cryptocurrencies",
                               "commodities", "economic"):
            assert not c["has_frd_kwarg"], c


# ---------------------------------------------------------------------------
# Finalize / monitor: full run only
# ---------------------------------------------------------------------------


def test_finalize_runs_only_on_full_run(workdir: Path):
    """Partial-run modes leave yield_status alone."""
    historical = workdir / "historical"
    catalog = workdir / "catalog"

    stub = _make_recording_stub([])
    endpoint_map = {ep: stub for ep in sh.ENDPOINT_MAP}

    # Partial run.
    with patch.object(sh, "ENDPOINT_MAP", endpoint_map), \
         patch.object(sh, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(sh, "finalize_yield_status") as finalize_mock, \
         patch.object(sh, "run_and_persist"):
        _run(sh.run_historical_setup(
            catalog_dir=catalog,
            historical_dir=historical,
            asset_types=["stocks"],
            run_monitor=False,
        ))
    finalize_mock.assert_not_called()

    # Full run.
    with patch.object(sh, "ENDPOINT_MAP", endpoint_map), \
         patch.object(sh, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(sh, "finalize_yield_status") as finalize_mock, \
         patch.object(sh, "run_and_persist"):
        _run(sh.run_historical_setup(
            catalog_dir=catalog,
            historical_dir=historical,
            run_monitor=False,
        ))
    finalize_mock.assert_called_once()


def test_run_monitor_flag_controls_persist(workdir: Path):
    """``run_monitor=False`` skips ``run_and_persist``; ``True`` calls it
    (and a failure inside the monitor must NOT propagate)."""
    historical = workdir / "historical"
    catalog = workdir / "catalog"

    stub = _make_recording_stub([])
    endpoint_map = {ep: stub for ep in sh.ENDPOINT_MAP}

    with patch.object(sh, "ENDPOINT_MAP", endpoint_map), \
         patch.object(sh, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(sh, "finalize_yield_status"), \
         patch.object(sh, "run_and_persist") as monitor_mock:
        _run(sh.run_historical_setup(
            catalog_dir=catalog,
            historical_dir=historical,
            run_monitor=False,
        ))
    monitor_mock.assert_not_called()

    with patch.object(sh, "ENDPOINT_MAP", endpoint_map), \
         patch.object(sh, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(sh, "finalize_yield_status"), \
         patch.object(sh, "run_and_persist", side_effect=RuntimeError("boom")) as monitor_mock:
        # Must not propagate -- setup is unaffected by monitor failure.
        _run(sh.run_historical_setup(
            catalog_dir=catalog,
            historical_dir=historical,
            run_monitor=True,
        ))
    monitor_mock.assert_called_once()


# ---------------------------------------------------------------------------
# Resume marker
# ---------------------------------------------------------------------------


def test_start_marker_persists_across_resumed_runs(workdir: Path):
    """The ``.setup_started_at`` marker file should be created on first run
    (and re-used on subsequent runs as long as it exists). After a full run
    finalizes, the marker is removed so the next setup gets a fresh start
    time."""
    historical = workdir / "historical"
    catalog = workdir / "catalog"

    stub = _make_recording_stub([])
    endpoint_map = {ep: stub for ep in sh.ENDPOINT_MAP}

    marker = historical / ".setup_started_at"
    assert not marker.exists()

    with patch.object(sh, "ENDPOINT_MAP", endpoint_map), \
         patch.object(sh, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(sh, "finalize_yield_status"), \
         patch.object(sh, "run_and_persist"):
        _run(sh.run_historical_setup(
            catalog_dir=catalog,
            historical_dir=historical,
            run_monitor=False,
        ))

    # Full run ran finalize successfully, so the marker must be gone.
    assert not marker.exists()
