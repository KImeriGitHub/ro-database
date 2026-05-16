"""Smoke tests for ``daily_data_service.setup_daily.run_daily_pull``.

Covers the orchestrator's high-level flow without driving any real endpoint:

  - The ``previous_date >= folder_date`` no-op branch (and that the marker is
    removed).
  - The (asset_type, endpoint) plan: tasks scheduled match the
    ``ASSET_ENDPOINTS`` cross-product, ``--asset-types`` / ``--endpoints``
    subsetting works, finalize runs only on full runs.
  - ``skip_empty_yield`` reaches ``YIELD_SKIP_ENDPOINTS`` only.
  - The ingestion report is saved under ``daily/<folder-date>/``.
"""

from __future__ import annotations

import asyncio
import sys
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from daily_data_service import setup_daily as sd


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def workdir(tmp_path: Path) -> Path:
    (tmp_path / "catalog").mkdir()
    (tmp_path / "daily").mkdir()
    return tmp_path


def _write_yield_status(catalog_dir: Path, prev: date) -> None:
    """Minimal yield_status.parquet with just ``date`` and ``symbol``."""
    pl.DataFrame({
        "symbol": ["AAPL"],
        "date": [prev],
    }).write_parquet(catalog_dir / "yield_status.parquet")


def _make_recording_stub(calls: list[dict]):
    """Return a dummy endpoint coroutine that records its kwargs."""
    def _stub(**kwargs):
        calls.append({
            "asset_type": kwargs["asset_type"],
            "folder_date": kwargs["folder_date"],
            "previous_date": kwargs["previous_date"],
            "skip_empty_yield": kwargs.get("skip_empty_yield"),
        })

        async def _go():
            return None
        return _go()
    return _stub


# ---------------------------------------------------------------------------
# Folder-date computation lives in ``_common.compute_folder_date``; here we
# instead patch ``resolve_start_marker`` to inject a deterministic
# (started_at, folder_date) so the tests don't depend on system time.
# ---------------------------------------------------------------------------


def _patch_resolve_marker(daily_dir: Path, folder_date: date):
    started_at = datetime(folder_date.year, folder_date.month, folder_date.day,
                          21, 0, tzinfo=ZoneInfo("America/New_York"))
    marker = daily_dir / ".setup_started_at"
    marker.touch()
    return patch.object(sd, "resolve_start_marker",
                        return_value=(started_at, folder_date, marker))


# ---------------------------------------------------------------------------
# No-op branch
# ---------------------------------------------------------------------------


def test_run_daily_pull_noop_when_previous_ge_folder(workdir: Path):
    """``previous_date >= folder_date`` -> log and return; no plan, no
    finalize. Marker file is cleaned up so the next invocation can recompute."""
    folder_date = date(2026, 4, 17)
    daily = workdir / "daily"
    catalog = workdir / "catalog"
    _write_yield_status(catalog, folder_date)  # previous == folder
    # A prior YYYY-MM-DD subdir is required so read_previous_date takes the
    # steady-state path (reads yield_status) instead of the bootstrap fallback
    # to folder_date - PRICE_WINDOW_DAYS.
    (daily / "2026-04-14").mkdir()

    finalize_calls: list = []

    def fake_finalize(*a, **kw):
        finalize_calls.append((a, kw))

    with _patch_resolve_marker(daily, folder_date), \
         patch.object(sd, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(sd, "finalize_yield_status", side_effect=fake_finalize), \
         patch.object(sd, "ENDPOINT_MAP", {}):
        _run(sd.run_daily_pull(catalog_dir=catalog, daily_dir=daily))

    assert finalize_calls == []
    assert not (daily / ".setup_started_at").exists()
    assert not (daily / folder_date.isoformat()).exists()


# ---------------------------------------------------------------------------
# Plan composition
# ---------------------------------------------------------------------------


def test_run_daily_pull_plans_full_cross_product_on_full_run(workdir: Path):
    """A full run schedules every (asset_type, endpoint) pair from
    ``ASSET_ENDPOINTS`` -- a sanity check that the plan loop covers every
    combination."""
    folder_date = date(2026, 4, 17)
    daily = workdir / "daily"
    catalog = workdir / "catalog"
    _write_yield_status(catalog, date(2026, 4, 14))

    calls: list[dict] = []
    stub = _make_recording_stub(calls)
    endpoint_map = {ep: stub for ep in sd.ENDPOINT_MAP}

    with _patch_resolve_marker(daily, folder_date), \
         patch.object(sd, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(sd, "finalize_yield_status"), \
         patch.object(sd, "fetch_earnings_calendar"), \
         patch.object(sd, "ENDPOINT_MAP", endpoint_map):
        _run(sd.run_daily_pull(catalog_dir=catalog, daily_dir=daily))

    # Expected pair count = sum of len(applicable) for each asset_type.
    expected_pairs = sum(len(v) for v in sd.ASSET_ENDPOINTS.values())
    assert len(calls) == expected_pairs


def test_run_daily_pull_subsets_by_asset_types(workdir: Path):
    """``--asset-types stocks`` -> only stock endpoints in the plan."""
    folder_date = date(2026, 4, 17)
    daily = workdir / "daily"
    catalog = workdir / "catalog"
    _write_yield_status(catalog, date(2026, 4, 14))

    calls: list[dict] = []
    stub = _make_recording_stub(calls)
    endpoint_map = {ep: stub for ep in sd.ENDPOINT_MAP}

    with _patch_resolve_marker(daily, folder_date), \
         patch.object(sd, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(sd, "finalize_yield_status"), \
         patch.object(sd, "fetch_earnings_calendar"), \
         patch.object(sd, "ENDPOINT_MAP", endpoint_map):
        _run(sd.run_daily_pull(
            catalog_dir=catalog, daily_dir=daily,
            asset_types=["stocks"],
        ))

    asset_types = {c["asset_type"] for c in calls}
    assert asset_types == {"stocks"}
    assert len(calls) == len(sd.ASSET_ENDPOINTS["stocks"])


def test_run_daily_pull_subsets_by_endpoints(workdir: Path):
    """``--endpoints prices_daily`` cross-product against every asset type
    that lists prices_daily in ASSET_ENDPOINTS (stocks + etfs)."""
    folder_date = date(2026, 4, 17)
    daily = workdir / "daily"
    catalog = workdir / "catalog"
    _write_yield_status(catalog, date(2026, 4, 14))

    calls: list[dict] = []
    stub = _make_recording_stub(calls)
    endpoint_map = {ep: stub for ep in sd.ENDPOINT_MAP}

    with _patch_resolve_marker(daily, folder_date), \
         patch.object(sd, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(sd, "finalize_yield_status"), \
         patch.object(sd, "fetch_earnings_calendar"), \
         patch.object(sd, "ENDPOINT_MAP", endpoint_map):
        _run(sd.run_daily_pull(
            catalog_dir=catalog, daily_dir=daily,
            endpoints=["prices_daily"],
        ))

    asset_types = sorted({c["asset_type"] for c in calls})
    assert asset_types == ["etfs", "stocks"]


# ---------------------------------------------------------------------------
# Finalize: full run only
# ---------------------------------------------------------------------------


def test_finalize_yield_status_runs_only_on_full_run(workdir: Path):
    """Partial runs (subset flags set) skip ``finalize_yield_status`` so
    cells don't get prematurely flipped while only some endpoints reported."""
    folder_date = date(2026, 4, 17)
    daily = workdir / "daily"
    catalog = workdir / "catalog"
    _write_yield_status(catalog, date(2026, 4, 14))

    stub = _make_recording_stub([])
    endpoint_map = {ep: stub for ep in sd.ENDPOINT_MAP}

    with _patch_resolve_marker(daily, folder_date), \
         patch.object(sd, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(sd, "finalize_yield_status") as finalize_mock, \
         patch.object(sd, "fetch_earnings_calendar"), \
         patch.object(sd, "ENDPOINT_MAP", endpoint_map):
        _run(sd.run_daily_pull(
            catalog_dir=catalog, daily_dir=daily,
            asset_types=["stocks"],  # partial run
        ))
    finalize_mock.assert_not_called()

    with _patch_resolve_marker(daily, folder_date), \
         patch.object(sd, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(sd, "finalize_yield_status") as finalize_mock, \
         patch.object(sd, "fetch_earnings_calendar"), \
         patch.object(sd, "ENDPOINT_MAP", endpoint_map):
        _run(sd.run_daily_pull(catalog_dir=catalog, daily_dir=daily))
    finalize_mock.assert_called_once()


# ---------------------------------------------------------------------------
# skip_empty_yield only flows to YIELD_SKIP_ENDPOINTS
# ---------------------------------------------------------------------------


def test_skip_empty_yield_only_passed_to_yield_skip_endpoints(workdir: Path):
    """Non-fundamental endpoints (e.g. prices, sentiment) MUST receive no
    ``skip_empty_yield`` kwarg -- their signatures don't accept it."""
    folder_date = date(2026, 4, 17)
    daily = workdir / "daily"
    catalog = workdir / "catalog"
    _write_yield_status(catalog, date(2026, 4, 14))

    calls: list[dict] = []
    stub = _make_recording_stub(calls)
    endpoint_map = {ep: stub for ep in sd.ENDPOINT_MAP}

    with _patch_resolve_marker(daily, folder_date), \
         patch.object(sd, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(sd, "finalize_yield_status"), \
         patch.object(sd, "fetch_earnings_calendar"), \
         patch.object(sd, "ENDPOINT_MAP", endpoint_map):
        _run(sd.run_daily_pull(
            catalog_dir=catalog, daily_dir=daily,
            skip_empty_yield=True,
        ))

    yield_skip = sd.YIELD_SKIP_ENDPOINTS
    # Group calls by endpoint. We can recover endpoint by inspecting the call
    # tuple ordering -- but the stub doesn't capture endpoint, so instead
    # assert that every call with skip_empty_yield=None is to a
    # non-YIELD_SKIP_ENDPOINTS task (we ran with True), and at least
    # len(YIELD_SKIP_ENDPOINTS) calls saw True.
    saw_true = sum(1 for c in calls if c["skip_empty_yield"] is True)
    saw_none = sum(1 for c in calls if c["skip_empty_yield"] is None)
    # Every YIELD_SKIP endpoint applies only to stocks -> exactly len(yield_skip)
    # calls receive True.
    assert saw_true == len(yield_skip)
    assert saw_none == len(calls) - saw_true


# ---------------------------------------------------------------------------
# Ingestion report path
# ---------------------------------------------------------------------------


def test_ingestion_report_written_under_folder_date(workdir: Path):
    """Even with zero issues, the ingestion report path is computed under
    ``daily/<folder-date>/`` and the ``IssueTracker.save`` no-op-on-empty
    contract holds."""
    folder_date = date(2026, 4, 17)
    daily = workdir / "daily"
    catalog = workdir / "catalog"
    _write_yield_status(catalog, date(2026, 4, 14))

    stub = _make_recording_stub([])
    endpoint_map = {ep: stub for ep in sd.ENDPOINT_MAP}

    with _patch_resolve_marker(daily, folder_date), \
         patch.object(sd, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(sd, "finalize_yield_status"), \
         patch.object(sd, "fetch_earnings_calendar"), \
         patch.object(sd, "ENDPOINT_MAP", endpoint_map):
        _run(sd.run_daily_pull(catalog_dir=catalog, daily_dir=daily))

    day_root = daily / folder_date.isoformat()
    assert day_root.exists()
    # No issues -> save() returns early; report file does not exist.
    assert not (day_root / "ingestion_report.parquet").exists()


# ---------------------------------------------------------------------------
# earnings_calendar gating
# ---------------------------------------------------------------------------


def test_earnings_calendar_runs_on_full_run(workdir: Path):
    folder_date = date(2026, 4, 17)
    daily = workdir / "daily"
    catalog = workdir / "catalog"
    _write_yield_status(catalog, date(2026, 4, 14))

    stub = _make_recording_stub([])
    endpoint_map = {ep: stub for ep in sd.ENDPOINT_MAP}

    with _patch_resolve_marker(daily, folder_date), \
         patch.object(sd, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(sd, "finalize_yield_status"), \
         patch.object(sd, "fetch_earnings_calendar") as ec_mock, \
         patch.object(sd, "ENDPOINT_MAP", endpoint_map):
        _run(sd.run_daily_pull(catalog_dir=catalog, daily_dir=daily))

    day_root = daily / folder_date.isoformat()
    ec_mock.assert_called_once_with("fake-key", day_root)


def test_earnings_calendar_runs_when_named_in_endpoints(workdir: Path):
    folder_date = date(2026, 4, 17)
    daily = workdir / "daily"
    catalog = workdir / "catalog"
    _write_yield_status(catalog, date(2026, 4, 14))

    stub = _make_recording_stub([])
    endpoint_map = {ep: stub for ep in sd.ENDPOINT_MAP}

    with _patch_resolve_marker(daily, folder_date), \
         patch.object(sd, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(sd, "finalize_yield_status"), \
         patch.object(sd, "fetch_earnings_calendar") as ec_mock, \
         patch.object(sd, "ENDPOINT_MAP", endpoint_map):
        _run(sd.run_daily_pull(
            catalog_dir=catalog, daily_dir=daily,
            endpoints=["prices_daily", "earnings_calendar"],
        ))

    day_root = daily / folder_date.isoformat()
    ec_mock.assert_called_once_with("fake-key", day_root)


def test_earnings_calendar_skipped_on_partial_endpoints(workdir: Path):
    folder_date = date(2026, 4, 17)
    daily = workdir / "daily"
    catalog = workdir / "catalog"
    _write_yield_status(catalog, date(2026, 4, 14))

    stub = _make_recording_stub([])
    endpoint_map = {ep: stub for ep in sd.ENDPOINT_MAP}

    with _patch_resolve_marker(daily, folder_date), \
         patch.object(sd, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(sd, "finalize_yield_status"), \
         patch.object(sd, "fetch_earnings_calendar") as ec_mock, \
         patch.object(sd, "ENDPOINT_MAP", endpoint_map):
        _run(sd.run_daily_pull(
            catalog_dir=catalog, daily_dir=daily,
            endpoints=["prices_daily"],
        ))

    ec_mock.assert_not_called()
