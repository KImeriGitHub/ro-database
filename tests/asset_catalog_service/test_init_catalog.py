"""Smoke tests for ``asset_catalog_service.init_catalog.init_all``.

Each catalog-update step is patched to a recording stub so we can assert on
the orchestrator's behaviour itself: step ordering, exception isolation
(one failing step does not abort the rest), and FRD validation running
before any update step.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from asset_catalog_service import init_catalog as ic


# Step names appear in this exact order in init_all (FRD validation aside).
EXPECTED_ORDER = [
    "init_stocks_etfs",
    "update_indices",
    "update_forex",
    "update_cryptocurrencies",
    "update_commodities",
    "update_economic",
    "update_yield_status",
    "update_earnings_calendar",
]


def _patch_all_steps(call_log: list[str]):
    """Return a contextmanager-like list of patches that record into ``call_log``."""

    def make_recorder(name: str):
        def _rec(*args, **kwargs):
            call_log.append(name)
        return _rec

    return [
        patch.object(ic, "validate_firstrate_csvs", side_effect=make_recorder("validate_firstrate_csvs")),
        patch.object(ic, "init_stocks_etfs", side_effect=make_recorder("init_stocks_etfs")),
        patch.object(ic, "update_indices", side_effect=make_recorder("update_indices")),
        patch.object(ic, "update_forex", side_effect=make_recorder("update_forex")),
        patch.object(ic, "update_cryptocurrencies", side_effect=make_recorder("update_cryptocurrencies")),
        patch.object(ic, "update_commodities", side_effect=make_recorder("update_commodities")),
        patch.object(ic, "update_economic", side_effect=make_recorder("update_economic")),
        patch.object(ic, "update_yield_status", side_effect=make_recorder("update_yield_status")),
        patch.object(ic, "update_earnings_calendar", side_effect=make_recorder("update_earnings_calendar")),
        patch.object(ic, "get_alpha_vantage_key", return_value="fake-key"),
    ]


def test_init_all_runs_every_step_in_order(tmp_path):
    """Validation runs first, then each update step in the documented order."""
    call_log: list[str] = []
    patches = _patch_all_steps(call_log)
    for p in patches:
        p.start()
    try:
        ic.init_all(catalog_dir=tmp_path / "catalog")
    finally:
        for p in reversed(patches):
            p.stop()

    assert call_log[0] == "validate_firstrate_csvs"
    assert call_log[1:] == EXPECTED_ORDER


def test_init_all_creates_catalog_dir(tmp_path):
    """``catalog_dir`` is created if it does not already exist (the orchestrator
    must not assume the caller pre-created it)."""
    call_log: list[str] = []
    patches = _patch_all_steps(call_log)
    for p in patches:
        p.start()
    try:
        cat_dir = tmp_path / "fresh_catalog"
        assert not cat_dir.exists()
        ic.init_all(catalog_dir=cat_dir)
        assert cat_dir.exists() and cat_dir.is_dir()
    finally:
        for p in reversed(patches):
            p.stop()


def test_init_all_continues_when_one_step_raises(tmp_path):
    """A failing step is logged but the loop must keep running -- partial
    progress is preferable to bailing out and re-running a long catalog
    init from scratch."""
    call_log: list[str] = []

    def make_rec(name):
        def _f(*a, **kw): call_log.append(name)
        return _f

    def boom(*a, **kw):
        call_log.append("update_forex")
        raise RuntimeError("AV down")

    with patch.object(ic, "validate_firstrate_csvs", side_effect=make_rec("validate_firstrate_csvs")), \
         patch.object(ic, "init_stocks_etfs", side_effect=make_rec("init_stocks_etfs")), \
         patch.object(ic, "update_indices", side_effect=make_rec("update_indices")), \
         patch.object(ic, "update_forex", side_effect=boom), \
         patch.object(ic, "update_cryptocurrencies", side_effect=make_rec("update_cryptocurrencies")), \
         patch.object(ic, "update_commodities", side_effect=make_rec("update_commodities")), \
         patch.object(ic, "update_economic", side_effect=make_rec("update_economic")), \
         patch.object(ic, "update_yield_status", side_effect=make_rec("update_yield_status")), \
         patch.object(ic, "update_earnings_calendar", side_effect=make_rec("update_earnings_calendar")), \
         patch.object(ic, "get_alpha_vantage_key", return_value="fake-key"):
        ic.init_all(catalog_dir=tmp_path / "catalog")

    # Every step still got its turn, including those AFTER forex.
    assert call_log[1:] == EXPECTED_ORDER


def test_init_all_propagates_firstrate_validation_error(tmp_path):
    """FRD validation runs *before* the API key is fetched; if it raises, no
    update step should run. (This is critical: validation guards against
    mismatched FRD CSVs corrupting the catalog later in the run.)"""
    call_log: list[str] = []

    def fail(*a, **kw):
        raise ValueError("missing column")

    def rec(name):
        def _f(*a, **kw): call_log.append(name)
        return _f

    with patch.object(ic, "validate_firstrate_csvs", side_effect=fail), \
         patch.object(ic, "init_stocks_etfs", side_effect=rec("init_stocks_etfs")), \
         patch.object(ic, "get_alpha_vantage_key", return_value="fake-key"):
        with pytest.raises(ValueError, match="missing column"):
            ic.init_all(
                catalog_dir=tmp_path / "catalog",
                stocks_dir=tmp_path / "frd_stocks",
            )

    assert call_log == []


def test_init_all_passes_frd_dirs_to_init_stocks_etfs(tmp_path):
    """``stocks_dir`` and ``etfs_dir`` flow through to the stocks-and-ETFs
    init step. (The other steps are AV-only.)"""
    captured: dict = {}

    def capture_init(api_key, catalog_dir, stocks_dir, etfs_dir):
        captured.update(api_key=api_key, catalog_dir=catalog_dir,
                        stocks_dir=stocks_dir, etfs_dir=etfs_dir)

    with patch.object(ic, "validate_firstrate_csvs"), \
         patch.object(ic, "init_stocks_etfs", side_effect=capture_init), \
         patch.object(ic, "update_indices"), \
         patch.object(ic, "update_forex"), \
         patch.object(ic, "update_cryptocurrencies"), \
         patch.object(ic, "update_commodities"), \
         patch.object(ic, "update_economic"), \
         patch.object(ic, "update_yield_status"), \
         patch.object(ic, "update_earnings_calendar"), \
         patch.object(ic, "get_alpha_vantage_key", return_value="fake-key"):
        ic.init_all(
            catalog_dir=tmp_path / "catalog",
            stocks_dir=tmp_path / "frd_stocks",
            etfs_dir=tmp_path / "frd_etfs",
        )

    assert captured["stocks_dir"] == tmp_path / "frd_stocks"
    assert captured["etfs_dir"] == tmp_path / "frd_etfs"
    assert captured["api_key"] == "fake-key"
