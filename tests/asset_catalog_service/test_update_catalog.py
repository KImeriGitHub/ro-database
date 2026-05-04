"""Smoke tests for ``asset_catalog_service.update_catalog.update_all``.

The orchestrator is much smaller than ``init_all``: no FRD validation, no
``init_stocks_etfs`` (it's ``update_stocks_etfs`` instead). We assert step
ordering and exception isolation.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from asset_catalog_service import update_catalog as uc


EXPECTED_ORDER = [
    "update_stocks_etfs",
    "update_indices",
    "update_forex",
    "update_cryptocurrencies",
    "update_commodities",
    "update_economic",
    "update_yield_status",
    "update_earnings_calendar",
]


def test_update_all_runs_every_step_in_order(tmp_path):
    call_log: list[str] = []

    def rec(name):
        def _f(*a, **kw): call_log.append(name)
        return _f

    with patch.object(uc, "update_stocks_etfs", side_effect=rec("update_stocks_etfs")), \
         patch.object(uc, "update_indices", side_effect=rec("update_indices")), \
         patch.object(uc, "update_forex", side_effect=rec("update_forex")), \
         patch.object(uc, "update_cryptocurrencies", side_effect=rec("update_cryptocurrencies")), \
         patch.object(uc, "update_commodities", side_effect=rec("update_commodities")), \
         patch.object(uc, "update_economic", side_effect=rec("update_economic")), \
         patch.object(uc, "update_yield_status", side_effect=rec("update_yield_status")), \
         patch.object(uc, "update_earnings_calendar", side_effect=rec("update_earnings_calendar")), \
         patch.object(uc, "get_alpha_vantage_key", return_value="fake-key"):
        uc.update_all(catalog_dir=tmp_path / "catalog")

    assert call_log == EXPECTED_ORDER


def test_update_all_continues_when_step_raises(tmp_path):
    """One failing step does not abort the rest of the daily catalog refresh."""
    call_log: list[str] = []

    def rec(name):
        def _f(*a, **kw): call_log.append(name)
        return _f

    def boom(*a, **kw):
        call_log.append("update_indices")
        raise RuntimeError("AV down")

    with patch.object(uc, "update_stocks_etfs", side_effect=rec("update_stocks_etfs")), \
         patch.object(uc, "update_indices", side_effect=boom), \
         patch.object(uc, "update_forex", side_effect=rec("update_forex")), \
         patch.object(uc, "update_cryptocurrencies", side_effect=rec("update_cryptocurrencies")), \
         patch.object(uc, "update_commodities", side_effect=rec("update_commodities")), \
         patch.object(uc, "update_economic", side_effect=rec("update_economic")), \
         patch.object(uc, "update_yield_status", side_effect=rec("update_yield_status")), \
         patch.object(uc, "update_earnings_calendar", side_effect=rec("update_earnings_calendar")), \
         patch.object(uc, "get_alpha_vantage_key", return_value="fake-key"):
        uc.update_all(catalog_dir=tmp_path / "catalog")

    assert call_log == EXPECTED_ORDER


def test_update_all_creates_catalog_dir(tmp_path):
    """``catalog_dir`` is created if missing, mirroring ``init_all``."""
    cat = tmp_path / "fresh"
    assert not cat.exists()
    with patch.object(uc, "update_stocks_etfs"), \
         patch.object(uc, "update_indices"), \
         patch.object(uc, "update_forex"), \
         patch.object(uc, "update_cryptocurrencies"), \
         patch.object(uc, "update_commodities"), \
         patch.object(uc, "update_economic"), \
         patch.object(uc, "update_yield_status"), \
         patch.object(uc, "update_earnings_calendar"), \
         patch.object(uc, "get_alpha_vantage_key", return_value="fake-key"):
        uc.update_all(catalog_dir=cat)
    assert cat.exists() and cat.is_dir()
