"""Tests for ``config.settings``.

The module is just paths and a couple of rate-limit constants, but the path
contract is load-bearing for every script in the repo: PROJECT_ROOT must
resolve to the repo top, the data trees must hang off it, and the
Alpha-Vantage budget must stay below the hard cap. A regression here
silently breaks every entrypoint, so the contract gets pinned.

Pure unit tests. No filesystem writes, no network.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config import settings


def test_project_root_is_repo_top():
    """PROJECT_ROOT must point at the directory holding SPEC.md / CLAUDE.md.
    The module derives it from ``__file__`` and the rest of the codebase
    builds paths off it; a wrong root makes every downstream path bogus."""
    root = settings.PROJECT_ROOT
    assert (root / "SPEC.md").is_file(), \
        f"PROJECT_ROOT={root} does not look like the repo root (no SPEC.md)"
    assert (root / "CLAUDE.md").is_file()
    assert (root / "config" / "settings.py").is_file()


def test_project_root_is_absolute():
    """Relative roots break scripts run from arbitrary cwds (e.g. inside
    Cloud Run where WORKDIR is /app)."""
    assert settings.PROJECT_ROOT.is_absolute()


def test_secrets_paths_live_under_secrets_dir():
    """API keys and GCS creds must not leak outside ``secrets/`` -- that
    directory is the only one in ``.gitignore`` for credentials."""
    assert settings.SECRETS_DIR == settings.PROJECT_ROOT / "secrets"
    assert settings.ALPHA_VANTAGE_KEYS_FILE.parent == settings.SECRETS_DIR
    assert settings.GCS_CREDENTIALS_FILE.parent == settings.SECRETS_DIR


def test_rate_limit_under_hard_cap():
    """The sliding-window limiter must leave headroom below AV's hard cap or
    the catalog-side sweeps and the daily run will collide and 429."""
    assert settings.AV_RATE_LIMIT_PER_MIN < settings.AV_HARD_CAP_PER_MIN
    assert settings.AV_HARD_CAP_PER_MIN == 75  # AV's documented premium cap


def test_indices_disabled():
    """INDEX_DATA is gated behind AV's 150+ requests/min plans. Until the plan
    changes, indices must stay disabled -- see the propagation test below for
    what that switches off."""
    assert "indices" in settings.DISABLED_ASSET_TYPES


def test_disabled_asset_types_reach_every_ingestion_registry():
    """The flag is only useful if every plan-building registry honours it. A
    registry that keeps a disabled asset type would silently resume calling a
    gated endpoint for every symbol in its catalog."""
    from asset_catalog_service.updates.yield_status import ASSET_TYPE_COLUMNS
    from daily_data_service.ensure_folders import DAILY_TREE
    from daily_data_service.setup_daily import ASSET_ENDPOINTS as DAILY_ASSETS
    from daily_data_service.setup_daily import ENDPOINT_MAP as DAILY_ENDPOINTS
    from historical_data_setup.ensure_folders import HISTORICAL_TREE
    from historical_data_setup.setup_historical import ASSET_ENDPOINTS as HIST_ASSETS
    from historical_data_setup.setup_historical import ENDPOINT_MAP as HIST_ENDPOINTS
    from monitoring_service.analyze_files import _ASSET_ENDPOINTS as MONITOR_ASSETS

    for disabled in settings.DISABLED_ASSET_TYPES:
        assert disabled not in DAILY_ASSETS
        assert disabled not in HIST_ASSETS
        assert disabled not in DAILY_ENDPOINTS
        assert disabled not in HIST_ENDPOINTS
        assert disabled not in ASSET_TYPE_COLUMNS
        assert disabled not in MONITOR_ASSETS
        assert not [l for l in DAILY_TREE if l.split("/")[0] == disabled]
        assert not [l for l in HISTORICAL_TREE if l.split("/")[0] == disabled]

    # Endpoints shared with a still-enabled asset type must survive the filter.
    assert "prices" in DAILY_ENDPOINTS and "prices" in HIST_ENDPOINTS
