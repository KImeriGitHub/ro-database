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
