"""Tests for ``config.gcp``.

The module reads every deployment-specific identifier from environment
variables and -- after the public-repo refactor -- ships **no** defaults
for any of them. These tests pin that contract: missing env yields None
(not a fabricated default), present env round-trips through, and the
``CONTAINER_IMAGE`` derivation behaves predictably.

Each test reloads ``config.gcp`` after patching env, then restores the
module to its default state via a teardown fixture so other tests are not
affected by leaked state.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import config.gcp as gcp_module


_ENV_VARS = (
    "GCP_PROJECT_ID",
    "GCP_REGION",
    "GCS_BUCKET",
    "CLOUD_RUN_JOB_NAME",
    "CONTAINER_IMAGE",
    "SECRET_AV_KEY_STANDARD",
    "SECRET_AV_KEY_PREMIUM",
    "USE_SECRET_MANAGER_FOR_AV_KEYS",
)


@pytest.fixture
def reload_gcp(monkeypatch):
    """Return a reloader: caller passes the env vars to set, gets the
    freshly-loaded ``config.gcp`` module back.

    Any var not passed is deleted from the environment for the duration of
    the test, so tests see a clean slate regardless of what is set on the
    developer's machine.
    """
    def _reload(**env: str) -> object:
        for var in _ENV_VARS:
            if var in env:
                monkeypatch.setenv(var, env[var])
            else:
                monkeypatch.delenv(var, raising=False)
        return importlib.reload(gcp_module)

    yield _reload

    # Restore the module's state after the test so other test files do not
    # observe the patched values.
    for var in _ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    importlib.reload(gcp_module)


def test_all_identifiers_none_when_env_unset(reload_gcp):
    """The public-repo guarantee: nothing leaks a real deployment's
    identifiers as a default. Missing env => ``None``."""
    gcp = reload_gcp()
    assert gcp.GCP_PROJECT_ID is None
    assert gcp.GCP_REGION is None
    assert gcp.GCS_BUCKET is None
    assert gcp.CLOUD_RUN_JOB_NAME is None
    assert gcp.CONTAINER_IMAGE is None
    assert gcp.SECRET_AV_KEY_STANDARD is None
    assert gcp.SECRET_AV_KEY_PREMIUM is None


def test_identifiers_populated_from_env(reload_gcp):
    gcp = reload_gcp(
        GCP_PROJECT_ID="my-project",
        GCP_REGION="europe-west3",
        GCS_BUCKET="my-bucket",
        CLOUD_RUN_JOB_NAME="my-job",
        SECRET_AV_KEY_STANDARD="av-standard",
        SECRET_AV_KEY_PREMIUM="av-premium",
    )
    assert gcp.GCP_PROJECT_ID == "my-project"
    assert gcp.GCP_REGION == "europe-west3"
    assert gcp.GCS_BUCKET == "my-bucket"
    assert gcp.CLOUD_RUN_JOB_NAME == "my-job"
    assert gcp.SECRET_AV_KEY_STANDARD == "av-standard"
    assert gcp.SECRET_AV_KEY_PREMIUM == "av-premium"


def test_data_layout_prefixes_are_fixed():
    """The catalog/historical/daily prefixes are part of the published
    schema (SPEC.md) and must NOT be env-driven -- they describe the bucket
    layout, not deployment identity."""
    assert gcp_module.GCS_CATALOG_PREFIX == "catalog"
    assert gcp_module.GCS_HISTORICAL_PREFIX == "historical"
    assert gcp_module.GCS_DAILY_PREFIX == "daily"


def test_container_image_derived_from_components(reload_gcp):
    """When CONTAINER_IMAGE is unset but the three components are present,
    gcp.py composes the conventional Artifact Registry ref."""
    gcp = reload_gcp(
        GCP_PROJECT_ID="my-project",
        GCP_REGION="europe-west3",
        CLOUD_RUN_JOB_NAME="my-job",
    )
    assert gcp.CONTAINER_IMAGE == (
        "europe-west3-docker.pkg.dev/my-project/ro/my-job:latest"
    )


def test_container_image_explicit_override_wins(reload_gcp):
    """An explicit CONTAINER_IMAGE env var must not be silently overwritten
    by the derived default -- operators may point at a custom registry."""
    gcp = reload_gcp(
        GCP_PROJECT_ID="my-project",
        GCP_REGION="europe-west3",
        CLOUD_RUN_JOB_NAME="my-job",
        CONTAINER_IMAGE="custom.registry/foo:tag",
    )
    assert gcp.CONTAINER_IMAGE == "custom.registry/foo:tag"


def test_container_image_none_when_components_incomplete(reload_gcp):
    """Partial components must NOT produce a half-built ref like
    ``None-docker.pkg.dev/...``; the derivation must give up cleanly."""
    gcp = reload_gcp(
        GCP_PROJECT_ID="my-project",
        # GCP_REGION missing
        CLOUD_RUN_JOB_NAME="my-job",
    )
    assert gcp.CONTAINER_IMAGE is None


@pytest.mark.parametrize("value,expected", [
    ("true", True),
    ("True", True),
    ("TRUE", True),
    ("false", False),
    ("False", False),
    ("", False),
    ("yes", False),     # only "true" (case-insensitive) flips the flag
    ("1", False),       # same; do not silently accept numeric truthy values
])
def test_use_secret_manager_flag_parsing(reload_gcp, value, expected):
    gcp = reload_gcp(USE_SECRET_MANAGER_FOR_AV_KEYS=value)
    assert gcp.USE_SECRET_MANAGER_FOR_AV_KEYS is expected


def test_use_secret_manager_default_is_false(reload_gcp):
    """Local-dev safety: with the flag unset, the Secret Manager fallback
    is off so misconfigured local runs fail loudly instead of reaching
    silently for a Secret Manager that may not exist."""
    gcp = reload_gcp()  # no env vars
    assert gcp.USE_SECRET_MANAGER_FOR_AV_KEYS is False
