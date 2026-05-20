"""Tests for ``config.gcp``.

The module reads every deployment-specific identifier from environment
variables first and from ``secrets/gcs_credentials.json`` as a local
fallback; after the public-repo refactor it ships **no** hard-coded
defaults for any of them. These tests pin that contract: missing env +
missing file yields None (not a fabricated default), present env
round-trips through, and the JSON fallback only kicks in when the env
var is unset.

Each test reloads ``config.gcp`` after patching env, then restores the
module to its default state via a teardown fixture so other tests are not
affected by leaked state. ``GCS_CREDENTIALS_FILE`` is repointed at a
scratch path so a developer's real local config never leaks into tests.
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import config.gcp as gcp_module
import config.settings as settings_module


_ENV_VARS = (
    "GCP_PROJECT_ID",
    "GCP_REGION",
    "GCS_BUCKET",
    "CLOUD_RUN_JOB_NAME",
    "SECRET_AV_KEY_STANDARD",
    "SECRET_AV_KEY_PREMIUM",
    "USE_SECRET_MANAGER_FOR_AV_KEYS",
)


@pytest.fixture
def reload_gcp(monkeypatch, tmp_path):
    """Return a reloader: caller passes env vars (and optionally a
    ``json_config`` dict) and gets the freshly-loaded ``config.gcp`` module
    back.

    Any var not passed is deleted from the environment for the duration of
    the test, so tests see a clean slate regardless of what is set on the
    developer's machine. ``GCS_CREDENTIALS_FILE`` is always redirected at a
    scratch file under ``tmp_path`` so a developer's real local config
    never leaks into tests; tests opt into JSON-fallback behaviour by
    passing ``json_config={...}``.
    """
    scratch = tmp_path / "gcs_credentials.json"
    monkeypatch.setattr(settings_module, "GCS_CREDENTIALS_FILE", scratch)

    def _reload(json_config: dict | None = None, **env: str) -> object:
        for var in _ENV_VARS:
            if var in env:
                monkeypatch.setenv(var, env[var])
            else:
                monkeypatch.delenv(var, raising=False)
        if json_config is None:
            if scratch.exists():
                scratch.unlink()
        else:
            scratch.write_text(json.dumps(json_config), encoding="utf-8")
        return importlib.reload(gcp_module)

    yield _reload

    # Restore the module's state after the test so other test files do not
    # observe the patched values.
    for var in _ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    importlib.reload(gcp_module)


def test_all_identifiers_none_when_env_unset(reload_gcp):
    """The public-repo guarantee: nothing leaks a real deployment's
    identifiers as a default. Missing env + missing JSON => ``None``."""
    gcp = reload_gcp()
    assert gcp.GCP_PROJECT_ID is None
    assert gcp.GCP_REGION is None
    assert gcp.GCS_BUCKET is None
    assert gcp.CLOUD_RUN_JOB_NAME is None
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


def test_identifiers_populated_from_json_when_env_unset(reload_gcp):
    """When env vars are not set, ``secrets/gcs_credentials.json`` provides
    the fallback so local devs do not have to export env vars in every
    shell."""
    gcp = reload_gcp(json_config={
        "project_id": "json-project",
        "gcp_region": "europe-west1",
        "gcs_bucket": "json-bucket",
        "cloud_run_job_name": "json-job",
        "secret_av_key_standard": "json-av-std",
        "secret_av_key_premium": "json-av-prem",
    })
    assert gcp.GCP_PROJECT_ID == "json-project"
    assert gcp.GCP_REGION == "europe-west1"
    assert gcp.GCS_BUCKET == "json-bucket"
    assert gcp.CLOUD_RUN_JOB_NAME == "json-job"
    assert gcp.SECRET_AV_KEY_STANDARD == "json-av-std"
    assert gcp.SECRET_AV_KEY_PREMIUM == "json-av-prem"


def test_env_wins_over_json(reload_gcp):
    """Env vars must take precedence over the JSON fallback so Cloud Run
    deployments are never silently overridden by a stray local file."""
    gcp = reload_gcp(
        GCP_PROJECT_ID="env-project",
        GCS_BUCKET="env-bucket",
        json_config={
            "project_id": "json-project",
            "gcs_bucket": "json-bucket",
            "secret_av_key_premium": "json-av-prem",
        },
    )
    assert gcp.GCP_PROJECT_ID == "env-project"
    assert gcp.GCS_BUCKET == "env-bucket"
    # Keys not overridden by env still come from the JSON file.
    assert gcp.SECRET_AV_KEY_PREMIUM == "json-av-prem"


def test_malformed_json_is_ignored(reload_gcp, tmp_path, monkeypatch):
    """A broken secrets file must not crash import: ``config.gcp`` is loaded
    on every CLI entrypoint, so a parse error there would brick the whole
    project. The module logs a warning and behaves as if the file were
    absent."""
    scratch = tmp_path / "broken.json"
    scratch.write_text("{ not valid json", encoding="utf-8")
    monkeypatch.setattr(settings_module, "GCS_CREDENTIALS_FILE", scratch)
    for var in _ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    gcp = importlib.reload(gcp_module)
    assert gcp.GCP_PROJECT_ID is None
    assert gcp.GCS_BUCKET is None


def test_data_layout_prefixes_are_fixed():
    """The catalog/historical/daily prefixes are part of the published
    schema (SPEC.md) and must NOT be env-driven -- they describe the bucket
    layout, not deployment identity."""
    assert gcp_module.GCS_CATALOG_PREFIX == "catalog"
    assert gcp_module.GCS_HISTORICAL_PREFIX == "historical"
    assert gcp_module.GCS_DAILY_PREFIX == "daily"


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
