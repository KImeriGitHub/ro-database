"""Tests for ``maintainance_scripts.get_api_key.get_alpha_vantage_key``.

Covers the local-file happy path and every branch where we fall back (or
refuse to fall back) to GCP Secret Manager. The Secret Manager client is
replaced with a stub so no network or google-cloud-secret-manager install
is required to run these tests.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from maintainance_scripts import get_api_key


# Fixtures

@pytest.fixture
def tmp_keys_file(tmp_path, monkeypatch):
    """Redirect the module-level KEYS_FILE to a scratch path per test."""
    path = tmp_path / "alpha_vantage_keys"
    monkeypatch.setattr(get_api_key, "KEYS_FILE", path)
    return path


@pytest.fixture
def stub_secret_manager(monkeypatch):
    """Install a fake ``secret_manager_client`` module so the fallback can be
    exercised without installing google-cloud-secret-manager.

    Returns the dict the test can populate, mapping secret-name -> value.
    """
    secrets: dict[str, str] = {}

    fake = types.ModuleType("maintainance_scripts.secret_manager_client")

    def get_secret(secret_name, version="latest", project_id=None):
        if secret_name not in secrets:
            raise RuntimeError(f"Unexpected secret lookup: {secret_name}")
        return secrets[secret_name]

    fake.get_secret = get_secret
    monkeypatch.setitem(sys.modules, "maintainance_scripts.secret_manager_client", fake)
    return secrets


@pytest.fixture
def flag_off(monkeypatch):
    """Force USE_SECRET_MANAGER_FOR_AV_KEYS to False for the test."""
    import config.gcp
    monkeypatch.setattr(config.gcp, "USE_SECRET_MANAGER_FOR_AV_KEYS", False)


@pytest.fixture
def flag_on(monkeypatch):
    """Force USE_SECRET_MANAGER_FOR_AV_KEYS to True and set deterministic secret names."""
    import config.gcp
    monkeypatch.setattr(config.gcp, "USE_SECRET_MANAGER_FOR_AV_KEYS", True)
    monkeypatch.setattr(config.gcp, "SECRET_AV_KEY_STANDARD", "test-av-standard")
    monkeypatch.setattr(config.gcp, "SECRET_AV_KEY_PREMIUM", "test-av-premium")


# Tests

def test_reads_from_local_file(tmp_keys_file, flag_off):
    tmp_keys_file.write_text("standard=LOCAL_STD_KEY\npremium=LOCAL_PREM_KEY\n")
    assert get_api_key.get_alpha_vantage_key("standard") == "LOCAL_STD_KEY"
    assert get_api_key.get_alpha_vantage_key("premium") == "LOCAL_PREM_KEY"


def test_rejects_unknown_tier(tmp_keys_file, flag_off):
    tmp_keys_file.write_text("standard=x\n")
    with pytest.raises(ValueError, match="Unknown tier"):
        get_api_key.get_alpha_vantage_key("gold")


def test_missing_file_without_flag_raises(tmp_keys_file, flag_off):
    # tmp_keys_file fixture doesn't create the file, so it's missing.
    with pytest.raises(FileNotFoundError, match="Keys file not found"):
        get_api_key.get_alpha_vantage_key("standard")


def test_missing_tier_without_flag_raises(tmp_keys_file, flag_off):
    tmp_keys_file.write_text("standard=LOCAL_STD_KEY\n")  # no premium entry
    with pytest.raises(KeyError, match="premium"):
        get_api_key.get_alpha_vantage_key("premium")


def test_placeholder_without_flag_raises(tmp_keys_file, flag_off):
    tmp_keys_file.write_text("standard=YOUR_KEY_HERE\n")
    with pytest.raises(KeyError, match="standard"):
        get_api_key.get_alpha_vantage_key("standard")


def test_missing_file_with_flag_falls_back(tmp_keys_file, flag_on, stub_secret_manager):
    stub_secret_manager["test-av-standard"] = "FROM_SECRET_MANAGER"
    assert get_api_key.get_alpha_vantage_key("standard") == "FROM_SECRET_MANAGER"


def test_missing_tier_with_flag_falls_back(tmp_keys_file, flag_on, stub_secret_manager):
    tmp_keys_file.write_text("standard=LOCAL_STD_KEY\n")
    stub_secret_manager["test-av-premium"] = "PREM_FROM_SM"
    assert get_api_key.get_alpha_vantage_key("premium") == "PREM_FROM_SM"
    # Standard still served by the file; no SM lookup needed for it.
    assert get_api_key.get_alpha_vantage_key("standard") == "LOCAL_STD_KEY"


def test_placeholder_with_flag_falls_back(tmp_keys_file, flag_on, stub_secret_manager):
    tmp_keys_file.write_text("standard=YOUR_KEY_HERE\npremium=REAL_PREM\n")
    stub_secret_manager["test-av-standard"] = "STD_FROM_SM"
    assert get_api_key.get_alpha_vantage_key("standard") == "STD_FROM_SM"
    assert get_api_key.get_alpha_vantage_key("premium") == "REAL_PREM"
