"""Tests for ``maintainance_scripts.gcp_credentials.get_gcp_credentials``.

The resolver picks one of three branches:

1. Cloud Run (``K_SERVICE`` env present on Services, ``CLOUD_RUN_JOB`` on
   Jobs -- either flips ``detect_cloud_run`` to True) -> return ``None`` so
   the client library uses Application Default Credentials.
2. Local with ``GOOGLE_APPLICATION_CREDENTIALS`` set -> load that file.
3. Local without the override -> load ``secrets/gcs_credentials.json``.

Missing files raise loudly. ``service_account.Credentials.from_service_account_file``
is stubbed so the tests do not require a real JSON key on disk.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from maintainance_scripts import gcp_credentials


@pytest.fixture
def stub_loader(monkeypatch):
    """Replace ``service_account.Credentials.from_service_account_file`` with
    a stub that records the path it was called with and returns a sentinel."""
    calls: list[str] = []
    sentinel = object()

    def fake_loader(path: str):
        calls.append(path)
        return sentinel

    monkeypatch.setattr(
        gcp_credentials.service_account.Credentials,
        "from_service_account_file",
        staticmethod(fake_loader),
    )
    return calls, sentinel


def test_cloud_run_returns_none(monkeypatch, stub_loader):
    """Under Cloud Run we hand control to ADC -- the client library finds
    the metadata-server credentials itself. Returning a real Credentials
    object here would shadow ADC and break ambient auth."""
    monkeypatch.setenv("K_SERVICE", "ro-daily-ingest")
    calls, _ = stub_loader

    assert gcp_credentials.get_gcp_credentials() is None
    assert calls == []  # The file loader must NOT be invoked on Cloud Run.


def test_uses_explicit_override_env_var(monkeypatch, tmp_path, stub_loader):
    """GOOGLE_APPLICATION_CREDENTIALS, when set locally, must win over the
    default ``secrets/gcs_credentials.json`` path."""
    monkeypatch.delenv("K_SERVICE", raising=False)
    monkeypatch.delenv("CLOUD_RUN_JOB", raising=False)
    override = tmp_path / "my-creds.json"
    override.write_text("{}")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(override))
    calls, sentinel = stub_loader

    result = gcp_credentials.get_gcp_credentials()

    assert result is sentinel
    assert calls == [str(override)]


def test_falls_back_to_default_secrets_path(monkeypatch, tmp_path, stub_loader):
    """Without the override the resolver loads
    ``settings.GCS_CREDENTIALS_FILE``. Redirect that constant to a scratch
    file so the test is hermetic."""
    monkeypatch.delenv("K_SERVICE", raising=False)
    monkeypatch.delenv("CLOUD_RUN_JOB", raising=False)
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    default = tmp_path / "gcs_credentials.json"
    default.write_text("{}")
    monkeypatch.setattr(gcp_credentials, "GCS_CREDENTIALS_FILE", default)
    calls, sentinel = stub_loader

    result = gcp_credentials.get_gcp_credentials()

    assert result is sentinel
    assert calls == [str(default)]


def test_missing_default_file_raises(monkeypatch, tmp_path, stub_loader):
    """File-missing is the most common local misconfig. The resolver must
    fail loudly with a path-bearing message so the operator knows where to
    drop the key, instead of silently writing to a wrong project."""
    monkeypatch.delenv("K_SERVICE", raising=False)
    monkeypatch.delenv("CLOUD_RUN_JOB", raising=False)
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    missing = tmp_path / "does-not-exist.json"
    monkeypatch.setattr(gcp_credentials, "GCS_CREDENTIALS_FILE", missing)

    with pytest.raises(FileNotFoundError, match=re.escape(str(missing))):
        gcp_credentials.get_gcp_credentials()


def test_missing_override_file_raises(monkeypatch, tmp_path, stub_loader):
    """An explicit GOOGLE_APPLICATION_CREDENTIALS pointing at a missing file
    must NOT silently fall back to the default path -- the override is the
    user's stated intent and must be honoured or rejected."""
    monkeypatch.delenv("K_SERVICE", raising=False)
    monkeypatch.delenv("CLOUD_RUN_JOB", raising=False)
    bogus = tmp_path / "missing.json"
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(bogus))
    # Also point the default at a file that DOES exist; if the resolver
    # fell back to it the test would pass for the wrong reason.
    default = tmp_path / "gcs_credentials.json"
    default.write_text("{}")
    monkeypatch.setattr(gcp_credentials, "GCS_CREDENTIALS_FILE", default)

    with pytest.raises(FileNotFoundError, match=re.escape(str(bogus))):
        gcp_credentials.get_gcp_credentials()
