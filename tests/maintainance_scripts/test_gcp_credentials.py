"""Tests for ``maintainance_scripts.gcp_credentials.get_gcp_credentials``.

The resolver now always returns ``None`` so every ``google.cloud.*`` client
falls through to Application Default Credentials. On Cloud Run ADC is the
bound service account; locally it is whatever ``gcloud auth
application-default login`` (or ``GOOGLE_APPLICATION_CREDENTIALS``)
provides. No service-account key is ever read from ``secrets/``.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from maintainance_scripts import gcp_credentials


def test_returns_none_on_cloud_run(monkeypatch):
    """Cloud Run path: the platform injects ADC; the resolver hands control
    to the client library by returning ``None``."""
    monkeypatch.setenv("K_SERVICE", "ro-daily-ingest")

    assert gcp_credentials.get_gcp_credentials() is None


def test_returns_none_locally(monkeypatch):
    """Local path: ADC is set up via ``gcloud auth application-default
    login`` (or the ``GOOGLE_APPLICATION_CREDENTIALS`` override). Either
    way the resolver returns ``None`` -- the client picks up ADC itself."""
    monkeypatch.delenv("K_SERVICE", raising=False)
    monkeypatch.delenv("CLOUD_RUN_JOB", raising=False)
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)

    assert gcp_credentials.get_gcp_credentials() is None
