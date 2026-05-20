"""Thin wrapper around ``google.cloud.secretmanager`` used to fetch API keys
and other small credentials at runtime.

Mirrors the pattern in ``gcs_client.py``: a cached module-level client so
the underlying HTTP session is reused, and small helpers on top. Anything
beyond plain ``access_secret_version`` belongs in its own module.
"""

from __future__ import annotations

import logging

from google.cloud import secretmanager
from google.cloud.secretmanager import SecretManagerServiceClient

from config.gcp import GCP_PROJECT_ID
from maintainance_scripts.gcp_credentials import get_gcp_credentials

logger = logging.getLogger(__name__)

_client: SecretManagerServiceClient | None = None


def get_client() -> SecretManagerServiceClient:
    """Return a cached Secret Manager client authenticated via ``get_gcp_credentials``."""
    global _client
    if _client is None:
        creds = get_gcp_credentials()
        _client = secretmanager.SecretManagerServiceClient(credentials=creds)
    return _client


def get_secret(
    secret_name: str,
    version: str = "latest",
    project_id: str | None = None,
) -> str:
    """Fetch the payload of ``projects/{project_id}/secrets/{secret_name}/versions/{version}``.

    ``project_id`` defaults to ``GCP_PROJECT_ID`` resolved at call time (not
    at import time), so an unset project id surfaces as a friendly
    ``RuntimeError`` here instead of an opaque ``InvalidArgument`` from the
    SDK after it tries to look up ``projects/None/secrets/...``. Tests that
    monkeypatch ``secret_manager_client.GCP_PROJECT_ID`` have their patch
    honoured.

    The payload is UTF-8 decoded and stripped of surrounding whitespace so
    callers do not need to worry about trailing newlines from ``gcloud
    secrets create --data-file=-``.
    """
    project = project_id or GCP_PROJECT_ID
    if not project:
        raise RuntimeError(
            "GCP_PROJECT_ID is not configured, so the Secret Manager resource "
            "path would be 'projects/None/secrets/...'. Set GCP_PROJECT_ID in "
            "the environment or add 'project_id' to secrets/gcs_credentials.json."
        )
    resource = f"projects/{project}/secrets/{secret_name}/versions/{version}"
    response = get_client().access_secret_version(request={"name": resource})
    logger.info(f"Fetched secret {secret_name} (version {version}) from Secret Manager")
    return response.payload.data.decode("utf-8").strip()
