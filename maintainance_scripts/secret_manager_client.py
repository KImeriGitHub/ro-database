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
    project_id: str = GCP_PROJECT_ID,
) -> str:
    """Fetch the payload of ``projects/{project_id}/secrets/{secret_name}/versions/{version}``.

    The payload is UTF-8 decoded and stripped of surrounding whitespace so
    callers do not need to worry about trailing newlines from ``gcloud
    secrets create --data-file=-``.
    """
    resource = f"projects/{project_id}/secrets/{secret_name}/versions/{version}"
    response = get_client().access_secret_version(request={"name": resource})
    logger.info(f"Fetched secret {secret_name} (version {version}) from Secret Manager")
    return response.payload.data.decode("utf-8").strip()
