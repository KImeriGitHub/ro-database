"""Resolve GCP credentials for both the container and local environments.

Used by every client library that talks to GCP (Cloud Storage, Secret
Manager, etc.). Credentials are service-agnostic, so a single helper is
enough for the whole codebase.

Resolution order:

1. Inside Cloud Run the platform injects Application Default Credentials, so
   we return ``None`` and let ``google.auth`` pick them up automatically.
2. Locally, load a service-account JSON file from ``secrets/gcs_credentials.json``
   (overridable via ``GOOGLE_APPLICATION_CREDENTIALS``) and build a
   ``google.oauth2.service_account.Credentials`` from it.
3. If neither path is available we raise: failing loudly beats silently
   writing to the wrong project.
"""

from __future__ import annotations

import os
from pathlib import Path

from google.oauth2 import service_account

from config.settings import GCS_CREDENTIALS_FILE
from maintainance_scripts.logging_setup import detect_cloud_run


def get_gcp_credentials() -> service_account.Credentials | None:
    """Return credentials suitable for any ``google.cloud.*`` client.

    ``None`` is a valid return value and tells the client to use ADC, which
    is the correct behaviour on Cloud Run.
    """
    if detect_cloud_run():
        return None

    override = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    path = Path(override) if override else GCS_CREDENTIALS_FILE
    if not path.exists():
        raise FileNotFoundError(
            f"GCP credentials file not found: {path}. "
            "Set GOOGLE_APPLICATION_CREDENTIALS or drop the service-account "
            "JSON at secrets/gcs_credentials.json."
        )

    return service_account.Credentials.from_service_account_file(str(path))
