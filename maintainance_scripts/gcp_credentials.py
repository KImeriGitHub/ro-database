"""Resolve GCP credentials for both the container and local environments.

Used by every client library that talks to GCP (Cloud Storage, Secret
Manager, etc.). Credentials are service-agnostic, so a single helper is
enough for the whole codebase.

This project uses Application Default Credentials (ADC) on every host:

- On Cloud Run the platform injects ADC via the bound service account; the
  Google client libraries pick them up from the metadata server.
- Locally the operator runs ``gcloud auth application-default login`` once,
  which writes credentials to the standard ADC location
  (``%APPDATA%\\gcloud\\application_default_credentials.json`` on Windows,
  ``~/.config/gcloud/application_default_credentials.json`` elsewhere).
  ``GOOGLE_APPLICATION_CREDENTIALS`` still overrides that path if set.

In every case we return ``None`` so the client library resolves ADC itself.
No service-account key is read from ``secrets/`` -- that file holds project
configuration (project id, bucket, secret names) only.
"""

from __future__ import annotations


def get_gcp_credentials() -> None:
    """Return ``None`` so callers fall through to Application Default Credentials.

    Kept as a function (rather than inlining ``None``) so callers retain a
    single seam for tests and future credential-resolution changes.
    """
    return None
