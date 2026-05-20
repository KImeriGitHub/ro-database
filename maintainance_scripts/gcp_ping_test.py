"""End-to-end GCP health check (the "ping").

Run this whenever the code is freshly placed in a new container, deployed to
a new project, or moved to a new local machine. If the ping passes, then
credentials, network egress, bucket permissions and Secret Manager access
are all healthy and the daily pipeline can be safely scheduled.

The script runs two steps:

**Step 1 -- GCS roundtrip.**
    1. Resolves GCP credentials via Application Default Credentials (the
       :func:`maintainance_scripts.gcp_credentials.get_gcp_credentials`
       helper returns ``None`` so the client picks ADC up itself -- the
       Cloud Run service account in the container, ``gcloud auth
       application-default login`` credentials locally).
    2. Instantiates the shared GCS client.
    3. Lists ``gs://<GCS_BUCKET>/`` (one page) -- proves read access.
    4. Writes a throwaway blob at ``_health/ping_<UTC>_<rand>.txt`` -- proves write.
    5. Reads it back, checks payload matches -- proves read-your-write.
    6. Deletes the blob -- proves delete access and avoids leftover litter.

**Step 2 -- Secret Manager access.**
    For each configured AV-key secret (``SECRET_AV_KEY_STANDARD`` /
    ``SECRET_AV_KEY_PREMIUM``), fetches the latest version's payload via
    :func:`maintainance_scripts.secret_manager_client.get_secret`. This
    confirms the secret exists, has at least one version, and the service
    account holds ``roles/secretmanager.secretAccessor`` on it. The
    fetched payload is discarded (length only is logged).

Each failure mode logs a specific diagnosis so the operator knows whether to
fix credentials, IAM, the bucket name, env-var configuration, or the
network. A successful run logs ``PING OK`` and exits 0; any failure exits 1.

Logs always land both on stdout (Cloud Logging on Cloud Run) and in
``logs/<UTC-timestamp>_gcp_ping_test.log`` so a failed deploy leaves a
durable trail next to the source tree.

Usage:
    python -m maintainance_scripts.gcp_ping_test

Bucket and project id are resolved through the shared GCS client, which
reads ``GCS_BUCKET`` and ``GCP_PROJECT_ID`` from ``config.gcp``.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from google.api_core import exceptions as gax
from google.auth import exceptions as gauth

from config.gcp import (
    SECRET_AV_KEY_PREMIUM,
    SECRET_AV_KEY_STANDARD,
)
from maintainance_scripts import gcs_client
from maintainance_scripts.logging_setup import configure_logging

logger = logging.getLogger(__name__)

_HEALTH_PREFIX = "_health"


def _run_gcs_ping() -> None:
    client = gcs_client.get_client()
    bucket = gcs_client.get_bucket()
    gcs_bucket = bucket.name

    if not gcs_bucket:
        raise RuntimeError(
            "GCS_BUCKET is not configured. Set the GCS_BUCKET environment "
            "variable before running the ping."
        )
    if not client.project:
        logger.warning(
            "GCP_PROJECT_ID is not set; falling back to the project bound to "
            "the resolved credentials (ADC default)."
        )

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    blob_name = f"{_HEALTH_PREFIX}/ping_{timestamp}_{uuid4().hex[:8]}.txt"
    payload = f"ping {timestamp}\n".encode("utf-8")

    logger.info(f"Ping target: gs://{gcs_bucket}/{blob_name}")

    logger.info(f"Listing gs://{gcs_bucket}/ (first page) ...")
    listed = list(bucket.list_blobs(max_results=5))
    logger.info(f"List ok ({len(listed)} blob(s) returned in first page)")

    logger.info("Writing ping blob ...")
    blob = bucket.blob(blob_name)
    blob.upload_from_string(payload, content_type="text/plain")
    logger.info("Write ok")

    logger.info("Reading ping blob back ...")
    roundtrip = blob.download_as_bytes()
    if roundtrip != payload:
        raise RuntimeError(
            f"Roundtrip mismatch: wrote {payload!r}, read back {roundtrip!r}."
        )
    logger.info("Read ok (payload matches)")

    logger.info("Deleting ping blob ...")
    blob.delete()
    logger.info("Delete ok")


def _run_secret_manager_ping() -> None:
    configured: list[tuple[str, str]] = [
        (tier, name)
        for tier, name in (
            ("standard", SECRET_AV_KEY_STANDARD),
            ("premium", SECRET_AV_KEY_PREMIUM),
        )
        if name
    ]
    if not configured:
        logger.warning(
            "Neither SECRET_AV_KEY_STANDARD nor SECRET_AV_KEY_PREMIUM is set; "
            "skipping Secret Manager check. Set them in the Cloud Run job "
            "env if the container should fetch AV keys from Secret Manager."
        )
        return

    gcp_project_id = gcs_client.get_client().project
    if not gcp_project_id:
        raise RuntimeError(
            "Cannot reach Secret Manager: GCP_PROJECT_ID is unset, so the "
            "secret resource path would be 'projects/None/secrets/...'. "
            "Set the GCP_PROJECT_ID environment variable."
        )

    # Lazy import so failures in the GCS step still surface their original
    # exception type instead of an import error from the secret_manager module.
    from maintainance_scripts.secret_manager_client import get_secret

    for tier, name in configured:
        resource = f"projects/{gcp_project_id}/secrets/{name}/versions/latest"
        logger.info(f"Fetching Secret Manager secret for AV '{tier}' tier: {resource}")
        try:
            value = get_secret(name)
        except gax.NotFound as exc:
            raise RuntimeError(
                f"Secret Manager secret '{name}' not found in project "
                f"'{gcp_project_id}'. Create it (`gcloud secrets create {name}`) "
                f"or check that SECRET_AV_KEY_{tier.upper()} matches the real "
                f"secret name."
            ) from exc
        except gax.FailedPrecondition as exc:
            raise RuntimeError(
                f"Secret '{name}' exists but has no versions to read. Add one "
                f"with `gcloud secrets versions add {name} --data-file=<path>`."
            ) from exc
        except gax.PermissionDenied as exc:
            raise RuntimeError(
                f"Permission denied reading secret '{name}'. Grant the runner "
                f"service account roles/secretmanager.secretAccessor on the "
                f"secret resource (resource-scoped, not project-wide)."
            ) from exc

        if not value:
            raise RuntimeError(
                f"Secret '{name}' returned an empty payload. The latest "
                f"version may be a placeholder; re-add it with the real key."
            )
        logger.info(f"Secret '{name}' ok (payload length {len(value)})")


def _run_step(label: str, fn) -> int:
    """Run *fn*, mapping every known failure mode to a specific log line."""
    try:
        fn()
    except FileNotFoundError as exc:
        logger.error(
            f"PING FAIL [{label}]: missing file -> {exc}. "
            "If this points at a credentials JSON, the ADC override "
            "GOOGLE_APPLICATION_CREDENTIALS is set to a path that does not "
            "exist; unset it or run 'gcloud auth application-default login'."
        )
        return 1
    except gauth.DefaultCredentialsError as exc:
        logger.error(
            f"PING FAIL [{label}]: no ADC resolvable -> {exc}. "
            "On Cloud Run check the service account binding; locally run "
            "'gcloud auth application-default login' (or point "
            "GOOGLE_APPLICATION_CREDENTIALS at a valid creds file)."
        )
        return 1
    except gax.Forbidden as exc:
        logger.error(
            f"PING FAIL [{label}]: permission denied -> {exc}. "
            "Check the service account's IAM roles on the target resource."
        )
        return 1
    except gax.NotFound as exc:
        logger.error(
            f"PING FAIL [{label}]: resource not found -> {exc}. "
            "Verify GCS_BUCKET and GCP_PROJECT_ID env vars match real "
            "resources the account can see."
        )
        return 1
    except gax.GoogleAPICallError as exc:
        logger.error(f"PING FAIL [{label}]: GCP API error -> {exc}")
        return 1
    except (ConnectionError, OSError) as exc:
        logger.error(
            f"PING FAIL [{label}]: network or socket error -> {exc}. "
            "Likely egress is blocked or DNS for *.googleapis.com is failing "
            "from this environment."
        )
        return 1
    except RuntimeError as exc:
        logger.error(f"PING FAIL [{label}]: {exc}")
        return 1
    except Exception as exc:
        logger.exception(f"PING FAIL [{label}]: unexpected error -> {exc}")
        return 1
    return 0


def main() -> int:
    configure_logging(log_to_file=True)

    rc = _run_step("GCS roundtrip", _run_gcs_ping)
    if rc != 0:
        return rc

    rc = _run_step("Secret Manager", _run_secret_manager_ping)
    if rc != 0:
        return rc

    logger.info("PING OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
