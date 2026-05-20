"""GCP project, bucket and container configuration.

All deployment-specific identifiers (project id, region, bucket name,
Cloud Run job, Artifact Registry image, Secret Manager secret names) are
read from environment variables first, then from
``secrets/gcs_credentials.json`` as a local-only fallback. There are
intentionally **no hard-coded defaults**: this repo is public, so leaking
a particular deployment's identifiers into source would be a footgun.
Missing values surface as ``None`` here and the client code fails loudly
the moment it tries to talk to GCP.

Resolution order for each identifier (first non-empty wins):

1. ``os.environ[<NAME>]`` -- the only path used inside Cloud Run, where
   the secrets file is not shipped with the container.
2. The matching key in ``secrets/gcs_credentials.json`` -- intended for
   local development so the operator does not need to export env vars in
   every shell. The file holds configuration only; authentication goes
   through Application Default Credentials (``gcloud auth
   application-default login``), so no service-account key lives on disk.

Expected env vars / JSON keys:

    GCP_PROJECT_ID / project_id            Project hosting the bucket / Cloud Run job.
    GCP_REGION / gcp_region                e.g. ``europe-west3``.
    GCS_BUCKET / gcs_bucket                The single data bucket.
    CLOUD_RUN_JOB_NAME / cloud_run_job_name  Cloud Run job name (build/deploy use this).
    SECRET_AV_KEY_STANDARD / secret_av_key_standard  Secret Manager name for the standard tier AV key.
    SECRET_AV_KEY_PREMIUM / secret_av_key_premium    Secret Manager name for the premium tier AV key.
    USE_SECRET_MANAGER_FOR_AV_KEYS / use_secret_manager_for_av_keys
                                           ``true`` to enable Secret Manager
                                           fallback for Alpha Vantage keys.
                                           Default ``false`` so local runs
                                           fail loudly on misconfig.

The data-layout prefixes inside the bucket (``catalog``, ``historical``,
``daily``) are part of the published schema in SPEC.md and stay hard-coded.
"""

import json
import logging
import os

from config.settings import GCS_CREDENTIALS_FILE

logger = logging.getLogger(__name__)


def _load_local_config() -> dict:
    """Load the local config JSON if present. Missing or malformed -> empty."""
    if not GCS_CREDENTIALS_FILE.exists():
        return {}
    try:
        with open(GCS_CREDENTIALS_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning(
            f"Could not parse {GCS_CREDENTIALS_FILE} for GCP config -> {exc}. "
            "Falling back to environment variables only."
        )
        return {}
    return data if isinstance(data, dict) else {}


_LOCAL_CFG = _load_local_config()


def _resolve(env_name: str, json_key: str) -> str | None:
    value = os.environ.get(env_name)
    if value:
        return value
    value = _LOCAL_CFG.get(json_key)
    return value if isinstance(value, str) and value else None


GCP_PROJECT_ID = _resolve("GCP_PROJECT_ID", "project_id")
GCP_REGION = _resolve("GCP_REGION", "gcp_region")
GCS_BUCKET = _resolve("GCS_BUCKET", "gcs_bucket")

# Data-layout prefixes inside the bucket. Kept in sync with the local layout
# from config.settings so the local <-> GCS path translator is trivial.
# These describe the public schema and are safe to keep hard-coded.
GCS_CATALOG_PREFIX = "catalog"
GCS_HISTORICAL_PREFIX = "historical"
GCS_DAILY_PREFIX = "daily"

# Cloud Run job that runs the daily ingest. The scheduler targets this name;
# the build pipeline uses it as the image/service name.
CLOUD_RUN_JOB_NAME = _resolve("CLOUD_RUN_JOB_NAME", "cloud_run_job_name")

# Secret Manager resource names. The container reads API keys from Secret
# Manager instead of a mounted file; the names are stable across revisions.
# One secret per tier, each holding a single API key as its payload.
SECRET_AV_KEY_STANDARD = _resolve("SECRET_AV_KEY_STANDARD", "secret_av_key_standard")
SECRET_AV_KEY_PREMIUM = _resolve("SECRET_AV_KEY_PREMIUM", "secret_av_key_premium")

# Opt-in fallback: when the local secrets file is missing or incomplete,
# maintainance_scripts.get_api_key will pull keys from Secret Manager only if
# this is True. Keeps local-only runs from failing on unconfigured GCP setups.
_use_sm = os.environ.get("USE_SECRET_MANAGER_FOR_AV_KEYS")
if _use_sm is None:
    _use_sm_local = _LOCAL_CFG.get("use_secret_manager_for_av_keys")
    if isinstance(_use_sm_local, bool):
        USE_SECRET_MANAGER_FOR_AV_KEYS = _use_sm_local
    else:
        USE_SECRET_MANAGER_FOR_AV_KEYS = (
            isinstance(_use_sm_local, str) and _use_sm_local.lower() == "true"
        )
else:
    USE_SECRET_MANAGER_FOR_AV_KEYS = _use_sm.lower() == "true"
