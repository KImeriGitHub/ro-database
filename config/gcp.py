"""GCP project, bucket and container configuration.

All deployment-specific identifiers (project id, region, bucket name,
Cloud Run job, Artifact Registry image, Secret Manager secret names) are
read from environment variables. There are intentionally **no defaults**:
this repo is public, so leaking a particular deployment's identifiers into
source would be a footgun. Missing values surface as ``None`` here and the
client code fails loudly the moment it tries to talk to GCP.

Expected env vars (set them in the Cloud Run job spec, your shell, or a
local ``.env`` loader before importing config.gcp):

    GCP_PROJECT_ID                   Project hosting the bucket / Cloud Run job.
    GCP_REGION                       e.g. ``europe-west3``.
    GCS_BUCKET                       The single data bucket.
    CLOUD_RUN_JOB_NAME               Cloud Run job name (build/deploy use this).
    CONTAINER_IMAGE                  Full image ref. If unset and the three
                                     variables above are set, a default of
                                     ``<region>-docker.pkg.dev/<project>/ro/<job>:latest``
                                     is composed at import time.
    SECRET_AV_KEY_STANDARD           Secret Manager name for the standard tier AV key.
    SECRET_AV_KEY_PREMIUM            Secret Manager name for the premium tier AV key.
    USE_SECRET_MANAGER_FOR_AV_KEYS   ``true`` to enable Secret Manager fallback
                                     for Alpha Vantage keys. Default ``false``
                                     so local runs fail loudly on misconfig.

The data-layout prefixes inside the bucket (``catalog``, ``historical``,
``daily``) are part of the published schema in SPEC.md and stay hard-coded.
"""

import os

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCP_REGION = os.environ.get("GCP_REGION")

GCS_BUCKET = os.environ.get("GCS_BUCKET")

# Data-layout prefixes inside the bucket. Kept in sync with the local layout
# from config.settings so the local <-> GCS path translator is trivial.
# These describe the public schema and are safe to keep hard-coded.
GCS_CATALOG_PREFIX = "catalog"
GCS_HISTORICAL_PREFIX = "historical"
GCS_DAILY_PREFIX = "daily"

# Cloud Run job that runs the daily ingest. The scheduler targets this name;
# the build pipeline uses it as the image/service name.
CLOUD_RUN_JOB_NAME = os.environ.get("CLOUD_RUN_JOB_NAME")

_image_env = os.environ.get("CONTAINER_IMAGE")
if _image_env:
    CONTAINER_IMAGE: str | None = _image_env
elif GCP_REGION and GCP_PROJECT_ID and CLOUD_RUN_JOB_NAME:
    CONTAINER_IMAGE = (
        f"{GCP_REGION}-docker.pkg.dev/{GCP_PROJECT_ID}/ro/{CLOUD_RUN_JOB_NAME}:latest"
    )
else:
    CONTAINER_IMAGE = None

# Secret Manager resource names. The container reads API keys from Secret
# Manager instead of a mounted file; the names are stable across revisions.
# One secret per tier, each holding a single API key as its payload.
SECRET_AV_KEY_STANDARD = os.environ.get("SECRET_AV_KEY_STANDARD")
SECRET_AV_KEY_PREMIUM = os.environ.get("SECRET_AV_KEY_PREMIUM")

# Opt-in fallback: when the local secrets file is missing or incomplete,
# maintainance_scripts.get_api_key will pull keys from Secret Manager only if
# this is True. Keeps local-only runs from failing on unconfigured GCP setups.
USE_SECRET_MANAGER_FOR_AV_KEYS = (
    os.environ.get("USE_SECRET_MANAGER_FOR_AV_KEYS", "false").lower() == "true"
)
