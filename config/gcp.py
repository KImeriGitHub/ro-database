"""GCP project, bucket and container configuration.

Read from environment variables first so the same module works locally and
inside the Cloud Run container. Local development falls back to values that
match the defaults described in the root ``README.md``.
"""

import os

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "randomodyssey")
GCP_REGION = os.environ.get("GCP_REGION", "europe-west3")

# Single bucket holds catalog, historical and daily trees.
GCS_BUCKET = os.environ.get("GCS_BUCKET", f"{GCP_PROJECT_ID}-algo-trading")

# Prefixes inside the bucket. Kept in sync with the local layout from
# config.settings so the local<->GCS path translator is trivial.
GCS_CATALOG_PREFIX = "catalog"
GCS_HISTORICAL_PREFIX = "historical"
GCS_DAILY_PREFIX = "daily"

# Cloud Run job that runs run_daily_ingest.py. The scheduler targets this
# name; the build pipeline uses it as the image/service name.
CLOUD_RUN_JOB_NAME = os.environ.get("CLOUD_RUN_JOB_NAME", "ro-daily-ingest")
CONTAINER_IMAGE = os.environ.get(
    "CONTAINER_IMAGE",
    f"{GCP_REGION}-docker.pkg.dev/{GCP_PROJECT_ID}/ro/{CLOUD_RUN_JOB_NAME}:latest",
)

# Secret Manager resource names. The container reads API keys from Secret
# Manager instead of a mounted file; the names are stable across revisions.
# One secret per tier, each holding a single API key as its payload.
SECRET_AV_KEY_STANDARD = os.environ.get("SECRET_AV_KEY_STANDARD", "alpha-vantage-key-standard")
SECRET_AV_KEY_PREMIUM = os.environ.get("SECRET_AV_KEY_PREMIUM", "alpha-vantage-key-premium")

# Opt-in fallback: when the local secrets file is missing or incomplete,
# maintainance_scripts.get_api_key will pull keys from Secret Manager only if
# this is True. Keeps local-only runs from failing on unconfigured GCP setups.
USE_SECRET_MANAGER_FOR_AV_KEYS = (
    os.environ.get("USE_SECRET_MANAGER_FOR_AV_KEYS", "false").lower() == "true"
)
