"""Local filesystem paths and project-wide constants.

All runtime code should import paths and tunables from here instead of
recomputing them from ``__file__``. Nothing in this module touches the
network or reads secrets.
"""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SECRETS_DIR = PROJECT_ROOT / "secrets"
ALPHA_VANTAGE_KEYS_FILE = SECRETS_DIR / "alpha_vantage_keys"
GCS_CREDENTIALS_FILE = SECRETS_DIR / "gcs_credentials.json"
DIR_LOCATION_FILE = SECRETS_DIR / "dir_location.txt"

# Alpha Vantage hard cap is 75 calls/min on the premium plan; we configure the
# sliding window to 70 and leave 5 calls as a safety margin for retries and
# catalog-side sweeps running in parallel.
AV_RATE_LIMIT_PER_MIN = 70
AV_HARD_CAP_PER_MIN = 75

# Asset types dropped from every ingestion plan at import time: their AV data
# endpoint is not available on the plan we hold. INDEX_DATA moved behind AV's
# 150+ requests/min plans (verified 2026-08-10). INDEX_CATALOG is not gated,
# so the catalog updater still keeps catalog/indices.parquet current.
DISABLED_ASSET_TYPES: frozenset[str] = frozenset({"indices"})
