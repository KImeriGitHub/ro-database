"""Local filesystem paths and project-wide constants.

All runtime code should import paths and tunables from here instead of
recomputing them from ``__file__``. Nothing in this module touches the
network or reads secrets.
"""

from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SECRETS_DIR = PROJECT_ROOT / "secrets"
ALPHA_VANTAGE_KEYS_FILE = SECRETS_DIR / "alpha_vantage_keys"
GCS_CREDENTIALS_FILE = SECRETS_DIR / "gcs_credentials.json"

CATALOG_DIR = PROJECT_ROOT / "catalog"
HISTORICAL_DIR = PROJECT_ROOT / "historical"
DAILY_DIR = PROJECT_ROOT / "daily"
TRANSFORMED_DIR = PROJECT_ROOT / "transformed"

# Date at which the homegrown PIT fundamentals collection began. Fundamentals
# observations before this date fall back to the 90 day reporting lag
# approximation; observations from this date onward are genuine snapshots.
PIT_COLLECTION_START_DATE = date(2026, 1, 1)

REPORTING_LAG_DAYS = 90

# Alpha Vantage hard cap is 75 calls/min on the premium plan; we configure the
# sliding window to 70 and leave 5 calls as a safety margin for retries and
# catalog-side sweeps running in parallel.
AV_RATE_LIMIT_PER_MIN = 70
AV_HARD_CAP_PER_MIN = 75
