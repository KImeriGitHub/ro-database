from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

SECRETS_DIR = Path(__file__).resolve().parent.parent / "secrets"
KEYS_FILE = SECRETS_DIR / "alpha_vantage_keys"

VALID_TIERS = ("standard", "premium")


def _read_key_from_file(tier: str) -> str | None:
    """Return the key for *tier* from the local secrets file, or ``None`` if
    the file, the tier entry, or a real value is missing.

    We treat file-missing, tier-missing and placeholder values uniformly:
    any of them signals "fall back to Secret Manager if enabled".
    """
    if not KEYS_FILE.exists():
        return None

    keys: dict[str, str] = {}
    for line in KEYS_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name, _, value = line.partition("=")
        keys[name.strip()] = value.strip()

    key = keys.get(tier)
    if not key or key.startswith("YOUR_"):
        return None
    return key


def _read_key_from_secret_manager(tier: str) -> str:
    # Imported lazily so local runs without the flag never import the GCP
    # client (and do not need google-cloud-secret-manager installed).
    from config.gcp import SECRET_AV_KEY_PREMIUM, SECRET_AV_KEY_STANDARD
    from maintainance_scripts.secret_manager_client import get_secret

    tier_to_secret = {
        "standard": SECRET_AV_KEY_STANDARD,
        "premium": SECRET_AV_KEY_PREMIUM,
    }
    secret_name = tier_to_secret[tier]
    logger.info(f"Falling back to Secret Manager for Alpha Vantage '{tier}' key (secret: {secret_name})")
    return get_secret(secret_name)


def get_alpha_vantage_key(tier: str = "standard") -> str:
    if tier not in VALID_TIERS:
        raise ValueError(f"Unknown tier '{tier}'. Choose from: {VALID_TIERS}")

    key = _read_key_from_file(tier)
    if key:
        return key

    # Lazy import so the config module is not loaded in tests that stub
    # KEYS_FILE but never exercise the fallback branch.
    from config.gcp import USE_SECRET_MANAGER_FOR_AV_KEYS

    if USE_SECRET_MANAGER_FOR_AV_KEYS:
        return _read_key_from_secret_manager(tier)

    if not KEYS_FILE.exists():
        raise FileNotFoundError(
            f"Keys file not found: {KEYS_FILE}. "
            "Create it, or set USE_SECRET_MANAGER_FOR_AV_KEYS=true to pull "
            "from GCP Secret Manager."
        )
    raise KeyError(
        f"No usable '{tier}' key in {KEYS_FILE} (missing or still a placeholder). "
        "Fill it in, or set USE_SECRET_MANAGER_FOR_AV_KEYS=true to pull from "
        "GCP Secret Manager."
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Retrieve an Alpha Vantage API key.")
    parser.add_argument("--tier", choices=VALID_TIERS, default="standard",
                        help="API key tier (default: standard)")
    args = parser.parse_args()

    print(get_alpha_vantage_key(args.tier))
