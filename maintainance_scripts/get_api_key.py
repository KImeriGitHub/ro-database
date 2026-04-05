from pathlib import Path

SECRETS_DIR = Path(__file__).resolve().parent.parent / "secrets"
KEYS_FILE = SECRETS_DIR / "alpha_vantage_keys"

VALID_TIERS = ("standard", "premium")


def get_alpha_vantage_key(tier: str = "standard") -> str:
    if tier not in VALID_TIERS:
        raise ValueError(f"Unknown tier '{tier}'. Choose from: {VALID_TIERS}")

    if not KEYS_FILE.exists():
        raise FileNotFoundError(f"Keys file not found: {KEYS_FILE}")

    keys = {}
    for line in KEYS_FILE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name, _, value = line.partition("=")
        keys[name.strip()] = value.strip()

    if tier not in keys:
        raise KeyError(f"No '{tier}' key found in {KEYS_FILE}")

    key = keys[tier]
    if not key or key.startswith("YOUR_"):
        raise ValueError(f"Replace the placeholder for '{tier}' in {KEYS_FILE} with your actual API key.")

    return key


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Retrieve an Alpha Vantage API key.")
    parser.add_argument("--tier", choices=VALID_TIERS, default="standard",
                        help="API key tier (default: standard)")
    args = parser.parse_args()

    print(get_alpha_vantage_key(args.tier))
