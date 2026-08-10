"""Create the historical/ directory tree.

Usage:
    python ensure_folders.py [--historical-dir PATH]
"""

from pathlib import Path

from config.settings import DISABLED_ASSET_TYPES


HISTORICAL_TREE = [
    "stocks/prices",
    "stocks/prices_daily",
    "stocks/income_statement",
    "stocks/balance_sheet",
    "stocks/cash_flow",
    "stocks/earnings",
    "stocks/earnings_estimates",
    "stocks/insider",
    "stocks/sentiment",
    "etfs/prices",
    "etfs/prices_daily",
    "etfs/etf_profile",
    "forex",
    "indices",
    "cryptocurrencies",
    "commodities",
    "economic",
]
# Disabled types get no folder; existing ones are left in place.
HISTORICAL_TREE = [
    leaf for leaf in HISTORICAL_TREE
    if leaf.split("/")[0] not in DISABLED_ASSET_TYPES
]


def ensure_historical_folders(historical_dir: Path | None = None) -> Path:
    """Create the full historical/ directory tree. Returns the historical_dir path."""
    if historical_dir is None:
        historical_dir = Path(__file__).resolve().parent.parent / "historical"

    for leaf in HISTORICAL_TREE:
        (historical_dir / leaf).mkdir(parents=True, exist_ok=True)

    return historical_dir


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Create historical data folder structure")
    parser.add_argument(
        "--historical-dir",
        type=Path,
        default=None,
        help="Historical directory (default: <project>/historical)",
    )
    args = parser.parse_args()
    path = ensure_historical_folders(args.historical_dir)
    print(f"Historical folder structure created at {path}")
