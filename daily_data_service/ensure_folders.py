"""Create the ``daily/YYYY-MM-DD/`` directory tree for a given folder-date.

Usage:
    python ensure_folders.py [--daily-dir PATH] [--folder-date YYYY-MM-DD]
"""

from datetime import date, datetime
from pathlib import Path

from config.settings import DISABLED_ASSET_TYPES
from daily_data_service._common import compute_folder_date, ET

DAILY_TREE = [
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
DAILY_TREE = [
    leaf for leaf in DAILY_TREE
    if leaf.split("/")[0] not in DISABLED_ASSET_TYPES
]


def ensure_daily_folders(daily_dir: Path, folder_date: date) -> Path:
    """Create ``daily_dir/<folder-date>/<subtree>``. Returns the day-root."""
    day_root = daily_dir / folder_date.isoformat()
    for leaf in DAILY_TREE:
        (day_root / leaf).mkdir(parents=True, exist_ok=True)
    return day_root


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Create daily data folder structure")
    parser.add_argument(
        "--daily-dir", type=Path, default=None,
        help="Daily directory (default: <project>/daily)",
    )
    parser.add_argument(
        "--folder-date", type=str, default=None,
        help="Folder date YYYY-MM-DD (default: computed from now)",
    )
    args = parser.parse_args()

    daily_dir = args.daily_dir or (Path(__file__).resolve().parent.parent / "daily")
    if args.folder_date:
        folder_date = date.fromisoformat(args.folder_date)
    else:
        folder_date = compute_folder_date(datetime.now(tz=ET))

    path = ensure_daily_folders(daily_dir, folder_date)
    print(f"Daily folder structure created at {path}")
