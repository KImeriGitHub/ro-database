"""Mirror the GCS bucket down to the local project tree.

Invoked from the workstation (not from the container). By default it pulls
``catalog/``, ``historical/`` and ``daily/`` into the project root so the
local mirror matches what the container has been writing.

Examples:
    python scheduled_scripts/sync_gcs_to_local.py
    python scheduled_scripts/sync_gcs_to_local.py --only daily
    python scheduled_scripts/sync_gcs_to_local.py --local-root ~/ro-mirror
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import settings
from maintainance_scripts import gcs_client
from maintainance_scripts.logging_setup import configure_logging
from maintainance_scripts.paths import (
    gcs_catalog_prefix,
    gcs_daily_prefix,
    gcs_historical_prefix,
)

logger = logging.getLogger(__name__)

TREES = {
    "catalog": (gcs_catalog_prefix, lambda r: r / "catalog"),
    "historical": (gcs_historical_prefix, lambda r: r / "historical"),
    "daily": (gcs_daily_prefix, lambda r: r / "daily"),
}


def sync(local_root: Path, which: list[str]) -> None:
    for name in which:
        prefix_fn, local_fn = TREES[name]
        prefix = prefix_fn() if name != "daily" else prefix_fn(None)
        dest = local_fn(local_root)
        logger.info(f"Syncing gs://.../{prefix}/ -> {dest}")
        written = gcs_client.download_tree(prefix, dest)
        logger.info(f"  {len(written)} new/changed files")


def main() -> int:
    configure_logging()
    parser = argparse.ArgumentParser(description="Download GCS bucket to local mirror")
    parser.add_argument(
        "--local-root", type=Path, default=settings.PROJECT_ROOT,
        help="Local destination root (default: project root)",
    )
    parser.add_argument(
        "--only", nargs="+", default=list(TREES.keys()), choices=list(TREES.keys()),
        help="Subset of trees to sync (default: all)",
    )
    args = parser.parse_args()
    sync(args.local_root, args.only)
    return 0


if __name__ == "__main__":
    sys.exit(main())
