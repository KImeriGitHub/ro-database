"""Mirror the GCS bucket down to the local project tree.

Invoked from the workstation (not from the container). By default it pulls
``catalog/``, ``historical/`` and ``daily/`` into the project root so the
local mirror matches what the container has been writing.

Examples:
    python scheduled_scripts/sync_gcs_to_local.py
    python scheduled_scripts/sync_gcs_to_local.py --only daily --workers 8 --from-date 2026-05-01
    python scheduled_scripts/sync_gcs_to_local.py --only daily --workers 8
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from maintainance_scripts import gcs_client
from maintainance_scripts.logging_setup import configure_logging
from maintainance_scripts.paths import (
    configured_database_dir,
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


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {value!r}")


def sync(
    local_root: Path,
    which: list[str],
    workers: int = 2,
    from_date: date | None = None,
) -> None:
    for name in which:
        prefix_fn, local_fn = TREES[name]
        prefix = prefix_fn() if name != "daily" else prefix_fn(None)
        dest = local_fn(local_root)
        name_filter = None
        if name == "daily" and from_date is not None:
            cutoff = from_date.isoformat()
            # daily blobs are <date>/.../file.parquet; ISO dates sort
            # lexicographically, so a string compare on the leading folder
            # keeps only days on or after the cutoff.
            name_filter = lambda rel, cutoff=cutoff: rel.split("/", 1)[0] >= cutoff
            logger.info(f"Syncing gs://.../{prefix}/ (from {cutoff}) -> {dest}")
        else:
            logger.info(f"Syncing gs://.../{prefix}/ -> {dest}")
        written = gcs_client.download_tree(
            prefix, dest, workers=workers, name_filter=name_filter
        )
        logger.info(f"  {len(written)} new/changed files")


def main() -> int:
    configure_logging()
    parser = argparse.ArgumentParser(description="Download GCS bucket to local mirror")
    parser.add_argument(
        "--local-root", type=Path, default=configured_database_dir(),
        help=(
            "Local destination root (default: database_dir from "
            "secrets/dir_location.txt, or PROJECT_ROOT when unset)."
        ),
    )
    parser.add_argument(
        "--only", nargs="+", default=list(TREES.keys()), choices=list(TREES.keys()),
        help="Subset of trees to sync (default: all)",
    )
    parser.add_argument(
        "--workers", type=int, default=1,
        help="Concurrent download workers per tree (default: 1). Raise on a fast link.",
    )
    parser.add_argument(
        "--from-date", type=_parse_date, default=None, metavar="YYYY-MM-DD",
        help=(
            "Only download daily/ folders dated on or after this date. "
            "Has no effect on catalog/ or historical/."
        ),
    )
    args = parser.parse_args()
    sync(args.local_root, args.only, workers=args.workers, from_date=args.from_date)
    return 0


if __name__ == "__main__":
    sys.exit(main())
