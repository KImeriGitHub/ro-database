"""Push the locally-built historical tree (and the initial catalog) up to GCS.

Run once, from the workstation, after ``historical_data_setup`` finishes.
After this upload the container takes over and the local ``historical/``
folder becomes a read-only mirror maintained by ``sync_gcs_to_local.py``.

By default this refuses to overwrite existing blobs: the historical setup is
append-only and clobbering a blob usually means something is wrong. Pass
``--force`` to allow overwrites (e.g. when re-uploading after a local
re-run that regenerated a few parquet files).
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from maintainance_scripts import gcs_client
from maintainance_scripts.logging_setup import configure_logging
from maintainance_scripts.paths import (
    configured_database_dir,
    gcs_catalog_prefix,
    gcs_historical_prefix,
)

logger = logging.getLogger(__name__)


def push(local_root: Path, include_catalog: bool, force: bool, workers: int = 2) -> None:
    hist_local = local_root / "historical"
    if not hist_local.exists():
        raise FileNotFoundError(f"historical/ not found at {hist_local}")

    hist_prefix = gcs_historical_prefix()
    if not force:
        only_local, _only_remote, size_mismatch = gcs_client.diff_local_vs_remote(
            hist_local, hist_prefix,
        )
        existing_overlap = [b for b in size_mismatch]
        if existing_overlap:
            raise RuntimeError(
                f"{len(existing_overlap)} blobs already exist with different sizes. "
                "Re-run with --force to overwrite, or inspect first:\n  "
                + "\n  ".join(existing_overlap[:10])
            )
        logger.info(f"Uploading {len(only_local)} new blobs under {hist_prefix}/")

    gcs_client.upload_tree(hist_local, hist_prefix, workers=workers)

    if include_catalog:
        cat_local = local_root / "catalog"
        if cat_local.exists():
            logger.info("Uploading catalog/ alongside historical/")
            gcs_client.upload_tree(cat_local, gcs_catalog_prefix(), workers=workers)
        else:
            logger.warning(f"catalog/ not found at {cat_local}, skipping")


def main() -> int:
    configure_logging()
    parser = argparse.ArgumentParser(description="Push local historical/ to GCS")
    parser.add_argument(
        "--local-root", type=Path, default=configured_database_dir(),
        help=(
            "Local source root (default: database_dir from "
            "secrets/dir_location.txt, or PROJECT_ROOT when unset)."
        ),
    )
    parser.add_argument(
        "--skip-catalog", action="store_true",
        help="Only push historical/, do not also push catalog/",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Overwrite blobs even if sizes differ",
    )
    parser.add_argument(
        "--workers", type=int, default=1,
        help="Concurrent upload workers (default: 1). Raise on a fast link.",
    )
    args = parser.parse_args()
    push(
        args.local_root,
        include_catalog=not args.skip_catalog,
        force=args.force,
        workers=args.workers,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
