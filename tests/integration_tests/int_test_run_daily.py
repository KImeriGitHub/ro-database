"""Integration test: daily_data_service/setup_daily.py.

Runs the daily incremental pull against ``database/`` and then writes a
monitoring report for the produced ``daily/<folder-date>/`` folder. After the
run, re-reduces ``catalog/`` to the integration-test subset (``setup_daily``
calls ``finalize_yield_status`` which expands ``yield_status.parquet`` to
include any new symbols, and we want subsequent runs to keep operating on
the small set).

Behaviour notes
---------------
* The script does NOT need ``daily/`` or any ``daily/<date>/`` folder to
  exist beforehand: ``resolve_start_marker`` creates the marker and
  ``ensure_daily_folders`` creates the dated subtree.
* Re-running on a different ET trading day should leave previously-written
  date folders untouched. The test snapshots ``(path, size, mtime_ns)`` for
  every file under any pre-existing date folder before the run and
  asserts the snapshot is unchanged afterwards.
* If ``previous_date >= folder_date`` (no-op condition), ``run_daily_pull``
  exits without producing a new folder; the test reports that and skips
  the new-folder asserts.

Usage:
    python tests/integration_tests/int_test_run_daily.py
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import re
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from daily_data_service.ensure_folders import DAILY_TREE
from daily_data_service.setup_daily import run_daily_pull
from historical_data_setup._common import get_av_call_count, reset_av_call_count
from monitoring_service.report import (
    REPORT_FILENAME_JSON,
    REPORT_FILENAME_MD,
    run_report_and_persist,
)

from tests.integration_tests._helpers import (
    CATALOG_DIR,
    DAILY_DIR,
    HISTORICAL_DIR,
    configure_int_test_logging,
    reduce_catalogs,
)

logger = logging.getLogger(__name__)

_DATE_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _list_date_dirs(daily_dir: Path) -> list[date]:
    if not daily_dir.exists():
        return []
    out: list[date] = []
    for child in daily_dir.iterdir():
        if not child.is_dir() or not _DATE_DIR_RE.match(child.name):
            continue
        try:
            out.append(date.fromisoformat(child.name))
        except ValueError:
            continue
    return sorted(out)


def _snapshot(roots: list[Path]) -> dict[str, tuple[int, int]]:
    """Return ``{relpath: (size, mtime_ns)}`` for every file under *roots*."""
    snap: dict[str, tuple[int, int]] = {}
    for root in roots:
        if not root.exists():
            continue
        for f in root.rglob("*"):
            if not f.is_file():
                continue
            st = f.stat()
            snap[str(f)] = (st.st_size, st.st_mtime_ns)
    return snap


def _diff_snapshots(
    before: dict[str, tuple[int, int]],
    after: dict[str, tuple[int, int]],
) -> tuple[list[str], list[str], list[str]]:
    """Return (added, removed, changed) keys."""
    before_keys = set(before)
    after_keys = set(after)
    added = sorted(after_keys - before_keys)
    removed = sorted(before_keys - after_keys)
    changed = sorted(k for k in (before_keys & after_keys) if before[k] != after[k])
    return added, removed, changed


def _previous_report_path(daily_dir: Path, folder_date: date) -> Path | None:
    """Return the most recent prior folder's monitoring_report.json, if any."""
    candidates = [d for d in _list_date_dirs(daily_dir) if d < folder_date]
    if not candidates:
        return None
    prior = max(candidates)
    p = daily_dir / prior.isoformat() / REPORT_FILENAME_JSON
    return p if p.exists() else None


def _check_tree(day_root: Path) -> None:
    missing: list[str] = []
    empty: list[str] = []
    for leaf in DAILY_TREE:
        sub = day_root / leaf
        if not sub.exists() or not sub.is_dir():
            missing.append(leaf)
            continue
        if not any(sub.glob("*.parquet")):
            empty.append(leaf)
    if missing:
        raise AssertionError(f"Missing daily subfolders under {day_root}: {missing}")
    if empty:
        logger.warning(
            f"Subfolders with no .parquet files (integration test only): {empty}"
        )

    ingestion = day_root / "ingestion_report.parquet"
    if not ingestion.exists():
        raise AssertionError(f"Missing ingestion report at {ingestion}")
    logger.info(f"All {len(DAILY_TREE)} daily subfolders present at {day_root}")


def _check_monitoring(day_root: Path) -> None:
    missing = [
        fname for fname in (REPORT_FILENAME_JSON, REPORT_FILENAME_MD)
        if not (day_root / fname).exists()
    ]
    if missing:
        raise AssertionError(
            f"Missing monitoring report files in {day_root}: {missing}"
        )
    logger.info(
        f"Monitoring report files present in {day_root.name}/ "
        f"({REPORT_FILENAME_JSON}, {REPORT_FILENAME_MD})."
    )


def main(argv: list[str] | None = None) -> int:
    configure_int_test_logging(__file__)
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--api-tier", default="premium",
        choices=("standard", "premium"),
    )
    parser.add_argument(
        "--skip-empty-yield", action="store_true",
        help=(
            "Pass through to run_daily_pull. Mirrors the production weekday "
            "default (skip cold yield_status cells)."
        ),
    )
    parser.add_argument(
        "--no-reduce", action="store_true",
        help=(
            "Skip the post-run catalog trim. setup_daily.finalize_yield_status "
            "may have appended new symbols; --no-reduce leaves them in place."
        ),
    )
    args = parser.parse_args(argv)

    if not CATALOG_DIR.exists():
        raise FileNotFoundError(
            f"Catalog dir not found at {CATALOG_DIR}; run init/historical first."
        )

    DAILY_DIR.mkdir(parents=True, exist_ok=True)

    pre_existing_dates = _list_date_dirs(DAILY_DIR)
    pre_existing_roots = [
        DAILY_DIR / d.isoformat() for d in pre_existing_dates
    ]
    pre_snap = _snapshot(pre_existing_roots)
    logger.info(
        f"Pre-existing daily folders: {[d.isoformat() for d in pre_existing_dates]} "
        f"({len(pre_snap)} files snapshotted)"
    )

    reset_av_call_count()

    logger.info(
        f"Running run_daily_pull(catalog={CATALOG_DIR}, daily={DAILY_DIR})"
    )
    asyncio.run(run_daily_pull(
        catalog_dir=CATALOG_DIR,
        daily_dir=DAILY_DIR,
        api_tier=args.api_tier,
        skip_empty_yield=args.skip_empty_yield,
    ))

    post_dates = _list_date_dirs(DAILY_DIR)
    new_dates = [d for d in post_dates if d not in pre_existing_dates]

    if not new_dates:
        logger.warning(
            "No new daily/<date>/ folder produced. This is expected when "
            "previous_date >= folder_date (no-op). Skipping new-folder "
            "and monitoring checks."
        )
    else:
        if len(new_dates) > 1:
            raise AssertionError(
                f"Expected at most one new daily folder, got {new_dates}"
            )
        folder_date = new_dates[0]
        day_root = DAILY_DIR / folder_date.isoformat()
        logger.info(f"New daily folder produced: {day_root}")
        _check_tree(day_root)

        # Build a monitoring report for this folder. setup_daily does NOT
        # do this itself (the cloud entrypoint does), so the int_test owns it.
        previous_path = _previous_report_path(DAILY_DIR, folder_date)
        run_report_and_persist(
            mode="daily",
            folder_date=folder_date,
            catalog_dir=CATALOG_DIR,
            folder_dir=day_root,
            previous_report_path=previous_path,
            api_call_count=get_av_call_count(),
        )
        _check_monitoring(day_root)

    # Verify pre-existing date folders weren't touched by the run.
    if pre_snap:
        post_snap = _snapshot(pre_existing_roots)
        added, removed, changed = _diff_snapshots(pre_snap, post_snap)
        if added or removed or changed:
            raise AssertionError(
                "Pre-existing daily folders were modified during this run.\n"
                f"  added:   {added[:10]}{'...' if len(added) > 10 else ''}\n"
                f"  removed: {removed[:10]}{'...' if len(removed) > 10 else ''}\n"
                f"  changed: {changed[:10]}{'...' if len(changed) > 10 else ''}"
            )
        logger.info(
            f"Pre-existing daily folders unchanged "
            f"({len(pre_snap)} files verified)."
        )

    if args.no_reduce:
        logger.info("--no-reduce passed: skipping catalog trim.")
    else:
        # setup_daily's finalize_yield_status may have appended new symbol
        # rows (rare, but possible if AV LISTING_STATUS shifted). Trim back
        # to the int-test set so the next run stays small.
        kept_stocks, kept_etfs = reduce_catalogs(
            CATALOG_DIR,
            historical_dir=HISTORICAL_DIR,
            daily_dir=DAILY_DIR,
        )
        logger.info(
            f"Reduced catalog post-run: {len(kept_stocks)} stocks, "
            f"{len(kept_etfs)} etfs"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
