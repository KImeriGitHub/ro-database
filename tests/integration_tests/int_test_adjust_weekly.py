"""Integration test: daily_data_service/adjust_weekly.py.

Reruns flagged ``(symbol, asset_type, endpoint)`` cells from the latest
daily folder's ingestion report. With a tiny int-test catalog the report
is usually empty, so the run will typically be a no-op (logged as
"Nothing to retry; exiting without changes."). This test still asserts
the catalog and latest daily folder are intact afterwards, then runs
the monitoring report in ``weekend`` mode and re-reduces the catalog.

Usage:
    python tests/integration_tests/int_test_adjust_weekly.py [--look-back-days 7]
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

from daily_data_service.adjust_weekly import adjust_weekly
from historical_data_setup._common import get_av_call_count, reset_av_call_count
from monitoring_service.report import (
    REPORT_FILENAME_JSON,
    REPORT_FILENAME_MD,
    run_report_and_persist,
)

from tests.integration_tests._helpers import (
    CATALOG_DIR,
    DAILY_DIR,
    configure_int_test_logging,
    reduce_catalogs,
)

logger = logging.getLogger(__name__)

_DATE_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _latest_date(daily_dir: Path) -> date:
    candidates: list[date] = []
    for child in daily_dir.iterdir():
        if not child.is_dir() or not _DATE_DIR_RE.match(child.name):
            continue
        try:
            candidates.append(date.fromisoformat(child.name))
        except ValueError:
            continue
    if not candidates:
        raise FileNotFoundError(
            f"No YYYY-MM-DD subdirectories under {daily_dir}; "
            f"run int_test_run_daily.py first."
        )
    return max(candidates)


def _previous_report_path(daily_dir: Path, folder_date: date) -> Path | None:
    candidates: list[date] = []
    for child in daily_dir.iterdir():
        if not child.is_dir() or not _DATE_DIR_RE.match(child.name):
            continue
        try:
            d = date.fromisoformat(child.name)
        except ValueError:
            continue
        if d < folder_date:
            candidates.append(d)
    if not candidates:
        return None
    prior = max(candidates)
    p = daily_dir / prior.isoformat() / REPORT_FILENAME_JSON
    return p if p.exists() else None


def main(argv: list[str] | None = None) -> int:
    configure_int_test_logging(__file__)
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--look-back-days", type=int, default=7,
        help="How many days back to look for previous_date (max useful: 7).",
    )
    parser.add_argument(
        "--api-tier", default="premium",
        choices=("standard", "premium"),
    )
    parser.add_argument(
        "--no-reduce", action="store_true",
        help=(
            "Skip the post-run catalog trim. adjust_weekly's "
            "finalize_yield_status may have appended new symbols; "
            "--no-reduce leaves them in place."
        ),
    )
    args = parser.parse_args(argv)

    if not CATALOG_DIR.exists():
        raise FileNotFoundError(
            f"Catalog dir not found at {CATALOG_DIR}; run init first."
        )
    if not DAILY_DIR.exists():
        raise FileNotFoundError(
            f"Daily dir not found at {DAILY_DIR}; run int_test_run_daily.py first."
        )

    folder_date = _latest_date(DAILY_DIR)
    day_root = DAILY_DIR / folder_date.isoformat()
    logger.info(f"adjust_weekly target folder: {day_root}")

    reset_av_call_count()

    asyncio.run(adjust_weekly(
        catalog_dir=CATALOG_DIR,
        daily_dir=DAILY_DIR,
        look_back_days=args.look_back_days,
        api_tier=args.api_tier,
    ))

    if not day_root.exists():
        raise AssertionError(
            f"Latest daily folder {day_root} disappeared after adjust_weekly."
        )
    if not (day_root / "ingestion_report.parquet").exists():
        raise AssertionError(
            f"ingestion_report.parquet missing at {day_root} "
            f"after adjust_weekly."
        )
    logger.info(f"Latest daily folder intact at {day_root}")

    previous_path = _previous_report_path(DAILY_DIR, folder_date)
    run_report_and_persist(
        mode="weekend",
        folder_date=folder_date,
        catalog_dir=CATALOG_DIR,
        folder_dir=day_root,
        previous_report_path=previous_path,
        api_call_count=get_av_call_count(),
    )
    missing = [
        fname for fname in (REPORT_FILENAME_JSON, REPORT_FILENAME_MD)
        if not (day_root / fname).exists()
    ]
    if missing:
        raise AssertionError(
            f"Missing monitoring report files in {day_root}: {missing}"
        )
    logger.info("Weekend monitoring report written.")

    if args.no_reduce:
        logger.info("--no-reduce passed: skipping catalog trim.")
    else:
        kept_stocks, kept_etfs = reduce_catalogs(CATALOG_DIR)
        logger.info(
            f"Reduced catalog post-run: {len(kept_stocks)} stocks, "
            f"{len(kept_etfs)} etfs"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
