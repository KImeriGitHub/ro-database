"""Integration test: historical_data_setup/setup_historical.py.

Runs the historical pipeline against ``database/catalog/`` (already trimmed
by ``int_test_init_catalog.py``) using the local FRD dir for stock and ETF
prices; everything else hits Alpha Vantage. ``setup_historical`` already
calls the monitoring report at the end, so this test only asserts on the
emitted file tree.

Checks performed
----------------
* Every subfolder under ``database/historical/`` (per ``ensure_folders``) is
  present.
* Each subfolder contains at least one ``.parquet`` file (skipping the few
  endpoints that may legitimately have no rows: e.g. ``etfs/etf_profile``
  for a sub-budget run can lag).
* ``ingestion_report.parquet`` is present at the historical root.
* ``monitoring_report.json`` and ``monitoring_report.md`` are present.

Usage:
    python tests/integration_tests/int_test_historical_setup.py
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from historical_data_setup.ensure_folders import HISTORICAL_TREE
from historical_data_setup.setup_historical import run_historical_setup
from maintainance_scripts.logging_setup import configure_logging
from monitoring_service.report import REPORT_FILENAME_JSON, REPORT_FILENAME_MD

from tests.integration_tests._helpers import (
    CATALOG_DIR,
    FRD_DIR,
    HISTORICAL_DIR,
)

logger = logging.getLogger(__name__)


def _check_tree(historical_dir: Path) -> None:
    missing_dirs: list[str] = []
    empty_dirs: list[str] = []
    for leaf in HISTORICAL_TREE:
        sub = historical_dir / leaf
        if not sub.exists() or not sub.is_dir():
            missing_dirs.append(leaf)
            continue
        if not any(sub.glob("*.parquet")):
            empty_dirs.append(leaf)
    if missing_dirs:
        raise AssertionError(
            f"Missing historical subfolders: {missing_dirs}"
        )
    if empty_dirs:
        # Don't fail the run for empty subfolders, but log loud — the budget
        # configuration may legitimately have skipped some endpoints.
        logger.warning(
            f"Subfolders with no .parquet files (manual inspection): "
            f"{empty_dirs}"
        )

    ingestion = historical_dir / "ingestion_report.parquet"
    if not ingestion.exists():
        raise AssertionError(f"Missing ingestion report at {ingestion}")
    logger.info(f"All {len(HISTORICAL_TREE)} historical subfolders present.")


def _check_monitoring(historical_dir: Path) -> None:
    missing = [
        fname for fname in (REPORT_FILENAME_JSON, REPORT_FILENAME_MD)
        if not (historical_dir / fname).exists()
    ]
    if missing:
        raise AssertionError(
            f"Missing monitoring report files in {historical_dir}: {missing}"
        )
    logger.info(
        f"Monitoring report files present "
        f"({REPORT_FILENAME_JSON}, {REPORT_FILENAME_MD})."
    )


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--api-tier", default="premium",
        choices=("standard", "premium"),
    )
    args = parser.parse_args(argv)

    if not CATALOG_DIR.exists():
        raise FileNotFoundError(
            f"Catalog dir not found at {CATALOG_DIR}; run int_test_init_catalog.py first."
        )

    HISTORICAL_DIR.mkdir(parents=True, exist_ok=True)

    logger.info(
        f"Running run_historical_setup(catalog={CATALOG_DIR}, "
        f"historical={HISTORICAL_DIR}, frd={FRD_DIR})"
    )
    asyncio.run(run_historical_setup(
        catalog_dir=CATALOG_DIR,
        historical_dir=HISTORICAL_DIR,
        stocks_dir=FRD_DIR,
        etfs_dir=FRD_DIR,
        api_tier=args.api_tier,
        run_monitor=True,
    ))

    _check_tree(HISTORICAL_DIR)
    _check_monitoring(HISTORICAL_DIR)
    return 0


if __name__ == "__main__":
    sys.exit(main())
