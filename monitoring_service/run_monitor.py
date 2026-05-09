"""CLI wrapper around :func:`monitoring_service.report.run_report_and_persist`.

Usage:
    python -m monitoring_service.run_monitor
        [--mode {daily,weekend,historical}]
        [--folder-date YYYY-MM-DD]
        [--catalog-dir PATH]
        [--daily-dir PATH]
        [--historical-dir PATH]
        [--previous-report PATH]

Defaults:
    --mode daily
    --folder-date    -> the lexicographically-greatest YYYY-MM-DD subdirectory
                        under --daily-dir (only consulted in daily/weekend modes)
    --catalog-dir    -> <project>/catalog
    --daily-dir      -> <project>/daily
    --historical-dir -> <project>/historical

The CLI cannot read the AV-call counter (a fresh process always sees zero),
so ``api_calls.total_calls_made`` will be ``null`` when invoked this way.
Orchestrators (``run_daily.py``, ``run_weekend.py``, ``setup_historical.py``)
read the counter from the live process before invoking the monitor and pass
it in via the Python API.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from maintainance_scripts.logging_setup import configure_logging
from monitoring_service.report import run_report_and_persist

logger = logging.getLogger(__name__)

_DATE_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _latest_folder_date(daily_dir: Path) -> date | None:
    if not daily_dir.exists():
        return None
    candidates: list[date] = []
    for child in daily_dir.iterdir():
        if child.is_dir() and _DATE_DIR_RE.match(child.name):
            try:
                candidates.append(date.fromisoformat(child.name))
            except ValueError:
                continue
    return max(candidates) if candidates else None


def _resolve_folder_dir(
    mode: str,
    folder_date: date | None,
    daily_dir: Path,
    historical_dir: Path,
) -> tuple[Path, date]:
    if mode == "historical":
        return historical_dir, folder_date or date.today()

    if folder_date is None:
        folder_date = _latest_folder_date(daily_dir)
        if folder_date is None:
            raise SystemExit(
                f"No YYYY-MM-DD folders under {daily_dir}; pass --folder-date."
            )
    return daily_dir / folder_date.isoformat(), folder_date


def main() -> int:
    configure_logging()
    project_root = Path(__file__).resolve().parent.parent

    parser = argparse.ArgumentParser(description="End-of-run monitoring report")
    parser.add_argument(
        "--mode", default="daily",
        choices=("daily", "weekend", "historical"),
    )
    parser.add_argument(
        "--folder-date", type=date.fromisoformat, default=None,
        help="YYYY-MM-DD; defaults to the latest folder under --daily-dir.",
    )
    parser.add_argument(
        "--catalog-dir", type=Path, default=project_root / "catalog",
    )
    parser.add_argument(
        "--daily-dir", type=Path, default=project_root / "daily",
    )
    parser.add_argument(
        "--historical-dir", type=Path, default=project_root / "historical",
    )
    parser.add_argument(
        "--previous-report", type=Path, default=None,
        help="Path to the previous monitoring_report.json for delta computation.",
    )
    args = parser.parse_args()

    folder_dir, folder_date = _resolve_folder_dir(
        args.mode, args.folder_date, args.daily_dir, args.historical_dir,
    )

    run_report_and_persist(
        mode=args.mode,
        folder_date=folder_date,
        catalog_dir=args.catalog_dir,
        folder_dir=folder_dir,
        previous_report_path=args.previous_report,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
