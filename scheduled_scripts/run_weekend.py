"""Container entrypoint for the weekly Alpha Vantage adjustment pass.

Runs inside the Cloud Run job invoked by Cloud Scheduler on Saturday evening.
Steps:

1. Download ``catalog/`` from GCS to a temp workdir.
2. List the ``daily/`` prefix in GCS to discover every ``YYYY-MM-DD`` folder
   name; materialise an empty local subdirectory for each so
   :func:`daily_data_service.adjust_weekly.resolve_dates` can scan them.
3. Download only the most recent ``daily/<folder_date>/`` folder from GCS
   (that's the folder whose ingestion report we're working from).
4. Execute :func:`daily_data_service.adjust_weekly.adjust_weekly` against the
   workdir. ``update_catalog_all`` is deliberately NOT run.
5. Upload the extended ``daily/<folder_date>/`` folder and the refreshed
   ``catalog/`` back to GCS.
6. Exit non-zero on failure so Cloud Scheduler/Cloud Run retries cleanly.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import re
import sys
import tempfile
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.gcp import GCS_BUCKET
from daily_data_service.adjust_weekly import adjust_weekly
from historical_data_setup._common import get_av_call_count, reset_av_call_count
from maintainance_scripts import gcs_client
from maintainance_scripts.logging_setup import configure_logging
from maintainance_scripts.paths import (
    gcs_catalog_prefix,
    gcs_daily_prefix,
)
from monitoring_service.report import (
    REPORT_FILENAME_JSON,
    REPORT_FILENAME_MD,
    run_and_persist,
)

logger = logging.getLogger(__name__)

_DATE_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _pull_catalog(workdir: Path) -> Path:
    catalog_local = workdir / "catalog"
    logger.info(f"Pulling catalog/ to {catalog_local}")
    gcs_client.download_tree(gcs_catalog_prefix(), catalog_local)
    return catalog_local


def _discover_remote_folder_dates() -> list[date]:
    """Return every ``YYYY-MM-DD`` folder under the GCS ``daily/`` prefix."""
    daily_prefix = gcs_daily_prefix()
    names: set[str] = set()
    for info in gcs_client.list_blobs(f"{daily_prefix}/"):
        rel = info.name[len(daily_prefix) + 1:]
        if not rel or "/" not in rel:
            continue
        head = rel.split("/", 1)[0]
        if _DATE_DIR_RE.match(head):
            names.add(head)
    out: list[date] = []
    for n in names:
        try:
            out.append(date.fromisoformat(n))
        except ValueError:
            continue
    return sorted(out)


def _stub_local_folders(daily_local: Path, folder_dates: list[date]) -> None:
    """Create empty ``daily/<YYYY-MM-DD>/`` directories so the local folder
    scan in ``adjust_weekly`` sees every remote folder."""
    daily_local.mkdir(parents=True, exist_ok=True)
    for d in folder_dates:
        (daily_local / d.isoformat()).mkdir(parents=True, exist_ok=True)


def _pull_folder(daily_local: Path, folder_date: date) -> None:
    prefix = gcs_daily_prefix(folder_date)
    dest = daily_local / folder_date.isoformat()
    logger.info(f"Pulling {prefix}/ to {dest}")
    gcs_client.download_tree(prefix, dest)


def _push_folder(daily_local: Path, folder_date: date) -> None:
    prefix = gcs_daily_prefix(folder_date)
    date_dir = daily_local / folder_date.isoformat()
    if not date_dir.exists():
        logger.info(f"No daily output at {date_dir}; nothing to upload.")
        return
    logger.info(f"Uploading {date_dir} to gs://{GCS_BUCKET}/{prefix}/")
    gcs_client.upload_tree(date_dir, prefix)


def _push_catalog(catalog_local: Path) -> None:
    logger.info(f"Uploading updated catalog/ from {catalog_local}")
    gcs_client.upload_tree(catalog_local, gcs_catalog_prefix())


def _build_and_push_monitoring_report(
    catalog_local: Path,
    daily_local: Path,
    folder_date: date,
    api_call_count: int,
) -> None:
    folder_dir = daily_local / folder_date.isoformat()
    if not folder_dir.exists():
        logger.info(f"No daily output at {folder_dir}; skipping monitoring report.")
        return

    # The previous report for a weekend run is the daily monitoring_report
    # written into the same folder before adjust_weekly ran (uploaded by
    # run_daily.py). Pull it from GCS to a sibling path so the diff captures
    # what the weekend pass changed.
    previous_path: Path | None = None
    blob_name = f"{gcs_daily_prefix(folder_date)}/{REPORT_FILENAME_JSON}"
    if gcs_client.blob_exists(blob_name):
        previous_path = folder_dir / "monitoring_report.previous.json"
        gcs_client.download_file(blob_name, previous_path)
        logger.info(f"Pulled previous (pre-weekend) monitoring report from {blob_name}")

    try:
        run_and_persist(
            mode="weekend",
            folder_date=folder_date,
            catalog_dir=catalog_local,
            folder_dir=folder_dir,
            previous_report_path=previous_path,
            api_call_count=api_call_count,
        )
    except Exception:
        logger.exception("Monitoring report failed; weekend pull is unaffected.")
        return

    prefix = gcs_daily_prefix(folder_date)
    for fname in (REPORT_FILENAME_JSON, REPORT_FILENAME_MD):
        local_path = folder_dir / fname
        if local_path.exists():
            gcs_client.upload_file(local_path, f"{prefix}/{fname}")


async def _run(workdir: Path, look_back_days: int, api_tier: str) -> int:
    catalog_local = _pull_catalog(workdir)
    daily_local = workdir / "daily"

    folder_dates = _discover_remote_folder_dates()
    if not folder_dates:
        logger.error("No YYYY-MM-DD folders found under daily/ in GCS; aborting.")
        return 1

    _stub_local_folders(daily_local, folder_dates)
    folder_date = folder_dates[-1]
    _pull_folder(daily_local, folder_date)

    reset_av_call_count()

    try:
        await adjust_weekly(
            catalog_dir=catalog_local,
            daily_dir=daily_local,
            look_back_days=look_back_days,
            api_tier=api_tier,
        )
    except Exception:
        logger.exception("adjust_weekly failed")
        return 1

    api_calls_used = get_av_call_count()
    _build_and_push_monitoring_report(
        catalog_local, daily_local, folder_date, api_calls_used,
    )

    _push_folder(daily_local, folder_date)
    _push_catalog(catalog_local)
    return 0


def main() -> int:
    configure_logging()
    parser = argparse.ArgumentParser(description="Weekly adjustment entrypoint")
    parser.add_argument(
        "--workdir", type=Path, default=None,
        help="Working directory (default: a tempdir)",
    )
    parser.add_argument("--look-back-days", type=int, default=7)
    parser.add_argument(
        "--api-tier", default="premium", choices=("standard", "premium"),
    )
    args = parser.parse_args()

    if args.workdir is not None:
        args.workdir.mkdir(parents=True, exist_ok=True)
        return asyncio.run(_run(args.workdir, args.look_back_days, args.api_tier))

    with tempfile.TemporaryDirectory(prefix="ro-weekly-") as tmp:
        return asyncio.run(_run(Path(tmp), args.look_back_days, args.api_tier))


if __name__ == "__main__":
    sys.exit(main())
