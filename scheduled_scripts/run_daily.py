"""Container entrypoint for the daily Alpha Vantage pull.

Runs inside the Cloud Run job invoked by Cloud Scheduler. Steps:

1. Download ``catalog/`` from GCS to a temp workdir.
2. Run :func:`asset_catalog_service.update_catalog.update_all` against that
   workdir so catalog metadata and yield_status are refreshed before the pull.
3. Execute :func:`daily_data_service.setup_daily.run_daily_pull` writing to
   that workdir's ``daily/YYYY-MM-DD/`` folder.
4. Upload the newly-written daily folder and the updated catalog back to GCS.
5. Exit non-zero on failure so Cloud Scheduler/Cloud Run retries cleanly.

The workdir path can be overridden with ``--workdir`` for local smoke tests
against the real bucket.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import tempfile
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from asset_catalog_service.update_catalog import update_all as update_catalog_all
from config.gcp import GCS_BUCKET
from daily_data_service.setup_daily import run_daily_pull
from daily_data_service._common import resolve_start_marker
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


def _pull_catalog(workdir: Path) -> Path:
    catalog_local = workdir / "catalog"
    logger.info(f"Pulling catalog/ to {catalog_local}")
    gcs_client.download_tree(gcs_catalog_prefix(), catalog_local)
    return catalog_local


def _push_daily_folder(daily_local: Path, folder_date: date) -> None:
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


def _try_pull_previous_monitoring_report(
    daily_local: Path, folder_date: date,
) -> Path | None:
    """Look one folder-date back in GCS for the prior monitoring_report.json.

    Best-effort: if the prior folder doesn't exist or the blob is absent the
    monitor just records ``previous_available=False`` and moves on.
    """
    daily_prefix = gcs_daily_prefix()
    prior_dates: list[date] = []
    for info in gcs_client.list_blobs(f"{daily_prefix}/"):
        rel = info.name[len(daily_prefix) + 1:]
        head = rel.split("/", 1)[0] if "/" in rel else ""
        try:
            d = date.fromisoformat(head)
        except ValueError:
            continue
        if d < folder_date:
            prior_dates.append(d)
    if not prior_dates:
        return None
    prior = max(prior_dates)
    blob_name = f"{gcs_daily_prefix(prior)}/{REPORT_FILENAME_JSON}"
    if not gcs_client.blob_exists(blob_name):
        return None
    local = daily_local / prior.isoformat() / REPORT_FILENAME_JSON
    gcs_client.download_file(blob_name, local)
    logger.info(f"Pulled previous monitoring report from {blob_name}")
    return local


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

    previous_path = _try_pull_previous_monitoring_report(daily_local, folder_date)

    try:
        run_and_persist(
            mode="daily",
            folder_date=folder_date,
            catalog_dir=catalog_local,
            folder_dir=folder_dir,
            previous_report_path=previous_path,
            api_call_count=api_call_count,
        )
    except Exception:
        logger.exception("Monitoring report failed; pull is unaffected.")
        return

    prefix = gcs_daily_prefix(folder_date)
    for fname in (REPORT_FILENAME_JSON, REPORT_FILENAME_MD):
        local_path = folder_dir / fname
        if local_path.exists():
            gcs_client.upload_file(local_path, f"{prefix}/{fname}")


async def _run(workdir: Path, api_tier: str) -> int:
    catalog_local = _pull_catalog(workdir)
    daily_local = workdir / "daily"
    daily_local.mkdir(parents=True, exist_ok=True)

    reset_av_call_count()

    try:
        update_catalog_all(catalog_local)
    except Exception:
        logger.exception("update_catalog_all failed")
        return 1

    try:
        await run_daily_pull(
            catalog_dir=catalog_local,
            daily_dir=daily_local,
            api_tier=api_tier,
            skip_empty_yield=True,
        )
    except Exception:
        logger.exception("run_daily_pull failed")
        return 1

    # resolve_start_marker is idempotent: if the marker was already unlinked
    # by a full-run finalize it recreates one just to compute today's date
    # for the upload step. We delete it again afterwards so the next run
    # starts fresh.
    _, folder_date, marker = resolve_start_marker(daily_local)
    marker.unlink(missing_ok=True)

    api_calls_used = get_av_call_count()
    _build_and_push_monitoring_report(
        catalog_local, daily_local, folder_date, api_calls_used,
    )

    _push_daily_folder(daily_local, folder_date)
    _push_catalog(catalog_local)
    return 0


def main() -> int:
    configure_logging()
    parser = argparse.ArgumentParser(description="Daily ingest entrypoint")
    parser.add_argument("--workdir", type=Path, default=None,
                        help="Working directory (default: a tempdir)")
    parser.add_argument("--api-tier", default="premium", choices=("standard", "premium"))
    args = parser.parse_args()

    if args.workdir is not None:
        args.workdir.mkdir(parents=True, exist_ok=True)
        return asyncio.run(_run(args.workdir, args.api_tier))

    with tempfile.TemporaryDirectory(prefix="ro-daily-") as tmp:
        return asyncio.run(_run(Path(tmp), args.api_tier))


if __name__ == "__main__":
    sys.exit(main())
