"""Unit tests for scheduled_scripts.run_daily helpers.

The async ``_run`` orchestrator is glue over four well-tested services; the
parts with their own logic are the GCS-facing helpers: skipping the upload
when no daily folder was produced, locating the previous day's monitoring
report in the bucket (pick the greatest date strictly before today), and
wiring the monitoring report into both ``run_report_and_persist`` and the
follow-up blob uploads. Those are what these tests pin.
"""

import asyncio
import sys
from datetime import date, datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import scheduled_scripts.run_daily as mod
from monitoring_service.report import REPORT_FILENAME_JSON, REPORT_FILENAME_MD


def _blob(name: str):
    return mod.gcs_client.BlobInfo(name=name, size=0, md5_hash=None, updated_iso=None)


@pytest.fixture
def gcs_prefixes(monkeypatch):
    monkeypatch.setattr(mod, "gcs_daily_prefix",
                        lambda d=None: "daily" if d is None else f"daily/{d.isoformat()}")
    monkeypatch.setattr(mod, "gcs_catalog_prefix", lambda: "catalog")


# ---------------------------------------------------------------------------
# _push_daily_folder
# ---------------------------------------------------------------------------

def test_push_daily_folder_skips_when_missing(tmp_path, gcs_prefixes, monkeypatch):
    uploaded = []
    monkeypatch.setattr(mod.gcs_client, "upload_tree",
                        lambda *a, **k: uploaded.append(a))
    daily_local = tmp_path / "daily"
    daily_local.mkdir()
    mod._push_daily_folder(daily_local, date(2026, 6, 1), workers=1)
    assert uploaded == []


def test_push_daily_folder_uploads_when_present(tmp_path, gcs_prefixes, monkeypatch):
    uploaded = []
    monkeypatch.setattr(mod.gcs_client, "upload_tree",
                        lambda local, prefix, workers=2: uploaded.append((Path(local), prefix)))
    daily_local = tmp_path / "daily"
    (daily_local / "2026-06-01").mkdir(parents=True)
    mod._push_daily_folder(daily_local, date(2026, 6, 1), workers=1)
    assert uploaded == [(daily_local / "2026-06-01", "daily/2026-06-01")]


# ---------------------------------------------------------------------------
# _try_pull_previous_monitoring_report
# ---------------------------------------------------------------------------

def test_previous_report_none_when_no_prior_dates(tmp_path, gcs_prefixes, monkeypatch):
    # Only today's folder exists in the bucket -> nothing strictly before it.
    monkeypatch.setattr(mod.gcs_client, "list_blobs",
                        lambda prefix: iter([_blob("daily/2026-06-01/x.parquet")]))
    out = mod._try_pull_previous_monitoring_report(tmp_path, date(2026, 6, 1))
    assert out is None


def test_previous_report_picks_greatest_prior_date(tmp_path, gcs_prefixes, monkeypatch):
    monkeypatch.setattr(mod.gcs_client, "list_blobs", lambda prefix: iter([
        _blob("daily/2026-05-28/x.parquet"),
        _blob("daily/2026-05-31/x.parquet"),
        _blob("daily/2026-06-01/x.parquet"),  # == folder_date, excluded
    ]))
    monkeypatch.setattr(mod.gcs_client, "blob_exists", lambda name: True)
    downloaded = {}
    monkeypatch.setattr(mod.gcs_client, "download_file",
                        lambda blob, local: downloaded.update(blob=blob, local=Path(local)))
    out = mod._try_pull_previous_monitoring_report(tmp_path, date(2026, 6, 1))
    # Greatest prior date is 2026-05-31.
    assert downloaded["blob"] == f"daily/2026-05-31/{REPORT_FILENAME_JSON}"
    assert out == tmp_path / "2026-05-31" / REPORT_FILENAME_JSON


def test_previous_report_none_when_blob_absent(tmp_path, gcs_prefixes, monkeypatch):
    monkeypatch.setattr(mod.gcs_client, "list_blobs",
                        lambda prefix: iter([_blob("daily/2026-05-31/x.parquet")]))
    monkeypatch.setattr(mod.gcs_client, "blob_exists", lambda name: False)
    out = mod._try_pull_previous_monitoring_report(tmp_path, date(2026, 6, 1))
    assert out is None


# ---------------------------------------------------------------------------
# _build_and_push_monitoring_report
# ---------------------------------------------------------------------------

def test_build_report_skips_when_folder_absent(tmp_path, gcs_prefixes, monkeypatch):
    called = []
    monkeypatch.setattr(mod, "run_report_and_persist",
                        lambda **k: called.append(k))
    mod._build_and_push_monitoring_report(
        tmp_path / "catalog", tmp_path / "daily", date(2026, 6, 1), api_call_count=1
    )
    assert called == []


def test_build_report_runs_and_uploads(tmp_path, gcs_prefixes, monkeypatch):
    catalog_local = tmp_path / "catalog"
    daily_local = tmp_path / "daily"
    folder_dir = daily_local / "2026-06-01"
    folder_dir.mkdir(parents=True)

    # No prior report in the bucket.
    monkeypatch.setattr(mod.gcs_client, "list_blobs", lambda prefix: iter([]))

    persist_kwargs = {}

    def _fake_persist(**kwargs):
        persist_kwargs.update(kwargs)
        # Emulate the report writer dropping both artifacts.
        (folder_dir / REPORT_FILENAME_JSON).write_text("{}", encoding="utf-8")
        (folder_dir / REPORT_FILENAME_MD).write_text("# r", encoding="utf-8")

    monkeypatch.setattr(mod, "run_report_and_persist", _fake_persist)

    uploaded = []
    monkeypatch.setattr(mod.gcs_client, "upload_file",
                        lambda local, blob: uploaded.append((Path(local), blob)))

    mod._build_and_push_monitoring_report(
        catalog_local, daily_local, date(2026, 6, 1), api_call_count=4242
    )

    assert persist_kwargs["mode"] == "daily"
    assert persist_kwargs["api_call_count"] == 4242
    assert persist_kwargs["folder_date"] == date(2026, 6, 1)
    blobs = {b for _, b in uploaded}
    assert f"daily/2026-06-01/{REPORT_FILENAME_JSON}" in blobs
    assert f"daily/2026-06-01/{REPORT_FILENAME_MD}" in blobs


# ---------------------------------------------------------------------------
# _run: phase-boundary partial upload (Option A)
# ---------------------------------------------------------------------------

def test_run_pushes_daily_folder_at_phase_boundary_and_at_end(tmp_path, gcs_prefixes, monkeypatch):
    """``_run`` wires an ``on_phase_complete`` callback that pushes the partial
    daily folder when the non-financial phase finishes, in addition to the
    final push after the whole pull. So ``_push_daily_folder`` runs twice."""
    workdir = tmp_path
    (workdir / "daily").mkdir()
    folder_date = date(2026, 6, 1)

    monkeypatch.setattr(mod, "_pull_catalog", lambda wd, w: wd / "catalog")
    monkeypatch.setattr(mod, "update_catalog_all", lambda c: None)
    monkeypatch.setattr(mod, "_build_and_push_monitoring_report", lambda *a, **k: None)
    monkeypatch.setattr(mod, "_push_catalog", lambda *a, **k: None)

    pushes: list = []
    monkeypatch.setattr(
        mod, "_push_daily_folder",
        lambda daily_local, fd, workers: pushes.append((Path(daily_local), fd, workers)),
    )

    async def fake_pull(*, on_phase_complete=None, **kwargs):
        # Emulate the orchestrator firing the phase-1 callback mid-pull.
        if on_phase_complete is not None:
            await on_phase_complete("non_financial", folder_date)
        return datetime(2026, 6, 1, 21, 0), folder_date

    monkeypatch.setattr(mod, "run_daily_pull", fake_pull)

    rc = asyncio.run(mod._run(workdir, api_tier="premium", workers=3))

    assert rc == 0
    # Partial push (from the callback) then the final push, same args both times.
    assert pushes == [
        (workdir / "daily", folder_date, 3),
        (workdir / "daily", folder_date, 3),
    ]


def test_build_report_swallows_persist_failure(tmp_path, gcs_prefixes, monkeypatch):
    folder_dir = tmp_path / "daily" / "2026-06-01"
    folder_dir.mkdir(parents=True)
    monkeypatch.setattr(mod.gcs_client, "list_blobs", lambda prefix: iter([]))

    def _boom(**k):
        raise RuntimeError("report build failed")

    monkeypatch.setattr(mod, "run_report_and_persist", _boom)
    uploaded = []
    monkeypatch.setattr(mod.gcs_client, "upload_file",
                        lambda local, blob: uploaded.append(blob))

    # Must not raise; the pull is unaffected by a monitoring failure.
    mod._build_and_push_monitoring_report(
        tmp_path / "catalog", tmp_path / "daily", date(2026, 6, 1), api_call_count=1
    )
    assert uploaded == []
