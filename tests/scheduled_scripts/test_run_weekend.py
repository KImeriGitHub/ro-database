"""Unit tests for scheduled_scripts.run_weekend helpers.

The weekend pass discovers every ``YYYY-MM-DD`` folder already in the bucket
(so ``adjust_weekly``'s local scan sees the full history), stubs empty local
dirs for each, pulls only the newest folder, and -- after adjusting -- writes
a monitoring report whose "previous" is the pre-weekend report sitting in the
same folder. These tests cover discovery/stub/pull/push and the report wiring;
the async ``_run`` glue itself is left to integration coverage.
"""

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import scheduled_scripts.run_weekend as mod
from monitoring_service.report import REPORT_FILENAME_JSON, REPORT_FILENAME_MD


def _blob(name: str):
    return mod.gcs_client.BlobInfo(name=name, size=0, md5_hash=None, updated_iso=None)


@pytest.fixture
def gcs_prefixes(monkeypatch):
    monkeypatch.setattr(mod, "gcs_daily_prefix",
                        lambda d=None: "daily" if d is None else f"daily/{d.isoformat()}")
    monkeypatch.setattr(mod, "gcs_catalog_prefix", lambda: "catalog")


# ---------------------------------------------------------------------------
# _discover_remote_folder_dates
# ---------------------------------------------------------------------------

def test_discover_dedups_and_sorts(gcs_prefixes, monkeypatch):
    monkeypatch.setattr(mod.gcs_client, "list_blobs", lambda prefix: iter([
        _blob("daily/2026-06-02/stocks/prices/AAPL.parquet"),
        _blob("daily/2026-06-01/stocks/prices/AAPL.parquet"),
        _blob("daily/2026-06-02/stocks/prices/MSFT.parquet"),  # dup date
    ]))
    out = mod._discover_remote_folder_dates()
    assert out == [date(2026, 6, 1), date(2026, 6, 2)]


def test_discover_ignores_non_date_and_bare_prefix(gcs_prefixes, monkeypatch):
    monkeypatch.setattr(mod.gcs_client, "list_blobs", lambda prefix: iter([
        _blob("daily/2026-06-01/stocks/x.parquet"),
        _blob("daily/not-a-date/x.parquet"),
        _blob("daily/README.md"),         # no nested slash -> skipped
    ]))
    out = mod._discover_remote_folder_dates()
    assert out == [date(2026, 6, 1)]


# ---------------------------------------------------------------------------
# _stub_local_folders
# ---------------------------------------------------------------------------

def test_stub_local_folders_creates_one_dir_per_date(tmp_path):
    daily_local = tmp_path / "daily"
    mod._stub_local_folders(daily_local, [date(2026, 6, 1), date(2026, 6, 2)])
    assert (daily_local / "2026-06-01").is_dir()
    assert (daily_local / "2026-06-02").is_dir()


# ---------------------------------------------------------------------------
# _pull_folder / _push_folder
# ---------------------------------------------------------------------------

def test_pull_folder_downloads_to_dated_dest(tmp_path, gcs_prefixes, monkeypatch):
    calls = []
    monkeypatch.setattr(mod.gcs_client, "download_tree",
                        lambda prefix, dest, workers=2: calls.append((prefix, Path(dest))))
    daily_local = tmp_path / "daily"
    mod._pull_folder(daily_local, date(2026, 6, 1), workers=1)
    assert calls == [("daily/2026-06-01", daily_local / "2026-06-01")]


def test_push_folder_skips_when_missing(tmp_path, gcs_prefixes, monkeypatch):
    uploaded = []
    monkeypatch.setattr(mod.gcs_client, "upload_tree",
                        lambda *a, **k: uploaded.append(a))
    daily_local = tmp_path / "daily"
    daily_local.mkdir()
    mod._push_folder(daily_local, date(2026, 6, 1), workers=1)
    assert uploaded == []


def test_push_folder_uploads_when_present(tmp_path, gcs_prefixes, monkeypatch):
    uploaded = []
    monkeypatch.setattr(mod.gcs_client, "upload_tree",
                        lambda local, prefix, workers=2: uploaded.append((Path(local), prefix)))
    daily_local = tmp_path / "daily"
    (daily_local / "2026-06-01").mkdir(parents=True)
    mod._push_folder(daily_local, date(2026, 6, 1), workers=1)
    assert uploaded == [(daily_local / "2026-06-01", "daily/2026-06-01")]


# ---------------------------------------------------------------------------
# _build_and_push_monitoring_report
# ---------------------------------------------------------------------------

def test_build_report_pulls_preweekend_report_as_previous(tmp_path, gcs_prefixes, monkeypatch):
    catalog_local = tmp_path / "catalog"
    daily_local = tmp_path / "daily"
    folder_dir = daily_local / "2026-06-01"
    folder_dir.mkdir(parents=True)

    # The pre-weekend daily report exists in the bucket.
    monkeypatch.setattr(mod.gcs_client, "blob_exists", lambda name: True)
    downloaded = {}
    monkeypatch.setattr(mod.gcs_client, "download_file",
                        lambda blob, local: downloaded.update(blob=blob, local=Path(local)))

    persist_kwargs = {}

    def _fake_persist(**kwargs):
        persist_kwargs.update(kwargs)
        (folder_dir / REPORT_FILENAME_JSON).write_text("{}", encoding="utf-8")
        (folder_dir / REPORT_FILENAME_MD).write_text("# r", encoding="utf-8")

    monkeypatch.setattr(mod, "run_report_and_persist", _fake_persist)
    uploaded = []
    monkeypatch.setattr(mod.gcs_client, "upload_file",
                        lambda local, blob: uploaded.append(blob))

    mod._build_and_push_monitoring_report(
        catalog_local, daily_local, date(2026, 6, 1), api_call_count=7
    )

    assert downloaded["blob"] == f"daily/2026-06-01/{REPORT_FILENAME_JSON}"
    assert persist_kwargs["mode"] == "weekend"
    assert persist_kwargs["previous_report_path"] == folder_dir / "monitoring_report.previous.json"
    assert f"daily/2026-06-01/{REPORT_FILENAME_JSON}" in uploaded


def test_build_report_no_previous_when_blob_absent(tmp_path, gcs_prefixes, monkeypatch):
    folder_dir = tmp_path / "daily" / "2026-06-01"
    folder_dir.mkdir(parents=True)
    monkeypatch.setattr(mod.gcs_client, "blob_exists", lambda name: False)

    persist_kwargs = {}
    monkeypatch.setattr(mod, "run_report_and_persist",
                        lambda **k: persist_kwargs.update(k))
    monkeypatch.setattr(mod.gcs_client, "upload_file", lambda local, blob: None)

    mod._build_and_push_monitoring_report(
        tmp_path / "catalog", tmp_path / "daily", date(2026, 6, 1), api_call_count=0
    )
    assert persist_kwargs["previous_report_path"] is None


def test_build_report_skips_when_folder_absent(tmp_path, gcs_prefixes, monkeypatch):
    called = []
    monkeypatch.setattr(mod, "run_report_and_persist", lambda **k: called.append(k))
    mod._build_and_push_monitoring_report(
        tmp_path / "catalog", tmp_path / "daily", date(2026, 6, 1), api_call_count=0
    )
    assert called == []
