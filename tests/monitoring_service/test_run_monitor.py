"""Unit tests for monitoring_service.run_monitor CLI helpers.

The CLI's load-bearing logic is folder-date resolution: in daily/weekend
modes it picks the lexicographically-greatest ``YYYY-MM-DD`` folder under
``--daily-dir`` (ignoring stray non-date directories), and in historical mode
it ignores ``--daily-dir`` entirely. ``main`` itself is a thin argparse wrapper
that delegates to ``run_report_and_persist``; we verify the delegation rather
than re-test the report builder here.
"""

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import monitoring_service.run_monitor as run_monitor
from monitoring_service.run_monitor import _latest_folder_date, _resolve_folder_dir


# ---------------------------------------------------------------------------
# _latest_folder_date
# ---------------------------------------------------------------------------

def test_latest_folder_date_missing_dir(tmp_path):
    assert _latest_folder_date(tmp_path / "nope") is None


def test_latest_folder_date_empty_dir(tmp_path):
    assert _latest_folder_date(tmp_path) is None


def test_latest_folder_date_picks_greatest(tmp_path):
    for name in ("2026-05-30", "2026-06-01", "2026-05-31"):
        (tmp_path / name).mkdir()
    assert _latest_folder_date(tmp_path) == date(2026, 6, 1)


def test_latest_folder_date_ignores_non_date_dirs_and_files(tmp_path):
    (tmp_path / "2026-05-30").mkdir()
    (tmp_path / "not-a-date").mkdir()
    (tmp_path / "logs").mkdir()
    (tmp_path / "2026-06-01.parquet").write_text("x", encoding="utf-8")  # a file
    assert _latest_folder_date(tmp_path) == date(2026, 5, 30)


# ---------------------------------------------------------------------------
# _resolve_folder_dir
# ---------------------------------------------------------------------------

def test_resolve_historical_ignores_daily_dir(tmp_path):
    hist = tmp_path / "historical"
    folder_dir, folder_date = _resolve_folder_dir(
        "historical", date(2026, 1, 1), tmp_path / "daily", hist
    )
    assert folder_dir == hist
    assert folder_date == date(2026, 1, 1)


def test_resolve_historical_defaults_date_to_today(tmp_path):
    hist = tmp_path / "historical"
    folder_dir, folder_date = _resolve_folder_dir(
        "historical", None, tmp_path / "daily", hist
    )
    assert folder_dir == hist
    assert folder_date == date.today()


def test_resolve_daily_with_explicit_date(tmp_path):
    daily = tmp_path / "daily"
    folder_dir, folder_date = _resolve_folder_dir(
        "daily", date(2026, 6, 1), daily, tmp_path / "historical"
    )
    assert folder_dir == daily / "2026-06-01"
    assert folder_date == date(2026, 6, 1)


def test_resolve_daily_defaults_to_latest_folder(tmp_path):
    daily = tmp_path / "daily"
    daily.mkdir()
    (daily / "2026-05-31").mkdir()
    (daily / "2026-06-02").mkdir()
    folder_dir, folder_date = _resolve_folder_dir(
        "daily", None, daily, tmp_path / "historical"
    )
    assert folder_date == date(2026, 6, 2)
    assert folder_dir == daily / "2026-06-02"


def test_resolve_daily_raises_when_no_folders(tmp_path):
    daily = tmp_path / "daily"
    daily.mkdir()
    with pytest.raises(SystemExit):
        _resolve_folder_dir("daily", None, daily, tmp_path / "historical")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def test_main_delegates_to_run_report_and_persist(tmp_path, monkeypatch):
    daily = tmp_path / "daily"
    (daily / "2026-06-01").mkdir(parents=True)
    catalog = tmp_path / "catalog"

    captured = {}

    def _fake_persist(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(run_monitor, "run_report_and_persist", _fake_persist)
    monkeypatch.setattr(run_monitor, "configure_logging", lambda: None)
    monkeypatch.setattr(sys, "argv", [
        "run_monitor",
        "--mode", "daily",
        "--folder-date", "2026-06-01",
        "--catalog-dir", str(catalog),
        "--daily-dir", str(daily),
    ])

    assert run_monitor.main() == 0
    assert captured["mode"] == "daily"
    assert captured["folder_date"] == date(2026, 6, 1)
    assert captured["folder_dir"] == daily / "2026-06-01"
    assert captured["catalog_dir"] == catalog
