"""Unit tests for monitoring_service.report.

``build_report`` is exercised with the five analyzers monkeypatched in the
module namespace, so this file pins the *assembly* contract (which keys land
where, that the delta is computed, that ``api_call_count`` flows into
``api_calls.total_calls_made``) without depending on real parquet inputs.
Rendering and persistence are tested against a hand-built report dict so the
Markdown layout and the JSON/MD round-trip are covered independently.
"""

import json
import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import monitoring_service.report as report_module
from monitoring_service.report import (
    REPORT_FILENAME_JSON,
    REPORT_FILENAME_MD,
    build_report,
    log_summary,
    render_markdown,
    run_report_and_persist,
    write_report,
)


def _sample_report() -> dict:
    """A fully-populated report dict matching the analyzers' output shape."""
    return {
        "mode": "daily",
        "folder_date": "2026-06-01",
        "generated_at": "2026-06-01T20:00:00+00:00",
        "catalog": {
            "stocks": {"total": 100, "active": 90, "delisted": 8, "corrupted": 2},
            "etfs": {"total": 30, "active": 28, "delisted": 2, "corrupted": 0},
            "indices": {"total": 5},
            "forex": {"total": 10},
            "cryptocurrencies": {"total": 4},
            "commodities": {"total": 6},
            "economic": {"total": 7},
            "yield_status": {
                "endpoints": {
                    "prices": {
                        "true": 80, "false": 15, "null": 5,
                        "true_ratio": 0.8421, "false_ratio": 0.1579,
                    },
                },
            },
            "earnings_calendar": {
                "total": 50, "cast_issues": 1, "avg_days_to_next_reportedDate": 12.5,
            },
        },
        "ingestion": {
            "missing": False,
            "total_issues": 7,
            "timezone_mismatch": 1,
            "av_throttle": 2,
            "structure_error": {
                "total": 2,
                "by_asset_endpoint": [
                    {"asset_type": "stocks", "endpoint": "prices", "count": 2},
                ],
            },
            "empty_content": {"total": 1, "by_asset_endpoint": []},
            "cast_failure": {"total": 1, "by_asset_endpoint": []},
        },
        "coverage": {
            "qqq_profile_status": "ok",
            "qqq_holdings_count": 100,
            "summary": {
                "total_checked": 10,
                "intraday_ok": 9,
                "daily_ok": 10,
                "failures": ["AAPL: intraday empty"],
            },
        },
        "file_counts": {
            "stocks": {
                "prices": {"files_written": 90, "expected": 95, "ratio": 0.947},
            },
        },
        "storage": {"bytes": 123456, "file_count": 42},
        "api_calls": {"total_calls_made": 5000},
        "delta": {"previous_available": False},
    }


# ---------------------------------------------------------------------------
# build_report
# ---------------------------------------------------------------------------

@pytest.fixture
def patched_analyzers(monkeypatch):
    """Replace each analyzer with a stub returning a marker dict so the
    assembly can be asserted without touching disk."""
    monkeypatch.setattr(report_module, "analyze_catalog",
                        lambda *a, **k: {
                            "stocks": {"total": 1, "active": 1,
                                       "delisted": 0, "corrupted": 0},
                            "yield_status": {"missing": True},
                            "earnings_calendar": {"missing": True},
                        })
    monkeypatch.setattr(report_module, "analyze_ingestion",
                        lambda p: {
                            "missing": False, "total_issues": 0,
                            "timezone_mismatch": 0, "av_throttle": 0,
                            "structure_error": {"total": 0, "by_asset_endpoint": []},
                            "empty_content": {"total": 0, "by_asset_endpoint": []},
                            "cast_failure": {"total": 0, "by_asset_endpoint": []},
                        })
    monkeypatch.setattr(report_module, "analyze_coverage",
                        lambda d: {
                            "qqq_profile_status": "ok",
                            "qqq_holdings_count": 0,
                            "summary": {
                                "total_checked": 0, "intraday_ok": 0,
                                "daily_ok": 0, "failures": [],
                            },
                        })
    monkeypatch.setattr(report_module, "analyze_files",
                        lambda d, c: {"stocks": {}})
    monkeypatch.setattr(report_module, "analyze_storage",
                        lambda d: {"bytes": 10, "file_count": 1})


def test_build_report_assembles_all_sections(patched_analyzers, tmp_path):
    report = build_report(
        mode="daily",
        folder_date=date(2026, 6, 1),
        catalog_dir=tmp_path / "catalog",
        folder_dir=tmp_path / "daily" / "2026-06-01",
        api_call_count=4242,
    )
    assert report["mode"] == "daily"
    assert report["folder_date"] == "2026-06-01"
    assert "generated_at" in report
    assert report["catalog"]["stocks"]["total"] == 1
    assert report["storage"] == {"bytes": 10, "file_count": 1}
    assert report["api_calls"]["total_calls_made"] == 4242
    # No previous report => delta records previous_available False.
    assert report["delta"] == {"previous_available": False}


def test_build_report_api_count_none_when_omitted(patched_analyzers, tmp_path):
    report = build_report(
        mode="daily",
        folder_date=date(2026, 6, 1),
        catalog_dir=tmp_path,
        folder_dir=tmp_path,
    )
    assert report["api_calls"]["total_calls_made"] is None


def test_build_report_computes_delta_against_previous(patched_analyzers, tmp_path):
    previous = {
        "mode": "daily",
        "folder_date": "2026-05-31",
        "catalog": {"stocks": {"total": 1}},
        "ingestion": {"total_issues": 0},
        "coverage": {"summary": {"total_checked": 0}},
    }
    report = build_report(
        mode="daily",
        folder_date=date(2026, 6, 1),
        catalog_dir=tmp_path,
        folder_dir=tmp_path,
        previous_report=previous,
    )
    assert report["delta"]["previous_available"] is True
    assert report["delta"]["previous_folder_date"] == "2026-05-31"


# ---------------------------------------------------------------------------
# render_markdown
# ---------------------------------------------------------------------------

def test_render_markdown_contains_headline_sections():
    md = render_markdown(_sample_report())
    assert "# Monitoring report (daily, 2026-06-01)" in md
    assert "Alpha Vantage calls this run: 5,000" in md
    assert "Folder size: 123,456 bytes across 42 files" in md
    assert "## Catalog" in md
    assert "## Ingestion report" in md
    assert "## Coverage probes" in md
    assert "## File counts vs expected" in md
    # yield_status table row
    assert "| prices | 80 | 15 | 5 | 0.8421 | 0.1579 |" in md


def test_render_markdown_marks_missing_catalog():
    report = _sample_report()
    report["catalog"]["stocks"] = {"missing": True}
    md = render_markdown(report)
    assert "| stocks | missing | -- | -- | -- |" in md


def test_render_markdown_no_previous_delta_section():
    md = render_markdown(_sample_report())
    assert "No previous monitoring report available; delta skipped." in md


def test_render_markdown_renders_delta_when_available():
    report = _sample_report()
    report["delta"] = {
        "previous_available": True,
        "previous_mode": "daily",
        "previous_folder_date": "2026-05-31",
        "catalog": {
            "stocks": {"total": 5, "active": 4, "delisted": 1, "corrupted": 0},
            "etfs": {"total": 0, "active": 0, "delisted": 0, "corrupted": 0},
            "indices": {"total": 0}, "forex": {"total": 0},
            "cryptocurrencies": {"total": 0}, "commodities": {"total": 0},
            "economic": {"total": 0},
        },
        "ingestion": {"total_issues": 2, "av_throttle": 1, "timezone_mismatch": 0},
    }
    md = render_markdown(report)
    assert "## Delta vs previous report" in md
    assert "Previous report: daily / 2026-05-31" in md
    # Signed formatting: +5 total for stocks.
    assert "stocks: total +5" in md


def test_render_markdown_truncates_long_failure_lists():
    report = _sample_report()
    report["coverage"]["summary"]["failures"] = [f"S{i}: fail" for i in range(60)]
    md = render_markdown(report)
    assert "... and 10 more" in md


# ---------------------------------------------------------------------------
# log_summary / write_report / run_report_and_persist
# ---------------------------------------------------------------------------

def test_log_summary_emits_info_and_warnings(caplog):
    import logging
    with caplog.at_level(logging.INFO):
        log_summary(_sample_report())
    text = caplog.text
    assert "Monitoring report ready for daily/2026-06-01" in text
    # av_throttle=2 and timezone_mismatch=1 => warning path.
    assert "av_throttle" in text
    # coverage failures non-empty => warning path.
    assert "coverage failures" in text


def test_write_report_writes_json_and_markdown(tmp_path):
    report = _sample_report()
    json_path, md_path = write_report(report, tmp_path)
    assert json_path.name == REPORT_FILENAME_JSON
    assert md_path.name == REPORT_FILENAME_MD
    loaded = json.loads(json_path.read_text(encoding="utf-8"))
    assert loaded["mode"] == "daily"
    assert md_path.read_text(encoding="utf-8").startswith("# Monitoring report")


def test_run_report_and_persist_end_to_end(patched_analyzers, tmp_path):
    folder_dir = tmp_path / "daily" / "2026-06-01"
    report, json_path, md_path = run_report_and_persist(
        mode="daily",
        folder_date=date(2026, 6, 1),
        catalog_dir=tmp_path / "catalog",
        folder_dir=folder_dir,
        api_call_count=100,
    )
    assert json_path.exists()
    assert md_path.exists()
    assert report["api_calls"]["total_calls_made"] == 100
