"""Unit tests for monitoring_service.diff."""

import json
import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from monitoring_service.diff import diff_reports, load_previous_report

MOCK_DIR = Path(__file__).parent / "mock_diff"


@pytest.fixture
def tmp_dir():
    if MOCK_DIR.exists():
        shutil.rmtree(MOCK_DIR)
    MOCK_DIR.mkdir(parents=True)
    yield MOCK_DIR
    shutil.rmtree(MOCK_DIR)


def test_diff_returns_unavailable_when_no_previous():
    out = diff_reports({"catalog": {}}, None)
    assert out == {"previous_available": False}


def test_diff_catalog_signed_deltas():
    current = {
        "catalog": {
            "stocks": {"total": 16100, "active": 5000, "delisted": 11050,
                       "corrupted": 50},
            "etfs": {"total": 6500, "active": 5500, "delisted": 1000,
                     "corrupted": 0},
            "yield_status": {
                "endpoints": {
                    "prices": {"true": 4900, "false": 100},
                }
            },
        },
        "ingestion": {
            "av_throttle": 0, "timezone_mismatch": 0, "total_issues": 5,
            "structure_error": {"total": 1},
            "empty_content": {"total": 4},
            "cast_failure": {"total": 0},
        },
        "coverage": {"summary": {"total_checked": 110, "intraday_ok": 109,
                                 "daily_ok": 110}},
    }
    previous = {
        "folder_date": "2026-04-22", "mode": "daily",
        "catalog": {
            "stocks": {"total": 16095, "active": 4998, "delisted": 11050,
                       "corrupted": 47},
            "etfs": {"total": 6500, "active": 5500, "delisted": 1000,
                     "corrupted": 0},
            "yield_status": {
                "endpoints": {
                    "prices": {"true": 4880, "false": 120},
                }
            },
        },
        "ingestion": {
            "av_throttle": 1, "timezone_mismatch": 0, "total_issues": 6,
            "structure_error": {"total": 2},
            "empty_content": {"total": 3},
            "cast_failure": {"total": 1},
        },
        "coverage": {"summary": {"total_checked": 110, "intraday_ok": 110,
                                 "daily_ok": 110}},
    }
    out = diff_reports(current, previous)
    assert out["previous_available"] is True
    assert out["previous_folder_date"] == "2026-04-22"
    assert out["catalog"]["stocks"]["total"] == 5
    assert out["catalog"]["stocks"]["active"] == 2
    assert out["catalog"]["stocks"]["corrupted"] == 3
    assert out["catalog"]["yield_status"]["prices"]["true"] == 20
    assert out["catalog"]["yield_status"]["prices"]["false"] == -20
    assert out["ingestion"]["av_throttle"] == -1
    assert out["ingestion"]["total_issues"] == -1
    assert out["coverage"]["intraday_ok"] == -1


def test_load_previous_report_handles_missing_and_invalid(tmp_dir):
    assert load_previous_report(tmp_dir / "nope.json") is None

    bad = tmp_dir / "bad.json"
    bad.write_text("not json", encoding="utf-8")
    assert load_previous_report(bad) is None

    good = tmp_dir / "good.json"
    good.write_text(json.dumps({"catalog": {}}), encoding="utf-8")
    assert load_previous_report(good) == {"catalog": {}}
