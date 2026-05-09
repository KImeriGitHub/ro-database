"""Unit tests for monitoring_service.analyze_catalog."""

import shutil
import sys
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from monitoring_service.analyze_catalog import analyze_catalog
from asset_catalog_service.updates._common import YIELD_ENDPOINTS

MOCK_DIR = Path(__file__).parent / "mock_catalog"


@pytest.fixture
def catalog_dir():
    if MOCK_DIR.exists():
        shutil.rmtree(MOCK_DIR)
    MOCK_DIR.mkdir(parents=True)
    yield MOCK_DIR
    shutil.rmtree(MOCK_DIR)


def _write_statused(path: Path, statuses: list[str]) -> None:
    pl.DataFrame({
        "symbol": [f"S{i}" for i in range(len(statuses))],
        "status": statuses,
    }).write_parquet(path)


def test_statused_summary_buckets_active_delisted_corrupted(catalog_dir):
    _write_statused(catalog_dir / "stocks.parquet",
                    ["Active", "Active", "Delisted", "Corrupted", "Active"])
    out = analyze_catalog(catalog_dir, today=date(2026, 4, 23))
    assert out["stocks"]["total"] == 5
    assert out["stocks"]["active"] == 3
    assert out["stocks"]["delisted"] == 1
    assert out["stocks"]["corrupted"] == 1


def test_statused_summary_handles_lowercase(catalog_dir):
    _write_statused(catalog_dir / "etfs.parquet", ["active", "delisted"])
    out = analyze_catalog(catalog_dir)
    assert out["etfs"]["active"] == 1
    assert out["etfs"]["delisted"] == 1


def test_count_only_catalogs(catalog_dir):
    pl.DataFrame({"symbol": ["WTI", "BRENT", "GOLD"]}).write_parquet(
        catalog_dir / "commodities.parquet"
    )
    pl.DataFrame({"symbol": ["GDP", "CPI"]}).write_parquet(
        catalog_dir / "economic.parquet"
    )
    out = analyze_catalog(catalog_dir)
    assert out["commodities"] == {"total": 3}
    assert out["economic"] == {"total": 2}


def test_missing_catalog_marked(catalog_dir):
    out = analyze_catalog(catalog_dir)
    assert out["stocks"] == {"missing": True}
    assert out["yield_status"] == {"missing": True}


def test_yield_status_summary(catalog_dir):
    data = {"symbol": [f"S{i}" for i in range(4)]}
    schema = {"symbol": pl.Utf8}
    for ep in YIELD_ENDPOINTS:
        data[ep] = [None] * 4
        schema[ep] = pl.Boolean
    data["date"] = [date(2026, 4, 22)] * 4
    schema["date"] = pl.Date

    # prices: 2 True, 1 False, 1 Null
    data["prices"] = [True, True, False, None]
    pl.DataFrame(data, schema=schema).write_parquet(
        catalog_dir / "yield_status.parquet"
    )

    out = analyze_catalog(catalog_dir)
    yld = out["yield_status"]["endpoints"]["prices"]
    assert yld["true"] == 2
    assert yld["false"] == 1
    assert yld["null"] == 1
    assert yld["true_ratio"] == pytest.approx(2 / 3, abs=1e-3)
    assert yld["false_ratio"] == pytest.approx(1 / 3, abs=1e-3)


def test_earnings_calendar_summary(catalog_dir, tmp_path):
    today = date(2026, 4, 23)
    folder_dir = tmp_path / "historical"
    folder_dir.mkdir()
    pl.DataFrame({
        "symbol": ["A", "B", "C"],
        "name": ["A Co", "B Co", "C Co"],
        "reportedDate": [today + timedelta(days=10), today + timedelta(days=20), None],
        "fiscalDateEnding": [today, today, today],
        "estimate": [1.0, 2.0, None],
        "currency": ["USD", "USD", "USD"],
        "timeOfTheDay": ["pre-market", "post-market", "pre-market"],
        "cast_issues": [None, "estimate", None],
    }).write_parquet(folder_dir / "earnings_calendar.parquet")

    out = analyze_catalog(catalog_dir, today=today, folder_dir=folder_dir)
    ec = out["earnings_calendar"]
    assert ec["total"] == 3
    assert ec["cast_issues"] == 1
    assert ec["avg_days_to_next_reportedDate"] == pytest.approx(15.0, abs=0.01)


def test_earnings_calendar_missing_when_folder_dir_omitted(catalog_dir):
    """When ``folder_dir`` is not passed, the earnings_calendar entry is
    flagged missing (no implicit fallback to catalog_dir)."""
    out = analyze_catalog(catalog_dir, today=date(2026, 4, 23))
    assert out["earnings_calendar"] == {"missing": True}
