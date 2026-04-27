"""Unit tests for monitoring_service.analyze_ingestion."""

import shutil
import sys
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from monitoring_service.analyze_ingestion import analyze_ingestion

MOCK_DIR = Path(__file__).parent / "mock_ingestion"


@pytest.fixture
def tmp_dir():
    if MOCK_DIR.exists():
        shutil.rmtree(MOCK_DIR)
    MOCK_DIR.mkdir(parents=True)
    yield MOCK_DIR
    shutil.rmtree(MOCK_DIR)


def _write_report(path: Path, rows: list[dict]) -> None:
    schema = {
        "symbol": pl.Utf8,
        "asset_type": pl.Utf8,
        "endpoint": pl.Utf8,
        "issue_type": pl.Utf8,
        "detail": pl.Utf8,
        "timestamp": pl.Datetime,
    }
    pl.DataFrame(rows, schema=schema).write_parquet(path)


def test_missing_report(tmp_dir):
    out = analyze_ingestion(tmp_dir / "ingestion_report.parquet")
    assert out["missing"] is True
    assert out["av_throttle"] == 0


def test_aggregates_flat_and_breakdown(tmp_dir):
    now = datetime.now()
    rows = [
        {"symbol": "AAPL", "asset_type": "stocks", "endpoint": "prices",
         "issue_type": "av_throttle", "detail": "x", "timestamp": now},
        {"symbol": "MSFT", "asset_type": "stocks", "endpoint": "prices",
         "issue_type": "av_throttle", "detail": "x", "timestamp": now},
        {"symbol": "AAPL", "asset_type": "stocks", "endpoint": "prices",
         "issue_type": "structure_error", "detail": "x", "timestamp": now},
        {"symbol": "MSFT", "asset_type": "stocks", "endpoint": "earnings",
         "issue_type": "empty_content", "detail": "x", "timestamp": now},
        {"symbol": "QQQ", "asset_type": "etfs", "endpoint": "etf_profile",
         "issue_type": "cast_failure", "detail": "x", "timestamp": now},
        {"symbol": "EUR", "asset_type": "forex", "endpoint": "forex",
         "issue_type": "timezone_mismatch", "detail": "tz=UTC", "timestamp": now},
    ]
    _write_report(tmp_dir / "ingestion_report.parquet", rows)
    out = analyze_ingestion(tmp_dir / "ingestion_report.parquet")

    assert out["missing"] is False
    assert out["total_issues"] == 6
    assert out["av_throttle"] == 2
    assert out["timezone_mismatch"] == 1
    assert out["structure_error"]["total"] == 1
    assert out["empty_content"]["total"] == 1
    assert out["cast_failure"]["total"] == 1

    # breakdown order: alphabetical by (asset_type, endpoint)
    sb = out["structure_error"]["by_asset_endpoint"]
    assert sb == [{"asset_type": "stocks", "endpoint": "prices", "count": 1}]
    cb = out["cast_failure"]["by_asset_endpoint"]
    assert cb == [{"asset_type": "etfs", "endpoint": "etf_profile", "count": 1}]
