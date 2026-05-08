"""Unit tests for monitoring_service.analyze_coverage."""

import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from monitoring_service.analyze_coverage import (
    INTRADAY_MIN_ROWS,
    REQUIRED_ETFS,
    analyze_coverage,
)
from historical_data_setup._common import symbol_parquet_name

MOCK_DIR = Path(__file__).parent / "mock_coverage"


@pytest.fixture
def folder_dir():
    if MOCK_DIR.exists():
        shutil.rmtree(MOCK_DIR)
    MOCK_DIR.mkdir(parents=True)
    yield MOCK_DIR
    shutil.rmtree(MOCK_DIR)


def _write_intraday(path: Path, rows: int, null_rows: int = 0) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    base = datetime(2026, 4, 23, 9, 30)
    dates = [base + timedelta(minutes=i) for i in range(rows)]

    def _col(default: float) -> list[float | None]:
        out: list[float | None] = [default] * rows
        for i in range(min(null_rows, rows)):
            out[i] = None
        return out

    pl.DataFrame({
        "Date": dates,
        "Open": _col(1.0),
        "High": _col(1.0),
        "Low": _col(1.0),
        "Close": _col(1.0),
        "Volume": _col(100.0),
    }).write_parquet(path)


def _write_daily(path: Path, rows: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    base = datetime(2026, 4, 23).date()
    pl.DataFrame({
        "Date": [base for _ in range(rows)],
        "Open": [1.0] * rows, "High": [1.0] * rows,
        "Low": [1.0] * rows, "Close": [1.0] * rows,
        "Volume": [100.0] * rows,
        "DividendAmount": [0.0] * rows, "SplitCoefficient": [1.0] * rows,
    }).write_parquet(path)


def test_all_required_etfs_missing(folder_dir):
    out = analyze_coverage(folder_dir)
    assert out["qqq_profile_status"] == "missing"
    assert out["summary"]["total_checked"] == len(REQUIRED_ETFS)
    assert out["summary"]["intraday_ok"] == 0
    assert out["summary"]["daily_ok"] == 0


def test_required_etf_passes_when_files_valid(folder_dir):
    for sym in REQUIRED_ETFS:
        fname = symbol_parquet_name("etfs", sym)
        _write_intraday(folder_dir / "etfs" / "prices" / fname,
                        INTRADAY_MIN_ROWS)
        _write_daily(folder_dir / "etfs" / "prices_daily" / fname, 1)
    out = analyze_coverage(folder_dir)
    assert out["summary"]["intraday_ok"] == len(REQUIRED_ETFS)
    assert out["summary"]["daily_ok"] == len(REQUIRED_ETFS)
    assert out["summary"]["failures"] == []


def test_intraday_failure_when_too_few_rows(folder_dir):
    spy_fname = symbol_parquet_name("etfs", "SPY")
    _write_intraday(folder_dir / "etfs" / "prices" / spy_fname, 100)
    _write_daily(folder_dir / "etfs" / "prices_daily" / spy_fname, 1)
    out = analyze_coverage(folder_dir)
    spy = next(r for r in out["etf_results"] if r["symbol"] == "SPY")
    assert not spy["intraday"]["ok"]
    assert "rows=100" in spy["intraday"]["reason"]
    assert spy["daily"]["ok"]


def test_intraday_null_ratio_per_column(folder_dir):
    # 500 rows, 10 nulls in Open -> 0.02 ratio, fails the 0.01 threshold
    spy_fname = symbol_parquet_name("etfs", "SPY")
    _write_intraday(folder_dir / "etfs" / "prices" / spy_fname,
                    500, null_rows=10)
    _write_daily(folder_dir / "etfs" / "prices_daily" / spy_fname, 1)
    out = analyze_coverage(folder_dir)
    spy = next(r for r in out["etf_results"] if r["symbol"] == "SPY")
    assert not spy["intraday"]["ok"]
    assert "Open" in spy["intraday"]["reason"]


def test_daily_passes_with_seven_day_window(folder_dir):
    """The daily check is a coverage floor (``rows >= DAILY_MIN_ROWS``), not
    a strict equality, since the 7-day prices_daily window can yield 1 to ~5
    rows depending on holidays and how many trading days fall in the
    window."""
    spy_fname = symbol_parquet_name("etfs", "SPY")
    _write_intraday(folder_dir / "etfs" / "prices" / spy_fname,
                    INTRADAY_MIN_ROWS)
    _write_daily(folder_dir / "etfs" / "prices_daily" / spy_fname, 5)
    out = analyze_coverage(folder_dir)
    spy = next(r for r in out["etf_results"] if r["symbol"] == "SPY")
    assert spy["daily"]["ok"]
    assert spy["daily"]["rows"] == 5


def test_daily_zero_rows_fails(folder_dir):
    """A parquet that exists but has zero rows fails the coverage floor."""
    spy_fname = symbol_parquet_name("etfs", "SPY")
    _write_intraday(folder_dir / "etfs" / "prices" / spy_fname,
                    INTRADAY_MIN_ROWS)
    _write_daily(folder_dir / "etfs" / "prices_daily" / spy_fname, 0)
    out = analyze_coverage(folder_dir)
    spy = next(r for r in out["etf_results"] if r["symbol"] == "SPY")
    assert not spy["daily"]["ok"]


def test_qqq_holdings_extend_probe(folder_dir):
    profile_path = (
        folder_dir / "etfs" / "etf_profile" / symbol_parquet_name("etfs", "QQQ")
    )
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "date": [datetime(2026, 4, 23).date()],
            "holdings": [
                [
                    {"symbol": "AAPL", "weight": 0.1},
                    {"symbol": "MSFT", "weight": 0.09},
                ]
            ],
        },
        schema={
            "date": pl.Date,
            "holdings": pl.List(pl.Struct({"symbol": pl.Utf8, "weight": pl.Float32})),
        },
    ).write_parquet(profile_path)

    aapl_fname = symbol_parquet_name("stocks", "AAPL")
    _write_intraday(folder_dir / "stocks" / "prices" / aapl_fname,
                    INTRADAY_MIN_ROWS)
    _write_daily(folder_dir / "stocks" / "prices_daily" / aapl_fname, 1)

    out = analyze_coverage(folder_dir)
    assert out["qqq_profile_status"] == "present"
    assert out["qqq_holdings_count"] == 2
    holdings = {r["symbol"]: r for r in out["holdings_results"]}
    assert holdings["AAPL"]["intraday"]["ok"]
    assert not holdings["MSFT"]["intraday"]["ok"]
    assert holdings["MSFT"]["intraday"]["reason"] == "missing"
