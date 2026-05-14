"""Tests for Phase 4: shareprice_intraday for stocks and etfs.

Covers source-column rename, concat of historical + multiple daily folders,
dedup discrepancy logging, orphan-date drop against the
``shareprice_daily.Date`` axis, null-OHLCV field counting (no row drop),
schema exactness, and the combined orchestrator
(``transform_stocks_or_etfs``) wiring for intraday.
"""

from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from data_transformation._common import (
    TransformationReport,
    is_already_transformed,
    symbol_dest_dir,
)
from data_transformation.AssetData import StockData
from data_transformation.AssetDataService import SCHEMAS
from data_transformation.frames.price_intraday import (
    build_shareprice_intraday,
    _normalize_intraday_source,
)
from data_transformation.frames.stocks_etfs import transform_stocks_or_etfs


# ── Helpers ───────────────────────────────────────────────────────────────────

_INTRADAY_SOURCE_SCHEMA = {
    "Date": pl.Datetime,
    "Open": pl.Float32,
    "High": pl.Float32,
    "Low": pl.Float32,
    "Close": pl.Float32,
    "Volume": pl.Float32,
}

_DAILY_SOURCE_SCHEMA = {
    "Date": pl.Date,
    "Open": pl.Float32,
    "High": pl.Float32,
    "Low": pl.Float32,
    "Close": pl.Float32,
    "Volume": pl.Float32,
    "DividendAmount": pl.Float32,
    "SplitCoefficient": pl.Float32,
}


def _write_intraday_source(path: Path, rows: list[tuple]) -> None:
    """rows: (Datetime, Open, High, Low, Close, Volume).
    Source schema uses column name 'Date' for the Datetime column.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "Date": [r[0] for r in rows],
            "Open": [r[1] for r in rows],
            "High": [r[2] for r in rows],
            "Low": [r[3] for r in rows],
            "Close": [r[4] for r in rows],
            "Volume": [r[5] for r in rows],
        },
        schema=_INTRADAY_SOURCE_SCHEMA,
    ).write_parquet(path)


def _write_daily_source(path: Path, rows: list[tuple]) -> None:
    """rows: (Date, Open, High, Low, Close, Volume, Div, SC)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "Date": [r[0] for r in rows],
            "Open": [r[1] for r in rows],
            "High": [r[2] for r in rows],
            "Low": [r[3] for r in rows],
            "Close": [r[4] for r in rows],
            "Volume": [r[5] for r in rows],
            "DividendAmount": [r[6] for r in rows],
            "SplitCoefficient": [r[7] for r in rows],
        },
        schema=_DAILY_SOURCE_SCHEMA,
    ).write_parquet(path)


def _daily_dates(rows: list[date]) -> pl.Series:
    return pl.Series("Date", rows, dtype=pl.Date)


def _make_overview(rows: list[tuple[str, str, str, str]]) -> pl.DataFrame:
    """rows: (symbol, assetType, about, sector)."""
    return pl.DataFrame(
        {
            "symbol": [r[0] for r in rows],
            "assetType": [r[1] for r in rows],
            "about": [r[2] for r in rows],
            "reportedDate": [None] * len(rows),
            "timeOfTheDay": [""] * len(rows),
            "sector": [r[3] for r in rows],
        },
        schema={
            "symbol": pl.Utf8, "assetType": pl.Utf8, "about": pl.Utf8,
            "reportedDate": pl.Date, "timeOfTheDay": pl.Utf8, "sector": pl.Utf8,
        },
    )


# ── 1. Source column rename ───────────────────────────────────────────────────

def test_normalize_renames_date_to_datetime():
    src = pl.DataFrame(
        {
            "Date": [datetime(2020, 1, 1, 9, 30)],
            "Open": [100.0], "High": [101.0], "Low": [99.0],
            "Close": [100.5], "Volume": [1000.0],
        },
        schema=_INTRADAY_SOURCE_SCHEMA,
    )
    out = _normalize_intraday_source(src)
    assert "Datetime" in out.columns
    assert "Date" not in out.columns
    assert out.schema["Datetime"] == pl.Datetime("us")


def test_normalize_strips_timezone_keeping_wallclock():
    """A tz-aware source has its tz dropped while the wall-clock value
    survives. The source fixture is built naive-first then tagged with
    ``replace_time_zone("US/Eastern")`` so the displayed wall-clock is
    9:30 ET; passing a naive Python datetime directly with a tz schema
    would instead make polars treat it as UTC and shift the wall-clock.
    """
    src = pl.DataFrame(
        {
            "Date": [datetime(2020, 1, 1, 9, 30)],
            "Open": [100.0], "High": [100.0], "Low": [100.0],
            "Close": [100.0], "Volume": [1000.0],
        },
        schema={
            "Date": pl.Datetime("us"),
            "Open": pl.Float32, "High": pl.Float32, "Low": pl.Float32,
            "Close": pl.Float32, "Volume": pl.Float32,
        },
    ).with_columns(pl.col("Date").dt.replace_time_zone("US/Eastern"))
    assert src.schema["Date"].time_zone == "US/Eastern"

    out = _normalize_intraday_source(src)
    assert out.schema["Datetime"] == pl.Datetime("us")  # no tz
    assert out["Datetime"][0] == datetime(2020, 1, 1, 9, 30)


# ── 2. Concat across historical + multiple daily folders ──────────────────────

def test_concat_historical_plus_multiple_daily(tmp_path):
    h = tmp_path / "stocks_AAPL_hist.parquet"
    d1 = tmp_path / "stocks_AAPL_d1.parquet"
    d2 = tmp_path / "stocks_AAPL_d2.parquet"
    _write_intraday_source(h,  [(datetime(2020, 1, 1, 9, 30), 100, 100, 100, 100, 500)])
    _write_intraday_source(d1, [(datetime(2020, 1, 2, 9, 30), 101, 101, 101, 101, 600)])
    _write_intraday_source(d2, [(datetime(2020, 1, 3, 9, 30), 102, 102, 102, 102, 700)])

    out = build_shareprice_intraday(
        "stocks", "AAPL", [h, d1, d2],
        _daily_dates([date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3)]),
        TransformationReport(),
    )
    assert out.height == 3
    assert out["Datetime"].to_list() == [
        datetime(2020, 1, 1, 9, 30),
        datetime(2020, 1, 2, 9, 30),
        datetime(2020, 1, 3, 9, 30),
    ]


# ── 3. Dedup discrepancy logging ──────────────────────────────────────────────

def test_dedup_under_1pct_logged_and_daily_wins(tmp_path):
    """Historic extends past the daily-overlap minute so the overlap is on
    an *interior* Datetime -- the boundary-suppression rule for price
    frames silences discrepancies on max(historic Datetime) only."""
    h = tmp_path / "h.parquet"
    d = tmp_path / "d.parquet"
    _write_intraday_source(h, [
        (datetime(2020, 1, 1, 9, 30), 100, 100, 100, 100.0,  1000),
        (datetime(2020, 1, 2, 9, 30), 101, 101, 101, 101.0,  1000),
    ])
    _write_intraday_source(d, [(datetime(2020, 1, 1, 9, 30), 100, 100, 100, 100.5,  1000)])  # 0.5%
    report = TransformationReport()
    out = build_shareprice_intraday(
        "stocks", "AAPL", [h, d],
        _daily_dates([date(2020, 1, 1), date(2020, 1, 2)]),
        report,
    )
    assert out.height == 2
    overlap = out.filter(pl.col("Datetime") == datetime(2020, 1, 1, 9, 30))
    assert pytest.approx(100.5, rel=1e-4) == overlap["Close"][0]
    rep = report.to_frame()
    assert rep.filter(
        pl.col("issue_type") == "dedup_value_discrepancy_under_1pct"
    ).height == 1


def test_dedup_over_1pct_logged_and_daily_wins(tmp_path):
    h = tmp_path / "h.parquet"
    d = tmp_path / "d.parquet"
    _write_intraday_source(h, [
        (datetime(2020, 1, 1, 9, 30), 100, 100, 100, 100.0, 1000),
        (datetime(2020, 1, 2, 9, 30), 101, 101, 101, 101.0, 1000),
    ])
    _write_intraday_source(d, [(datetime(2020, 1, 1, 9, 30), 100, 100, 100, 110.0, 1000)])  # 10%
    report = TransformationReport()
    out = build_shareprice_intraday(
        "stocks", "AAPL", [h, d],
        _daily_dates([date(2020, 1, 1), date(2020, 1, 2)]),
        report,
    )
    overlap = out.filter(pl.col("Datetime") == datetime(2020, 1, 1, 9, 30))
    assert pytest.approx(110.0, rel=1e-4) == overlap["Close"][0]
    assert report.to_frame().filter(
        pl.col("issue_type") == "dedup_value_discrepancy_over_1pct"
    ).height == 1


# ── 4. Orphan-date drop ───────────────────────────────────────────────────────

def test_orphan_date_dropped_and_logged(tmp_path):
    p = tmp_path / "stocks_AAPL.parquet"
    _write_intraday_source(p, [
        (datetime(2020, 1, 1, 9, 30), 100, 100, 100, 100, 500),
        (datetime(2020, 1, 2, 9, 30), 101, 101, 101, 101, 600),
        (datetime(2020, 1, 3, 9, 30), 102, 102, 102, 102, 700),  # orphan
        (datetime(2020, 1, 4, 9, 30), 103, 103, 103, 103, 800),  # orphan
    ])
    report = TransformationReport()
    out = build_shareprice_intraday(
        "stocks", "AAPL", [p],
        _daily_dates([date(2020, 1, 1), date(2020, 1, 2)]),
        report,
    )
    assert out.height == 2
    rep = report.to_frame().filter(
        pl.col("issue_type") == "intraday_orphan_date_dropped"
    )
    assert rep.height == 1
    assert rep["count"][0] == 2
    assert pytest.approx(0.5, rel=1e-6) == rep["relative"][0]


# ── 5. Raw OHLCV preserved ────────────────────────────────────────────────────

def test_output_carries_raw_ohlcv(tmp_path):
    """No adjustment is applied; OHLCV survives verbatim."""
    p = tmp_path / "p.parquet"
    _write_intraday_source(p, [
        (datetime(2020, 1, 1, 9, 30), 400.0, 410.0, 395.0, 405.0, 500.0),
        (datetime(2020, 1, 2, 9, 30), 100.0, 102.5, 99.0, 101.0, 4000.0),
    ])
    out = build_shareprice_intraday(
        "stocks", "AAPL", [p],
        _daily_dates([date(2020, 1, 1), date(2020, 1, 2)]),
        TransformationReport(),
    )
    assert out.height == 2
    day1 = out.filter(pl.col("Datetime") == datetime(2020, 1, 1, 9, 30))
    assert pytest.approx(405.0, rel=1e-4) == day1["Close"][0]
    assert pytest.approx(500.0, rel=1e-4) == day1["Volume"][0]
    day2 = out.filter(pl.col("Datetime") == datetime(2020, 1, 2, 9, 30))
    assert pytest.approx(101.0, rel=1e-4) == day2["Close"][0]
    assert pytest.approx(4000.0, rel=1e-4) == day2["Volume"][0]


# ── 6. Null OHLCV fields preserved & logged ───────────────────────────────────

def test_null_ohlcv_fields_not_dropped_only_logged(tmp_path):
    """A row with null source Open survives and is reported. Row is NOT
    dropped.
    """
    p = tmp_path / "p.parquet"
    _write_intraday_source(p, [
        (datetime(2020, 1, 1, 9, 30), 100.0, 100.0, 100.0, 100.0, 1000.0),
        (datetime(2020, 1, 1, 9, 31), None,  100.0, 100.0, 100.0, 1000.0),  # null Open
    ])
    report = TransformationReport()
    out = build_shareprice_intraday(
        "stocks", "AAPL", [p], _daily_dates([date(2020, 1, 1)]), report,
    )
    assert out.height == 2  # row preserved
    null_open_row = out.filter(pl.col("Datetime") == datetime(2020, 1, 1, 9, 31))
    assert null_open_row["Open"][0] is None
    rep = report.to_frame().filter(pl.col("issue_type") == "intraday_null_field")
    assert rep.height == 1
    assert rep["count"][0] == 1  # one null field
    # 2 rows * 5 OHLCV cols = 10 fields; 1 null -> relative = 1/10
    assert pytest.approx(0.1, rel=1e-6) == rep["relative"][0]


# ── 7. Output schema exact ────────────────────────────────────────────────────

def test_output_schema_exact(tmp_path):
    p = tmp_path / "p.parquet"
    _write_intraday_source(p, [
        (datetime(2020, 1, 1, 9, 30), 100.0, 101.0, 99.0, 100.5, 1000.0),
    ])
    out = build_shareprice_intraday(
        "stocks", "AAPL", [p], _daily_dates([date(2020, 1, 1)]),
        TransformationReport(),
    )
    assert dict(out.schema) == SCHEMAS["shareprice_intraday"]
    assert set(out.columns) == {
        "Datetime", "Open", "High", "Low", "Close", "Volume",
    }


# ── 8. Empty inputs ───────────────────────────────────────────────────────────

def test_empty_paths():
    out = build_shareprice_intraday(
        "stocks", "AAPL", [],
        _daily_dates([date(2020, 1, 1)]),
        TransformationReport(),
    )
    assert out.height == 0
    assert dict(out.schema) == SCHEMAS["shareprice_intraday"]


# ── 9. Empty daily-dates set ──────────────────────────────────────────────────

def test_empty_daily_dates_drops_every_intraday_row(tmp_path):
    p = tmp_path / "p.parquet"
    _write_intraday_source(p, [
        (datetime(2020, 1, 1, 9, 30), 100, 100, 100, 100, 1000),
        (datetime(2020, 1, 2, 9, 30), 101, 101, 101, 101, 1000),
    ])
    report = TransformationReport()
    out = build_shareprice_intraday(
        "stocks", "AAPL", [p], _daily_dates([]), report,
    )
    assert out.height == 0
    assert dict(out.schema) == SCHEMAS["shareprice_intraday"]
    rep = report.to_frame().filter(
        pl.col("issue_type") == "intraday_orphan_date_dropped"
    )
    assert rep.height == 1
    assert rep["count"][0] == 2
    assert rep["relative"][0] == 1.0


# ── 10. Orchestrator wiring (combined) ────────────────────────────────────────

def test_orchestrator_writes_both_daily_and_intraday(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"

    _write_daily_source(
        historical / "stocks" / "prices_daily" / "stocks_AAPL.parquet",
        [(date(2020, 1, 1), 100, 100, 100, 100, 1000, 0.0, 1.0)],
    )
    _write_intraday_source(
        historical / "stocks" / "prices" / "stocks_AAPL.parquet",
        [(datetime(2020, 1, 1, 9, 30), 100, 101, 99, 100, 500)],
    )
    overview = _make_overview([("AAPL", "stocks", "Apple", "Technology")])
    n = transform_stocks_or_etfs(
        "stocks", historical, daily, dest, overview, TransformationReport(),
    )
    assert n == 1

    inst = StockData.load_from(symbol_dest_dir(dest, "stocks", "AAPL"))
    assert inst.shareprice_daily.height == 1
    assert inst.shareprice_intraday.height == 1
    assert dict(inst.shareprice_intraday.schema) == SCHEMAS["shareprice_intraday"]


def test_orchestrator_symbols_filter_intraday(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"
    for sym in ("AAPL", "MSFT"):
        _write_daily_source(
            historical / "stocks" / "prices_daily" / f"stocks_{sym}.parquet",
            [(date(2020, 1, 1), 100, 100, 100, 100, 1000, 0.0, 1.0)],
        )
        _write_intraday_source(
            historical / "stocks" / "prices" / f"stocks_{sym}.parquet",
            [(datetime(2020, 1, 1, 9, 30), 100, 101, 99, 100, 500)],
        )
    overview = _make_overview([
        ("AAPL", "stocks", "Apple", "Technology"),
        ("MSFT", "stocks", "Microsoft", "Technology"),
    ])
    n = transform_stocks_or_etfs(
        "stocks", historical, daily, dest, overview, TransformationReport(),
        symbols_filter={"AAPL"},
    )
    assert n == 1
    assert is_already_transformed(dest, "stocks", "AAPL")
    assert not is_already_transformed(dest, "stocks", "MSFT")


def test_orchestrator_resume_skips_existing_with_intraday(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"
    _write_daily_source(
        historical / "stocks" / "prices_daily" / "stocks_AAPL.parquet",
        [(date(2020, 1, 1), 100, 100, 100, 100, 1000, 0.0, 1.0)],
    )
    _write_intraday_source(
        historical / "stocks" / "prices" / "stocks_AAPL.parquet",
        [(datetime(2020, 1, 1, 9, 30), 100, 101, 99, 100, 500)],
    )
    overview = _make_overview([("AAPL", "stocks", "Apple", "Technology")])

    transform_stocks_or_etfs(
        "stocks", historical, daily, dest, overview, TransformationReport(),
    )
    assert is_already_transformed(dest, "stocks", "AAPL")

    # Mutate intraday source; resume must skip without overwriting.
    _write_intraday_source(
        historical / "stocks" / "prices" / "stocks_AAPL.parquet",
        [(datetime(2020, 1, 1, 9, 30), 999, 999, 999, 999, 1)],
    )
    transform_stocks_or_etfs(
        "stocks", historical, daily, dest, overview, TransformationReport(),
    )
    inst = StockData.load_from(symbol_dest_dir(dest, "stocks", "AAPL"))
    assert pytest.approx(100.0, rel=1e-3) == inst.shareprice_intraday["Close"][0]
