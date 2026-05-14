"""Tests for data_transformation/frames/_dedup.py."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from data_transformation._common import TransformationReport
from data_transformation.frames._dedup import (
    SOURCE_ORDER_COL,
    attach_source_order,
    dedup_with_discrepancy_log,
)


_FLOAT_COLS = ("Open", "High", "Low", "Close", "Volume")


def _make_frame(rows: list[tuple]) -> pl.DataFrame:
    """rows: list of (Date, Open, High, Low, Close, Volume)"""
    return pl.DataFrame(
        {
            "Date": [r[0] for r in rows],
            "Open": [r[1] for r in rows],
            "High": [r[2] for r in rows],
            "Low": [r[3] for r in rows],
            "Close": [r[4] for r in rows],
            "Volume": [r[5] for r in rows],
        },
        schema={
            "Date": pl.Date,
            "Open": pl.Float32,
            "High": pl.Float32,
            "Low": pl.Float32,
            "Close": pl.Float32,
            "Volume": pl.Float32,
        },
    )


def test_attach_source_order():
    f1 = _make_frame([(date(2020, 1, 1), 1.0, 1.0, 1.0, 1.0, 100.0)])
    f2 = _make_frame([(date(2020, 1, 2), 2.0, 2.0, 2.0, 2.0, 200.0)])
    out = attach_source_order([f1, f2])
    assert out.height == 2
    assert SOURCE_ORDER_COL in out.columns
    assert out[SOURCE_ORDER_COL].to_list() == [0, 1]


def test_dedup_no_duplicates_no_log():
    df = attach_source_order([_make_frame([
        (date(2020, 1, 1), 1.0, 1.0, 1.0, 1.0, 100.0),
        (date(2020, 1, 2), 2.0, 2.0, 2.0, 2.0, 200.0),
    ])])
    report = TransformationReport()
    out = dedup_with_discrepancy_log(df, "Date", _FLOAT_COLS, report, "X", "forex", "price_daily")
    assert out.height == 2
    assert SOURCE_ORDER_COL not in out.columns
    assert report.to_frame().height == 0


def test_dedup_identical_duplicates_no_discrepancy_log():
    """Two sources, same Date, same values: dedup but no discrepancy."""
    f1 = _make_frame([(date(2020, 1, 1), 1.0, 1.0, 1.0, 1.0, 100.0)])
    f2 = _make_frame([(date(2020, 1, 1), 1.0, 1.0, 1.0, 1.0, 100.0)])
    df = attach_source_order([f1, f2])
    report = TransformationReport()
    out = dedup_with_discrepancy_log(df, "Date", _FLOAT_COLS, report, "X", "forex", "price_daily")
    assert out.height == 1
    assert report.to_frame().height == 0


def test_dedup_under_1pct_discrepancy_logged():
    """Close 100 vs 100.5 = 0.5% difference -> under_1pct."""
    f1 = _make_frame([(date(2020, 1, 1), 1.0, 1.0, 1.0, 100.0, 100.0)])
    f2 = _make_frame([(date(2020, 1, 1), 1.0, 1.0, 1.0, 100.5, 100.0)])
    df = attach_source_order([f1, f2])
    report = TransformationReport()
    out = dedup_with_discrepancy_log(df, "Date", _FLOAT_COLS, report, "AAPL", "stocks", "shareprice_daily")
    assert out.height == 1
    rep = report.to_frame()
    assert rep.height == 1
    assert rep["issue_type"][0] == "dedup_value_discrepancy_under_1pct"
    assert rep["count"][0] == 1


def test_dedup_over_1pct_discrepancy_logged():
    """Close 100 vs 110 = 10% difference -> over_1pct."""
    f1 = _make_frame([(date(2020, 1, 1), 1.0, 1.0, 1.0, 100.0, 100.0)])
    f2 = _make_frame([(date(2020, 1, 1), 1.0, 1.0, 1.0, 110.0, 100.0)])
    df = attach_source_order([f1, f2])
    report = TransformationReport()
    out = dedup_with_discrepancy_log(df, "Date", _FLOAT_COLS, report, "AAPL", "stocks", "shareprice_daily")
    assert out.height == 1
    rep = report.to_frame()
    assert rep.height == 1
    assert rep["issue_type"][0] == "dedup_value_discrepancy_over_1pct"


def test_dedup_keep_last_means_most_recent_source_wins():
    """keep='last' keeps the row from the highest source order (the most
    recent daily snapshot). This is what the price frames request."""
    f_hist = _make_frame([(date(2020, 1, 1), 1.0, 1.0, 1.0, 100.0, 100.0)])
    f_daily = _make_frame([(date(2020, 1, 1), 1.0, 1.0, 1.0, 110.0, 100.0)])
    df = attach_source_order([f_hist, f_daily])
    report = TransformationReport()
    out = dedup_with_discrepancy_log(
        df, "Date", _FLOAT_COLS, report, "AAPL", "stocks", "shareprice_daily",
        keep="last",
    )
    assert out.height == 1
    assert out["Close"][0] == 110.0


def test_dedup_keep_first_default_means_earliest_source_wins():
    """Default keep='first' keeps the row from the earliest source order
    (the first snapshot to capture it). PIT-correct: insider/sentiment
    and other non-price frames use this so restatements are dropped."""
    f_hist = _make_frame([(date(2020, 1, 1), 1.0, 1.0, 1.0, 100.0, 100.0)])
    f_daily = _make_frame([(date(2020, 1, 1), 1.0, 1.0, 1.0, 110.0, 100.0)])
    df = attach_source_order([f_hist, f_daily])
    report = TransformationReport()
    out = dedup_with_discrepancy_log(
        df, "Date", _FLOAT_COLS, report, "AAPL", "stocks", "sentiment_df",
    )
    assert out.height == 1
    assert out["Close"][0] == 100.0


def test_dedup_flag_under_1pct_false_suppresses_under_log():
    """flag_under_1pct=False (insider/sentiment) skips under_1pct
    logging entirely; over_1pct still fires on real >=1% drift, which
    is the signal worth reviewing."""
    f1 = _make_frame([
        (date(2020, 1, 1), 1.0, 1.0, 1.0, 100.0, 100.0),
        (date(2020, 1, 2), 1.0, 1.0, 1.0, 200.0, 100.0),
    ])
    f2 = _make_frame([
        (date(2020, 1, 1), 1.0, 1.0, 1.0, 100.5, 100.0),  # 0.5%
        (date(2020, 1, 2), 1.0, 1.0, 1.0, 220.0, 100.0),  # 10%
    ])
    df = attach_source_order([f1, f2])
    report = TransformationReport()
    dedup_with_discrepancy_log(
        df, "Date", _FLOAT_COLS, report, "AAPL", "stocks", "sentiment_df",
        flag_under_1pct=False,
    )
    rep = report.to_frame()
    assert set(rep["issue_type"].to_list()) == {"dedup_value_discrepancy_over_1pct"}


def test_dedup_null_in_one_source_no_discrepancy():
    """A duplicate where one source has null does not flag a discrepancy.
    keep='last' is requested (price-frame behavior) so the kept row is
    from the second source, whose Close is null."""
    f1 = _make_frame([(date(2020, 1, 1), 1.0, 1.0, 1.0, 100.0, 100.0)])
    f2 = _make_frame([(date(2020, 1, 1), 1.0, 1.0, 1.0, None, 100.0)])
    df = attach_source_order([f1, f2])
    report = TransformationReport()
    out = dedup_with_discrepancy_log(
        df, "Date", _FLOAT_COLS, report, "X", "forex", "price_daily",
        keep="last",
    )
    assert out.height == 1
    assert out["Close"][0] is None
    assert report.to_frame().height == 0


def test_dedup_mixed_under_and_over_in_same_symbol():
    """Two duplicate dates: one with <1% diff, one with >=1% -> two report rows."""
    f_hist = _make_frame([
        (date(2020, 1, 1), 1.0, 1.0, 1.0, 100.0, 100.0),
        (date(2020, 1, 2), 1.0, 1.0, 1.0, 200.0, 100.0),
    ])
    f_daily = _make_frame([
        (date(2020, 1, 1), 1.0, 1.0, 1.0, 100.5, 100.0),  # 0.5%
        (date(2020, 1, 2), 1.0, 1.0, 1.0, 220.0, 100.0),  # 10%
    ])
    df = attach_source_order([f_hist, f_daily])
    report = TransformationReport()
    out = dedup_with_discrepancy_log(df, "Date", _FLOAT_COLS, report, "AAPL", "stocks", "shareprice_daily")
    assert out.height == 2
    rep = report.to_frame()
    assert rep.height == 2
    assert set(rep["issue_type"].to_list()) == {
        "dedup_value_discrepancy_under_1pct",
        "dedup_value_discrepancy_over_1pct",
    }


def test_dedup_empty_frame():
    df = pl.DataFrame(
        schema={
            "Date": pl.Date, "Open": pl.Float32, "High": pl.Float32,
            "Low": pl.Float32, "Close": pl.Float32, "Volume": pl.Float32,
            SOURCE_ORDER_COL: pl.UInt32,
        }
    )
    report = TransformationReport()
    out = dedup_with_discrepancy_log(df, "Date", _FLOAT_COLS, report, "X", "forex", "price_daily")
    assert out.height == 0
    assert SOURCE_ORDER_COL not in out.columns
    assert report.to_frame().height == 0


def test_dedup_suppress_historic_boundary_skips_partial_last_bar():
    """The last historic date routinely carries a partial bar (24/7
    markets like crypto, or any historic pull captured mid-session).
    With suppress_historic_boundary=True, the historic-vs-daily
    discrepancy on that single boundary date is silent; earlier dates
    and daily-vs-daily disagreements still fire."""
    f_hist = _make_frame([
        (date(2020, 1, 1), 1.0, 1.0, 1.0, 100.0, 100.0),
        (date(2020, 1, 2), 1.0, 1.0, 1.0, 200.0, 50.0),   # boundary, partial
    ])
    f_daily = _make_frame([
        (date(2020, 1, 2), 1.0, 1.0, 1.0, 220.0, 1000.0),  # boundary, full bar
        (date(2020, 1, 3), 1.0, 1.0, 1.0, 300.0, 100.0),
    ])
    df = attach_source_order([f_hist, f_daily])
    report = TransformationReport()
    out = dedup_with_discrepancy_log(
        df, "Date", _FLOAT_COLS, report, "BTC", "cryptocurrencies", "price_daily",
        keep="last",
        suppress_historic_boundary=True,
    )
    assert out.height == 3
    assert out.filter(pl.col("Date") == date(2020, 1, 2))["Close"][0] == 220.0
    assert report.to_frame().height == 0


def test_dedup_suppress_historic_boundary_only_affects_boundary_date():
    """A discrepancy on a non-boundary date still fires when
    suppress_historic_boundary=True. Only the maximum historic date
    is suppressed."""
    f_hist = _make_frame([
        (date(2020, 1, 1), 1.0, 1.0, 1.0, 100.0, 100.0),   # interior overlap
        (date(2020, 1, 2), 1.0, 1.0, 1.0, 200.0, 50.0),    # boundary
    ])
    f_daily = _make_frame([
        (date(2020, 1, 1), 1.0, 1.0, 1.0, 150.0, 100.0),   # 50% diff: real restatement
        (date(2020, 1, 2), 1.0, 1.0, 1.0, 220.0, 1000.0),  # boundary, suppressed
    ])
    df = attach_source_order([f_hist, f_daily])
    report = TransformationReport()
    dedup_with_discrepancy_log(
        df, "Date", _FLOAT_COLS, report, "BTC", "cryptocurrencies", "price_daily",
        keep="last",
        suppress_historic_boundary=True,
    )
    rep = report.to_frame()
    assert rep.height == 1
    assert rep["issue_type"][0] == "dedup_value_discrepancy_over_1pct"


def test_dedup_suppress_historic_boundary_daily_vs_daily_still_fires():
    """If two daily snapshots disagree on the boundary date (i.e.,
    the discrepancy does NOT involve source 0), the suppression must
    not hide it -- that is a real cross-daily restatement."""
    f_hist = _make_frame([
        (date(2020, 1, 1), 1.0, 1.0, 1.0, 100.0, 100.0),
    ])
    f_daily_1 = _make_frame([
        (date(2020, 1, 1), 1.0, 1.0, 1.0, 100.0, 100.0),   # agrees with hist
        (date(2020, 1, 2), 1.0, 1.0, 1.0, 200.0, 1000.0),
    ])
    f_daily_2 = _make_frame([
        (date(2020, 1, 2), 1.0, 1.0, 1.0, 250.0, 1000.0),  # 25% diff vs daily_1
    ])
    df = attach_source_order([f_hist, f_daily_1, f_daily_2])
    report = TransformationReport()
    dedup_with_discrepancy_log(
        df, "Date", _FLOAT_COLS, report, "BTC", "cryptocurrencies", "price_daily",
        keep="last",
        suppress_historic_boundary=True,
    )
    rep = report.to_frame()
    assert rep.height == 1
    assert rep["issue_type"][0] == "dedup_value_discrepancy_over_1pct"


def test_dedup_output_sorted_by_key():
    """Output is sorted ascending by the key column."""
    f = _make_frame([
        (date(2020, 1, 5), 1.0, 1.0, 1.0, 1.0, 100.0),
        (date(2020, 1, 1), 2.0, 2.0, 2.0, 2.0, 200.0),
        (date(2020, 1, 3), 3.0, 3.0, 3.0, 3.0, 300.0),
    ])
    df = attach_source_order([f])
    report = TransformationReport()
    out = dedup_with_discrepancy_log(df, "Date", _FLOAT_COLS, report, "X", "forex", "price_daily")
    dates = out["Date"].to_list()
    assert dates == sorted(dates)
