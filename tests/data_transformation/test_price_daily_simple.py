"""Tests for data_transformation/frames/price_daily.py - the simple
price_daily group (forex, indices, cryptocurrencies, commodities, economic).
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from data_transformation._common import (
    TransformationReport,
    is_already_transformed,
    symbol_dest_dir,
)
from data_transformation.AssetData import (
    CommoditiesData,
    CryptocurrenciesData,
    EconomicData,
    ForexData,
    IndexData,
)
from data_transformation.AssetDataService import SCHEMAS
from data_transformation.frames.price_daily import (
    transform_simple_price_daily,
    _normalize_simple_source,
)


# ── Test fixtures ─────────────────────────────────────────────────────────────

def _write_forex_source(path: Path, rows: list[tuple]) -> None:
    """rows: list of (Date, Open, High, Low, Close)"""
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "Date": [r[0] for r in rows],
            "Open": [r[1] for r in rows],
            "High": [r[2] for r in rows],
            "Low": [r[3] for r in rows],
            "Close": [r[4] for r in rows],
        },
        schema={
            "Date": pl.Date, "Open": pl.Float32, "High": pl.Float32,
            "Low": pl.Float32, "Close": pl.Float32,
        },
    ).write_parquet(path)


def _write_crypto_source(path: Path, rows: list[tuple]) -> None:
    """rows: list of (Date, Open, High, Low, Close, Volume)"""
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
        schema={
            "Date": pl.Date, "Open": pl.Float32, "High": pl.Float32,
            "Low": pl.Float32, "Close": pl.Float32, "Volume": pl.Float32,
        },
    ).write_parquet(path)


def _write_value_source(path: Path, rows: list[tuple], with_unit: bool) -> None:
    """rows: list of (Date, value)"""
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {"Date": [r[0] for r in rows], "value": [r[1] for r in rows]}
    schema = {"Date": pl.Date, "value": pl.Float32}
    if with_unit:
        data["unit"] = ["dollars per barrel"] * len(rows)
        schema["unit"] = pl.Utf8
    pl.DataFrame(data, schema=schema).write_parquet(path)


def _make_overview(rows: list[tuple[str, str, str]]) -> pl.DataFrame:
    """rows: list of (symbol, assetType, about)"""
    return pl.DataFrame(
        {
            "symbol": [r[0] for r in rows],
            "assetType": [r[1] for r in rows],
            "about": [r[2] for r in rows],
            "reportedDate": [None] * len(rows),
            "timeOfTheDay": [""] * len(rows),
            "sector": [""] * len(rows),
        },
        schema={
            "symbol": pl.Utf8, "assetType": pl.Utf8, "about": pl.Utf8,
            "reportedDate": pl.Date, "timeOfTheDay": pl.Utf8, "sector": pl.Utf8,
        },
    )


# ── _normalize_simple_source ─────────────────────────────────────────────────

def test_normalize_forex_fills_volume_with_null():
    src = pl.DataFrame(
        {"Date": [date(2020, 1, 1)], "Open": [1.0], "High": [1.0], "Low": [1.0], "Close": [1.0]},
        schema={"Date": pl.Date, "Open": pl.Float32, "High": pl.Float32, "Low": pl.Float32, "Close": pl.Float32},
    )
    out = _normalize_simple_source("forex", src)
    assert out.columns == ["Date", "Open", "High", "Low", "Close", "Volume"]
    assert out["Volume"][0] is None
    assert out["Volume"].dtype == pl.Float32


def test_normalize_cryptocurrencies_keeps_volume():
    src = pl.DataFrame(
        {"Date": [date(2020, 1, 1)], "Open": [1.0], "High": [1.0], "Low": [1.0], "Close": [1.0], "Volume": [42.0]},
        schema={"Date": pl.Date, "Open": pl.Float32, "High": pl.Float32, "Low": pl.Float32, "Close": pl.Float32, "Volume": pl.Float32},
    )
    out = _normalize_simple_source("cryptocurrencies", src)
    assert out["Volume"][0] == 42.0


def test_normalize_commodity_value_broadcast_to_ohlc():
    src = pl.DataFrame(
        {"Date": [date(2020, 1, 1)], "value": [73.5], "unit": ["dollars per barrel"]},
        schema={"Date": pl.Date, "value": pl.Float32, "unit": pl.Utf8},
    )
    out = _normalize_simple_source("commodities", src)
    assert out["Open"][0] == 73.5
    assert out["High"][0] == 73.5
    assert out["Low"][0] == 73.5
    assert out["Close"][0] == 73.5
    assert out["Volume"][0] is None
    assert "unit" not in out.columns


def test_normalize_economic_value_broadcast_to_ohlc():
    src = pl.DataFrame(
        {"Date": [date(2020, 1, 1)], "value": [3.5]},
        schema={"Date": pl.Date, "value": pl.Float32},
    )
    out = _normalize_simple_source("economic", src)
    assert out["Open"][0] == 3.5
    assert out["Close"][0] == 3.5


# ── transform_simple_price_daily: end-to-end happy path ──────────────────────

def test_forex_end_to_end(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"

    _write_forex_source(
        historical / "forex" / "forex_EURUSD.parquet",
        [
            (date(2020, 1, 1), 1.10, 1.11, 1.09, 1.105),
            (date(2020, 1, 2), 1.105, 1.12, 1.10, 1.115),
        ],
    )
    _write_forex_source(
        daily / "2026-04-01" / "forex" / "forex_EURUSD.parquet",
        [(date(2026, 4, 1), 1.20, 1.21, 1.19, 1.205)],
    )

    overview = _make_overview([("EURUSD", "forex", "Euro / US Dollar")])
    report = TransformationReport()

    n = transform_simple_price_daily(
        "forex", historical, daily, dest, overview, report,
    )
    assert n == 1

    inst = ForexData.load_from(symbol_dest_dir(dest, "forex", "EURUSD"))
    assert inst.ticker == "EURUSD"
    assert inst.about == "Euro / US Dollar"
    assert inst.price_daily.height == 3
    assert dict(inst.price_daily.schema) == SCHEMAS["price_daily"]
    # Volume null preserved (forex has no volume).
    assert inst.price_daily["Volume"].null_count() == 3


def test_indices_end_to_end(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"

    _write_forex_source(   # same shape as indices: Date,O,H,L,C
        historical / "indices" / "indices_SPX.parquet",
        [(date(2020, 1, 1), 3300.0, 3320.0, 3290.0, 3310.0)],
    )
    overview = _make_overview([("SPX", "indices", "S&P 500 Index")])
    report = TransformationReport()
    n = transform_simple_price_daily("indices", historical, daily, dest, overview, report)
    assert n == 1
    inst = IndexData.load_from(symbol_dest_dir(dest, "indices", "SPX"))
    assert inst.price_daily["Close"][0] == 3310.0


def test_cryptocurrencies_volume_kept(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"

    _write_crypto_source(
        historical / "cryptocurrencies" / "cryptocurrencies_BTC.parquet",
        [(date(2020, 1, 1), 7200.0, 7300.0, 7100.0, 7250.0, 12000.0)],
    )
    overview = _make_overview([("BTC", "cryptocurrencies", "Bitcoin")])
    report = TransformationReport()
    n = transform_simple_price_daily("cryptocurrencies", historical, daily, dest, overview, report)
    assert n == 1
    inst = CryptocurrenciesData.load_from(symbol_dest_dir(dest, "cryptocurrencies", "BTC"))
    assert inst.price_daily["Volume"][0] == 12000.0


def test_commodity_value_to_ohlc(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"

    _write_value_source(
        historical / "commodities" / "commodities_WTI.parquet",
        [(date(2020, 1, 1), 60.0), (date(2020, 1, 2), 61.5)],
        with_unit=True,
    )
    overview = _make_overview([("WTI", "commodities", "WTI Crude Oil")])
    report = TransformationReport()
    n = transform_simple_price_daily("commodities", historical, daily, dest, overview, report)
    assert n == 1
    inst = CommoditiesData.load_from(symbol_dest_dir(dest, "commodities", "WTI"))
    assert inst.price_daily.height == 2
    row = inst.price_daily.filter(pl.col("Date") == date(2020, 1, 2)).row(0, named=True)
    assert row["Open"] == row["High"] == row["Low"] == row["Close"] == 61.5
    assert row["Volume"] is None


def test_economic_value_to_ohlc(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"

    _write_value_source(
        historical / "economic" / "economic_CPI.parquet",
        [(date(2020, 1, 1), 258.0)],
        with_unit=False,
    )
    overview = _make_overview([("CPI", "economic", "Consumer Price Index")])
    report = TransformationReport()
    n = transform_simple_price_daily("economic", historical, daily, dest, overview, report)
    assert n == 1
    inst = EconomicData.load_from(symbol_dest_dir(dest, "economic", "CPI"))
    assert inst.price_daily["Close"][0] == 258.0


# ── Concat across historical + multiple daily folders ────────────────────────

def test_historical_plus_two_daily_folders_concat(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"

    _write_forex_source(
        historical / "forex" / "forex_EURUSD.parquet",
        [(date(2020, 1, 1), 1.10, 1.11, 1.09, 1.105)],
    )
    _write_forex_source(
        daily / "2026-04-01" / "forex" / "forex_EURUSD.parquet",
        [(date(2026, 4, 1), 1.20, 1.21, 1.19, 1.205)],
    )
    _write_forex_source(
        daily / "2026-04-02" / "forex" / "forex_EURUSD.parquet",
        [(date(2026, 4, 2), 1.21, 1.22, 1.20, 1.215)],
    )

    overview = _make_overview([("EURUSD", "forex", "")])
    report = TransformationReport()
    transform_simple_price_daily("forex", historical, daily, dest, overview, report)

    inst = ForexData.load_from(symbol_dest_dir(dest, "forex", "EURUSD"))
    assert inst.price_daily.height == 3
    dates = inst.price_daily["Date"].to_list()
    assert dates == sorted(dates)


# ── Dedup discrepancy logging end-to-end ─────────────────────────────────────

def test_overlap_under_1pct_logged_and_daily_wins(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"

    # Historic extends through 2026-04-02 so the 2026-04-01 overlap is an
    # *interior* date, not the historic boundary -- the boundary-suppression
    # rule for price frames silences discrepancies on max(historic Date) only.
    _write_forex_source(
        historical / "forex" / "forex_EURUSD.parquet",
        [
            (date(2026, 4, 1), 1.10, 1.11, 1.09, 1.100),
            (date(2026, 4, 2), 1.11, 1.12, 1.10, 1.110),
        ],
    )
    _write_forex_source(
        daily / "2026-04-01" / "forex" / "forex_EURUSD.parquet",
        [(date(2026, 4, 1), 1.10, 1.11, 1.09, 1.105)],  # Close differs 0.45%
    )

    overview = _make_overview([("EURUSD", "forex", "")])
    report = TransformationReport()
    transform_simple_price_daily("forex", historical, daily, dest, overview, report)

    inst = ForexData.load_from(symbol_dest_dir(dest, "forex", "EURUSD"))
    # 2 deduped rows, daily snapshot wins on the overlapping 04-01 date.
    assert inst.price_daily.height == 2
    apr1 = inst.price_daily.filter(pl.col("Date") == date(2026, 4, 1))
    assert apr1["Close"][0] == pytest.approx(1.105, rel=1e-4)

    rep = report.to_frame()
    assert rep.height == 1
    assert rep["issue_type"][0] == "dedup_value_discrepancy_under_1pct"
    assert rep["count"][0] == 1


def test_overlap_over_1pct_logged_and_daily_wins(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"

    # 04-02 anchors the historic boundary; the 04-01 overlap is interior.
    _write_forex_source(
        historical / "forex" / "forex_EURUSD.parquet",
        [
            (date(2026, 4, 1), 1.10, 1.11, 1.09, 1.100),
            (date(2026, 4, 2), 1.11, 1.12, 1.10, 1.110),
        ],
    )
    _write_forex_source(
        daily / "2026-04-01" / "forex" / "forex_EURUSD.parquet",
        [(date(2026, 4, 1), 1.10, 1.11, 1.09, 1.500)],  # Close differs 26%
    )
    overview = _make_overview([("EURUSD", "forex", "")])
    report = TransformationReport()
    transform_simple_price_daily("forex", historical, daily, dest, overview, report)

    inst = ForexData.load_from(symbol_dest_dir(dest, "forex", "EURUSD"))
    apr1 = inst.price_daily.filter(pl.col("Date") == date(2026, 4, 1))
    assert apr1["Close"][0] == pytest.approx(1.500, rel=1e-4)

    rep = report.to_frame()
    assert rep.filter(pl.col("issue_type") == "dedup_value_discrepancy_over_1pct").height == 1


# ── Null OHLC drop ───────────────────────────────────────────────────────────

def test_null_ohlc_row_dropped_and_logged(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"

    _write_forex_source(
        historical / "forex" / "forex_EURUSD.parquet",
        [
            (date(2020, 1, 1), 1.10, 1.11, 1.09, 1.105),
            (date(2020, 1, 2), 1.105, 1.12, None, 1.115),  # Low null
            (date(2020, 1, 3), 1.115, None, 1.10, 1.120),  # High null
        ],
    )
    overview = _make_overview([("EURUSD", "forex", "")])
    report = TransformationReport()
    transform_simple_price_daily("forex", historical, daily, dest, overview, report)

    inst = ForexData.load_from(symbol_dest_dir(dest, "forex", "EURUSD"))
    assert inst.price_daily.height == 1

    rep = report.to_frame().filter(pl.col("issue_type") == "dedup_dropped_null_row")
    assert rep.height == 1
    assert rep["count"][0] == 2


def test_volume_null_does_not_drop_row(tmp_path):
    """Cryptocurrency with explicit null Volume should keep the row."""
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"

    _write_crypto_source(
        historical / "cryptocurrencies" / "cryptocurrencies_BTC.parquet",
        [(date(2020, 1, 1), 7200.0, 7300.0, 7100.0, 7250.0, None)],
    )
    overview = _make_overview([("BTC", "cryptocurrencies", "")])
    report = TransformationReport()
    transform_simple_price_daily("cryptocurrencies", historical, daily, dest, overview, report)

    inst = CryptocurrenciesData.load_from(symbol_dest_dir(dest, "cryptocurrencies", "BTC"))
    assert inst.price_daily.height == 1
    assert inst.price_daily["Volume"][0] is None


# ── Resume / skip ────────────────────────────────────────────────────────────

def test_resume_skips_already_transformed_symbol(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"

    _write_forex_source(
        historical / "forex" / "forex_EURUSD.parquet",
        [(date(2020, 1, 1), 1.10, 1.11, 1.09, 1.105)],
    )
    overview = _make_overview([("EURUSD", "forex", "")])

    # First run writes the symbol.
    transform_simple_price_daily("forex", historical, daily, dest, overview, TransformationReport())
    assert is_already_transformed(dest, "forex", "EURUSD")

    # Mutate the source so a fresh build would produce different output.
    _write_forex_source(
        historical / "forex" / "forex_EURUSD.parquet",
        [(date(2020, 1, 1), 99.0, 99.0, 99.0, 99.0)],
    )

    # Second run must skip (no overwrite).
    transform_simple_price_daily("forex", historical, daily, dest, overview, TransformationReport())
    inst = ForexData.load_from(symbol_dest_dir(dest, "forex", "EURUSD"))
    assert inst.price_daily["Close"][0] == pytest.approx(1.105, rel=1e-4)


# ── Symbol filtering & catalog gating ────────────────────────────────────────

def test_symbols_filter(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"

    for sym in ("EURUSD", "GBPUSD"):
        _write_forex_source(
            historical / "forex" / f"forex_{sym}.parquet",
            [(date(2020, 1, 1), 1.10, 1.11, 1.09, 1.105)],
        )
    overview = _make_overview([
        ("EURUSD", "forex", ""),
        ("GBPUSD", "forex", ""),
    ])
    report = TransformationReport()
    n = transform_simple_price_daily(
        "forex", historical, daily, dest, overview, report,
        symbols_filter={"EURUSD"},
    )
    assert n == 1
    assert is_already_transformed(dest, "forex", "EURUSD")
    assert not is_already_transformed(dest, "forex", "GBPUSD")


def test_source_present_but_no_overview_entry_skipped(tmp_path):
    """A symbol with source files but absent from the catalog is skipped."""
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"

    _write_forex_source(
        historical / "forex" / "forex_EURUSD.parquet",
        [(date(2020, 1, 1), 1.10, 1.11, 1.09, 1.105)],
    )
    overview = _make_overview([])  # no rows
    report = TransformationReport()
    n = transform_simple_price_daily("forex", historical, daily, dest, overview, report)
    assert n == 0
    assert not is_already_transformed(dest, "forex", "EURUSD")


# ── metadata.json content ────────────────────────────────────────────────────

def test_metadata_json_carries_ticker_and_about(tmp_path):
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    dest = tmp_path / "transformed"
    _write_forex_source(
        historical / "forex" / "forex_EURUSD.parquet",
        [(date(2020, 1, 1), 1.10, 1.11, 1.09, 1.105)],
    )
    overview = _make_overview([("EURUSD", "forex", "Euro / USD")])
    transform_simple_price_daily("forex", historical, daily, dest, overview, TransformationReport())
    md = json.loads((symbol_dest_dir(dest, "forex", "EURUSD") / "metadata.json").read_text())
    assert md["_asset_type"] == "ForexData"
    assert md["ticker"] == "EURUSD"
    assert md["about"] == "Euro / USD"


# ── Dispatch guard ───────────────────────────────────────────────────────────

def test_unsupported_asset_type_rejected(tmp_path):
    overview = _make_overview([])
    with pytest.raises(ValueError, match="does not handle"):
        transform_simple_price_daily(
            "stocks", tmp_path, tmp_path, tmp_path, overview, TransformationReport(),
        )
