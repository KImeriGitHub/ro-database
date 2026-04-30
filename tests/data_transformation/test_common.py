"""Tests for data_transformation/_common.py: source enumeration, sector
lookup, schema-strict casting, transformation report.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from data_transformation._common import (
    ASSET_TYPES,
    FLAT_ASSET_TYPES,
    REPORT_SCHEMA,
    TransformationReport,
    build_source_index,
    cast_to_schema,
    enumerate_daily_dates,
    is_already_transformed,
    sector_to_index,
    symbol_dest_dir,
    symbol_dirname,
)
from data_transformation.AssetData import CANONICAL_SECTORS


# ── sector_to_index ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("name,expected", [
    ("Technology", CANONICAL_SECTORS.index("Technology")),
    ("Healthcare", CANONICAL_SECTORS.index("Healthcare")),
    ("Other", CANONICAL_SECTORS.index("Other")),
])
def test_sector_to_index_known(name, expected):
    assert sector_to_index(name) == expected


@pytest.mark.parametrize("name", ["", None, "Made up sector"])
def test_sector_to_index_unknown_returns_other(name):
    assert sector_to_index(name) == CANONICAL_SECTORS.index("Other")


# ── symbol_dirname ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("symbol", ["AAPL", "CON", "PRN", "NUL", "COM1", "LPT9"])
def test_symbol_dirname_prefixes_with_data(symbol):
    """Even Windows-reserved tickers are safe under the data_ prefix."""
    assert symbol_dirname(symbol) == f"data_{symbol}"


def test_symbol_dest_dir_layout(tmp_path):
    p = symbol_dest_dir(tmp_path, "stocks", "AAPL")
    assert p == tmp_path / "stocks" / "data_AAPL"


def test_is_already_transformed(tmp_path):
    sym_dir = symbol_dest_dir(tmp_path, "stocks", "AAPL")
    assert not is_already_transformed(tmp_path, "stocks", "AAPL")
    sym_dir.mkdir(parents=True)
    (sym_dir / "metadata.json").write_text("{}")
    assert is_already_transformed(tmp_path, "stocks", "AAPL")


# ── enumerate_daily_dates ─────────────────────────────────────────────────────

def test_enumerate_daily_dates_sorted_and_filtered(tmp_path):
    (tmp_path / "2026-01-15").mkdir()
    (tmp_path / "2026-01-03").mkdir()
    (tmp_path / "2026-02-01").mkdir()
    (tmp_path / "not-a-date").mkdir()
    (tmp_path / "2026-13-99").mkdir()  # parses by regex but fails fromisoformat
    (tmp_path / "afile").write_text("ignore me")
    out = enumerate_daily_dates(tmp_path)
    assert out == [date(2026, 1, 3), date(2026, 1, 15), date(2026, 2, 1)]


def test_enumerate_daily_dates_missing_dir(tmp_path):
    assert enumerate_daily_dates(tmp_path / "does_not_exist") == []


# ── build_source_index ────────────────────────────────────────────────────────

def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")


def test_build_source_index_nested_asset_type(tmp_path):
    """stocks/etfs use historical/<a>/<endpoint>/<prefix><sym>.parquet."""
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    _touch(historical / "stocks" / "prices_daily" / "stocks_AAPL.parquet")
    _touch(historical / "stocks" / "prices_daily" / "stocks_MSFT.parquet")
    _touch(daily / "2026-04-01" / "stocks" / "prices_daily" / "stocks_AAPL.parquet")
    _touch(daily / "2026-04-02" / "stocks" / "prices_daily" / "stocks_AAPL.parquet")
    _touch(daily / "2026-04-02" / "stocks" / "prices_daily" / "stocks_NVDA.parquet")

    idx = build_source_index(historical, daily, "stocks", "prices_daily")

    assert set(idx.keys()) == {"AAPL", "MSFT", "NVDA"}
    # Historical first, then daily folders sorted ascending.
    aapl = idx["AAPL"]
    assert aapl[0] == historical / "stocks" / "prices_daily" / "stocks_AAPL.parquet"
    assert aapl[1] == daily / "2026-04-01" / "stocks" / "prices_daily" / "stocks_AAPL.parquet"
    assert aapl[2] == daily / "2026-04-02" / "stocks" / "prices_daily" / "stocks_AAPL.parquet"
    # MSFT only in historical.
    assert idx["MSFT"] == [historical / "stocks" / "prices_daily" / "stocks_MSFT.parquet"]
    # NVDA only in daily.
    assert idx["NVDA"] == [daily / "2026-04-02" / "stocks" / "prices_daily" / "stocks_NVDA.parquet"]


def test_build_source_index_flat_asset_type(tmp_path):
    """forex et al use historical/<a>/<prefix><sym>.parquet (no endpoint subdir)."""
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    _touch(historical / "forex" / "forex_EURUSD.parquet")
    _touch(daily / "2026-04-01" / "forex" / "forex_EURUSD.parquet")

    idx = build_source_index(historical, daily, "forex", endpoint=None)
    assert list(idx.keys()) == ["EURUSD"]
    assert len(idx["EURUSD"]) == 2


def test_build_source_index_ignores_other_asset_type_files(tmp_path):
    """A file in stocks/prices_daily/ that isn't prefixed stocks_ is ignored."""
    historical = tmp_path / "historical"
    _touch(historical / "stocks" / "prices_daily" / "stocks_AAPL.parquet")
    _touch(historical / "stocks" / "prices_daily" / "etfs_SPY.parquet")  # wrong prefix
    _touch(historical / "stocks" / "prices_daily" / "stocks_AAPL.txt")  # wrong ext

    idx = build_source_index(historical, tmp_path / "daily", "stocks", "prices_daily")
    assert list(idx.keys()) == ["AAPL"]


def test_build_source_index_suffix(tmp_path):
    """Fundamentals use _annual / _quarterly suffixes."""
    historical = tmp_path / "historical"
    _touch(historical / "stocks" / "earnings" / "stocks_AAPL_annual.parquet")
    _touch(historical / "stocks" / "earnings" / "stocks_AAPL_quarterly.parquet")

    annual_idx = build_source_index(
        historical, tmp_path / "daily", "stocks", "earnings", suffix="_annual"
    )
    assert annual_idx == {"AAPL": [historical / "stocks" / "earnings" / "stocks_AAPL_annual.parquet"]}


def test_build_source_index_flat_requires_no_endpoint_arg_meaning(tmp_path):
    """Passing endpoint to a flat asset_type is harmless (dir becomes flat)."""
    historical = tmp_path / "historical"
    _touch(historical / "forex" / "forex_EURUSD.parquet")
    # endpoint is ignored for flat asset_types
    idx = build_source_index(historical, tmp_path / "daily", "forex", "anything")
    assert list(idx.keys()) == ["EURUSD"]


# ── cast_to_schema ────────────────────────────────────────────────────────────

def test_cast_to_schema_happy_path():
    schema = {"a": pl.Int32, "b": pl.Float32}
    df = pl.DataFrame({"a": [1, 2], "b": [1.0, 2.0]})
    out = cast_to_schema(df, schema, "test")
    assert dict(out.schema) == schema
    assert out.columns == ["a", "b"]


def test_cast_to_schema_drops_extra_columns():
    schema = {"a": pl.Int32}
    df = pl.DataFrame({"a": [1], "b": ["junk"]})
    out = cast_to_schema(df, schema, "test")
    assert out.columns == ["a"]


def test_cast_to_schema_missing_required_column_raises():
    with pytest.raises(ValueError, match="missing required columns"):
        cast_to_schema(pl.DataFrame({"a": [1]}), {"a": pl.Int32, "b": pl.Float32}, "test")


def test_cast_to_schema_dtype_drift_raises():
    """An incompatible dtype must propagate, not silently coerce."""
    schema = {"a": pl.Int32}
    df = pl.DataFrame({"a": ["not an int"]})
    with pytest.raises(Exception):
        cast_to_schema(df, schema, "test")


# ── TransformationReport ──────────────────────────────────────────────────────

def test_transformation_report_record_and_flush(tmp_path):
    r = TransformationReport()
    r.record("AAPL", "stocks", "shareprice_daily", "dedup_dropped_null_row", count=3, relative=0.001, detail="3 rows had null Volume")
    r.record("EURUSD", "forex", "price_daily", "dedup_value_discrepancy_under_1pct", count=1, detail="Close 1.10 vs 1.101")

    frame = r.to_frame()
    assert frame.height == 2
    assert dict(frame.schema) == REPORT_SCHEMA
    assert set(frame["symbol"].to_list()) == {"AAPL", "EURUSD"}

    out = r.flush(tmp_path / "dest")
    assert out.exists()
    reloaded = pl.read_parquet(out)
    assert reloaded.height == 2
    assert dict(reloaded.schema) == REPORT_SCHEMA


def test_transformation_report_empty_flush_writes_empty_frame(tmp_path):
    r = TransformationReport()
    out = r.flush(tmp_path / "dest")
    df = pl.read_parquet(out)
    assert df.height == 0
    assert dict(df.schema) == REPORT_SCHEMA


def test_transformation_report_unknown_issue_type_raises():
    r = TransformationReport()
    with pytest.raises(ValueError, match="unknown issue_type"):
        r.record("AAPL", "stocks", "shareprice_daily", "made_up", count=1)


# ── Module-level constants ────────────────────────────────────────────────────

def test_asset_types_covers_all_seven():
    assert set(ASSET_TYPES) == {
        "stocks", "etfs", "forex", "indices",
        "cryptocurrencies", "commodities", "economic",
    }


def test_flat_asset_types_excludes_stocks_etfs():
    assert "stocks" not in FLAT_ASSET_TYPES
    assert "etfs" not in FLAT_ASSET_TYPES
    assert FLAT_ASSET_TYPES == frozenset({
        "forex", "indices", "cryptocurrencies", "commodities", "economic",
    })
