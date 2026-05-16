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
    load_metadata,
    paths_for_mode,
    resolve_mode,
    sector_to_index,
    snapshot_date_from_path,
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


def test_symbol_dirname_handles_slash_class_ticker():
    """Slash-class tickers like ``BC/PB`` must collapse to a single
    directory-name component (``data_BC%2FPB``) so they cannot split into
    a ``data_BC/`` parent and a ``PB`` child the way an unencoded symbol
    would."""
    name = symbol_dirname("BC/PB")
    assert name == "data_BC%2FPB"
    assert "/" not in name and "\\" not in name


def test_symbol_dest_dir_layout(tmp_path):
    p = symbol_dest_dir(tmp_path, "stocks", "AAPL")
    assert p == tmp_path / "stocks" / "data_AAPL"


def test_symbol_dest_dir_writes_one_dir_for_slash_ticker(tmp_path):
    """End-to-end: a slash-class symbol produces exactly one directory
    under the asset-type root, whose ``metadata.json`` round-trips
    through ``is_already_transformed``."""
    sym_dir = symbol_dest_dir(tmp_path, "stocks", "BC/PB")
    sym_dir.mkdir(parents=True)
    (sym_dir / "metadata.json").write_text("{}")
    children = list((tmp_path / "stocks").iterdir())
    assert children == [sym_dir]
    assert is_already_transformed(tmp_path, "stocks", "BC/PB")


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


def test_build_source_index_decodes_slash_class_symbol(tmp_path):
    """Files written by ``symbol_parquet_name`` for a slash-class ticker
    are stored as ``stocks_BC%2FPB.parquet`` on disk. ``build_source_index``
    must recover the canonical ``BC/PB`` ticker as the dict key, otherwise
    downstream lookups (which use the original symbol from the catalog)
    miss the file."""
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    _touch(historical / "stocks" / "prices_daily" / "stocks_BC%2FPB.parquet")
    _touch(daily / "2026-04-01" / "stocks" / "prices_daily" / "stocks_BC%2FPB.parquet")

    idx = build_source_index(historical, daily, "stocks", "prices_daily")

    assert list(idx.keys()) == ["BC/PB"]
    assert len(idx["BC/PB"]) == 2


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


# ── snapshot_date_from_path ───────────────────────────────────────────────────

def test_snapshot_date_from_path_nested_layout(tmp_path):
    """Nested asset_type: daily/<d>/<asset_type>/<endpoint>/file.parquet."""
    p = (tmp_path / "daily" / "2026-05-15" / "stocks" / "prices_daily"
         / "stocks_AAPL.parquet")
    assert snapshot_date_from_path(p) == date(2026, 5, 15)


def test_snapshot_date_from_path_flat_layout(tmp_path):
    """Flat asset_type: daily/<d>/<asset_type>/file.parquet."""
    p = tmp_path / "daily" / "2026-05-15" / "forex" / "forex_EURUSD.parquet"
    assert snapshot_date_from_path(p) == date(2026, 5, 15)


def test_snapshot_date_from_path_historical_returns_none(tmp_path):
    """Historical paths have no YYYY-MM-DD ancestor."""
    p = (tmp_path / "historical" / "stocks" / "prices_daily"
         / "stocks_AAPL.parquet")
    assert snapshot_date_from_path(p) is None


def test_snapshot_date_from_path_invalid_date_dir_returns_none(tmp_path):
    """A regex-matching but unparseable date (2026-13-99) is rejected."""
    p = tmp_path / "daily" / "2026-13-99" / "stocks" / "stocks_AAPL.parquet"
    assert snapshot_date_from_path(p) is None


# ── load_metadata ─────────────────────────────────────────────────────────────

def test_load_metadata_returns_none_when_absent(tmp_path):
    assert load_metadata(tmp_path) is None


def test_load_metadata_returns_dict(tmp_path):
    (tmp_path / "metadata.json").write_text(
        '{"ticker": "AAPL", "last_processed_daily_date": "2026-05-15"}'
    )
    meta = load_metadata(tmp_path)
    assert meta == {"ticker": "AAPL", "last_processed_daily_date": "2026-05-15"}


def test_load_metadata_returns_none_on_invalid_json(tmp_path):
    (tmp_path / "metadata.json").write_text("not json at all{")
    assert load_metadata(tmp_path) is None


# ── resolve_mode ──────────────────────────────────────────────────────────────

def test_resolve_mode_fresh_when_no_metadata(tmp_path):
    mode, since = resolve_mode(tmp_path, [date(2026, 5, 15)])
    assert mode == "fresh"
    assert since is None


def test_resolve_mode_fresh_when_field_null(tmp_path):
    (tmp_path / "metadata.json").write_text(
        '{"ticker": "AAPL", "last_processed_daily_date": null}'
    )
    mode, since = resolve_mode(tmp_path, [date(2026, 5, 15)])
    assert mode == "fresh"
    assert since is None


def test_resolve_mode_fresh_when_field_missing(tmp_path):
    """An older metadata.json predating this change has no field at all."""
    (tmp_path / "metadata.json").write_text('{"ticker": "AAPL"}')
    mode, since = resolve_mode(tmp_path, [date(2026, 5, 15)])
    assert mode == "fresh"
    assert since is None


def test_resolve_mode_fresh_on_unparseable_date(tmp_path):
    (tmp_path / "metadata.json").write_text(
        '{"ticker": "AAPL", "last_processed_daily_date": "not-a-date"}'
    )
    mode, since = resolve_mode(tmp_path, [date(2026, 5, 15)])
    assert mode == "fresh"
    assert since is None


def test_resolve_mode_skip_when_cache_covers_latest(tmp_path):
    (tmp_path / "metadata.json").write_text(
        '{"ticker": "AAPL", "last_processed_daily_date": "2026-05-15"}'
    )
    mode, since = resolve_mode(
        tmp_path, [date(2026, 5, 14), date(2026, 5, 15)]
    )
    assert mode == "skip"
    assert since == date(2026, 5, 15)


def test_resolve_mode_skip_when_cache_ahead_of_daily(tmp_path):
    """A cache date strictly greater than max(daily_dates) -- unusual but
    legal (e.g. a daily folder was removed) -- still routes to skip."""
    (tmp_path / "metadata.json").write_text(
        '{"ticker": "AAPL", "last_processed_daily_date": "2026-06-01"}'
    )
    mode, since = resolve_mode(tmp_path, [date(2026, 5, 15)])
    assert mode == "skip"
    assert since == date(2026, 6, 1)


def test_resolve_mode_skip_when_no_daily_dates(tmp_path):
    """No daily folders at all -> nothing new to do; skip."""
    (tmp_path / "metadata.json").write_text(
        '{"ticker": "AAPL", "last_processed_daily_date": "2026-05-15"}'
    )
    mode, since = resolve_mode(tmp_path, [])
    assert mode == "skip"
    assert since == date(2026, 5, 15)


def test_resolve_mode_incremental_when_newer_daily(tmp_path):
    (tmp_path / "metadata.json").write_text(
        '{"ticker": "AAPL", "last_processed_daily_date": "2026-05-14"}'
    )
    mode, since = resolve_mode(
        tmp_path, [date(2026, 5, 14), date(2026, 5, 15)]
    )
    assert mode == "incremental"
    assert since == date(2026, 5, 14)


# ── paths_for_mode ────────────────────────────────────────────────────────────

def test_paths_for_mode_fresh_returns_unchanged(tmp_path):
    paths = [
        tmp_path / "historical" / "stocks" / "prices_daily" / "stocks_AAPL.parquet",
        tmp_path / "daily" / "2026-05-14" / "stocks" / "prices_daily" / "stocks_AAPL.parquet",
        tmp_path / "daily" / "2026-05-15" / "stocks" / "prices_daily" / "stocks_AAPL.parquet",
    ]
    assert paths_for_mode(paths, "fresh", None) == paths
    assert paths_for_mode(paths, "fresh", date(2026, 5, 14)) == paths


def test_paths_for_mode_skip_returns_empty(tmp_path):
    paths = [tmp_path / "daily" / "2026-05-15" / "stocks" / "stocks_AAPL.parquet"]
    assert paths_for_mode(paths, "skip", date(2026, 5, 15)) == []


def test_paths_for_mode_incremental_filters_to_newer_daily_only(tmp_path):
    historical = tmp_path / "historical" / "stocks" / "prices_daily" / "stocks_AAPL.parquet"
    d14 = tmp_path / "daily" / "2026-05-14" / "stocks" / "prices_daily" / "stocks_AAPL.parquet"
    d15 = tmp_path / "daily" / "2026-05-15" / "stocks" / "prices_daily" / "stocks_AAPL.parquet"
    d16 = tmp_path / "daily" / "2026-05-16" / "stocks" / "prices_daily" / "stocks_AAPL.parquet"
    paths = [historical, d14, d15, d16]
    # since_date = 2026-05-14 -> keep only d15 and d16 (strictly greater).
    out = paths_for_mode(paths, "incremental", date(2026, 5, 14))
    assert out == [d15, d16]


def test_paths_for_mode_incremental_with_none_since_date_returns_paths(tmp_path):
    """Defensive: incremental with None since_date is unusual but should
    not crash; return paths unchanged."""
    paths = [tmp_path / "daily" / "2026-05-15" / "x.parquet"]
    assert paths_for_mode(paths, "incremental", None) == paths


def test_paths_for_mode_incremental_keep_historical(tmp_path):
    """``keep_historical=True`` retains paths with no daily/<d>/ ancestor
    (the financials builder needs them for PIT fallback)."""
    historical = tmp_path / "historical" / "stocks" / "earnings" / "stocks_AAPL_quarterly.parquet"
    d14 = tmp_path / "daily" / "2026-05-14" / "stocks" / "earnings" / "stocks_AAPL_quarterly.parquet"
    d15 = tmp_path / "daily" / "2026-05-15" / "stocks" / "earnings" / "stocks_AAPL_quarterly.parquet"
    paths = [historical, d14, d15]
    out = paths_for_mode(
        paths, "incremental", date(2026, 5, 14), keep_historical=True,
    )
    assert out == [historical, d15]
    out_no_hist = paths_for_mode(
        paths, "incremental", date(2026, 5, 14),
    )
    assert out_no_hist == [d15]


def test_paths_for_mode_unknown_mode_raises(tmp_path):
    with pytest.raises(ValueError, match="unknown mode"):
        paths_for_mode([], "garbage", None)
