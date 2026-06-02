"""Unit tests for monitoring_service.analyze_files.

Covers both the structural file count (``analyze_files``) and the cheap
storage rollup (``analyze_storage``). The interesting logic lives in the
per-endpoint counting rules -- fundamentals collapse annual+quarterly into
one symbol, sentiment ignores the shared ALL_MESSAGES file, and direct
endpoints write straight under ``<asset_type>/`` rather than a nested
``<asset_type>/<endpoint>/`` -- and in the expected-count derivation, which
filters the catalog by the per-symbol yield_status column.
"""

import shutil
import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from monitoring_service.analyze_files import analyze_files, analyze_storage

MOCK_DIR = Path(__file__).parent / "mock_files"


@pytest.fixture
def dirs():
    if MOCK_DIR.exists():
        shutil.rmtree(MOCK_DIR)
    folder = MOCK_DIR / "folder"
    catalog = MOCK_DIR / "catalog"
    folder.mkdir(parents=True)
    catalog.mkdir(parents=True)
    yield folder, catalog
    shutil.rmtree(MOCK_DIR)


def _touch_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"x": [1]}).write_parquet(path)


def _write_catalog(catalog_dir: Path, asset_type: str, symbols: list[str]) -> None:
    pl.DataFrame({"symbol": symbols}).write_parquet(
        catalog_dir / f"{asset_type}.parquet"
    )


# ---------------------------------------------------------------------------
# analyze_storage
# ---------------------------------------------------------------------------

def test_storage_missing_folder():
    out = analyze_storage(MOCK_DIR / "does_not_exist")
    assert out == {"missing": True, "bytes": 0, "file_count": 0}


def test_storage_counts_files_and_bytes(dirs):
    folder, _ = dirs
    _touch_parquet(folder / "stocks" / "prices" / "AAPL.parquet")
    _touch_parquet(folder / "stocks" / "prices" / "MSFT.parquet")
    out = analyze_storage(folder)
    assert out["file_count"] == 2
    assert out["bytes"] > 0
    assert "missing" not in out


# ---------------------------------------------------------------------------
# analyze_files: per-endpoint counting rules
# ---------------------------------------------------------------------------

def test_regular_endpoint_counts_one_per_file(dirs):
    folder, catalog = dirs
    _write_catalog(catalog, "stocks", ["AAPL", "MSFT", "GOOG"])
    _touch_parquet(folder / "stocks" / "prices" / "AAPL.parquet")
    _touch_parquet(folder / "stocks" / "prices" / "MSFT.parquet")

    out = analyze_files(folder, catalog)
    entry = out["stocks"]["prices"]
    assert entry["files_written"] == 2
    # No yield_status present -> expected is the full catalog.
    assert entry["expected"] == 3
    assert entry["ratio"] == pytest.approx(2 / 3, abs=1e-4)


def test_fundamental_endpoint_collapses_annual_and_quarterly(dirs):
    folder, catalog = dirs
    _write_catalog(catalog, "stocks", ["AAPL", "MSFT"])
    # AAPL contributes two files but counts as one symbol; MSFT only annual.
    _touch_parquet(folder / "stocks" / "income_statement" / "AAPL_annual.parquet")
    _touch_parquet(folder / "stocks" / "income_statement" / "AAPL_quarterly.parquet")
    _touch_parquet(folder / "stocks" / "income_statement" / "MSFT_annual.parquet")

    out = analyze_files(folder, catalog)
    assert out["stocks"]["income_statement"]["files_written"] == 2


def test_sentiment_ignores_all_messages_file(dirs):
    folder, catalog = dirs
    _write_catalog(catalog, "stocks", ["AAPL", "MSFT"])
    _touch_parquet(folder / "stocks" / "sentiment" / "ALL_MESSAGES.parquet")
    _touch_parquet(folder / "stocks" / "sentiment" / "AAPL.parquet")
    _touch_parquet(folder / "stocks" / "sentiment" / "MSFT.parquet")

    out = analyze_files(folder, catalog)
    assert out["stocks"]["sentiment"]["files_written"] == 2


def test_direct_endpoint_reads_flat_layout(dirs):
    folder, catalog = dirs
    _write_catalog(catalog, "commodities", ["WTI", "BRENT"])
    # Direct endpoints write SYMBOL.parquet straight under <asset_type>/.
    _touch_parquet(folder / "commodities" / "WTI.parquet")
    _touch_parquet(folder / "commodities" / "BRENT.parquet")

    out = analyze_files(folder, catalog)
    assert out["commodities"]["commodities"]["files_written"] == 2
    assert out["commodities"]["commodities"]["expected"] == 2


def test_missing_endpoint_folder_counts_zero(dirs):
    folder, catalog = dirs
    _write_catalog(catalog, "stocks", ["AAPL"])
    out = analyze_files(folder, catalog)
    entry = out["stocks"]["prices"]
    assert entry["files_written"] == 0
    # Zero expected stays unset for ratio (avoids 0/N noise) but expected known.
    assert entry["expected"] == 1
    assert "ratio" in entry  # expected truthy -> ratio computed (0.0)
    assert entry["ratio"] == 0.0


# ---------------------------------------------------------------------------
# expected-count derivation
# ---------------------------------------------------------------------------

def test_missing_catalog_yields_none_expected(dirs):
    folder, catalog = dirs
    # No stocks.parquet written.
    _touch_parquet(folder / "stocks" / "prices" / "AAPL.parquet")
    out = analyze_files(folder, catalog)
    entry = out["stocks"]["prices"]
    assert entry["expected"] is None
    assert "ratio" not in entry  # None expected -> no ratio


def test_yield_status_filters_expected_for_yield_column_endpoint(dirs):
    folder, catalog = dirs
    _write_catalog(catalog, "stocks", ["AAPL", "MSFT", "GOOG"])
    # Only AAPL and MSFT yield for prices; GOOG is False and should be excluded
    # from the expected denominator.
    pl.DataFrame(
        {
            "symbol": ["AAPL", "MSFT", "GOOG"],
            "prices": [True, True, False],
        },
        schema={"symbol": pl.Utf8, "prices": pl.Boolean},
    ).write_parquet(catalog / "yield_status.parquet")

    _touch_parquet(folder / "stocks" / "prices" / "AAPL.parquet")
    _touch_parquet(folder / "stocks" / "prices" / "MSFT.parquet")

    out = analyze_files(folder, catalog)
    entry = out["stocks"]["prices"]
    assert entry["expected"] == 2
    assert entry["ratio"] == 1.0


def test_yield_status_direct_endpoint_uses_direct_column(dirs):
    folder, catalog = dirs
    _write_catalog(catalog, "forex", ["EUR", "GBP", "JPY"])
    pl.DataFrame(
        {
            "symbol": ["EUR", "GBP", "JPY"],
            "direct": [True, False, True],
        },
        schema={"symbol": pl.Utf8, "direct": pl.Boolean},
    ).write_parquet(catalog / "yield_status.parquet")

    out = analyze_files(folder, catalog)
    # EUR + JPY yield => expected 2 (no files written here).
    assert out["forex"]["forex"]["expected"] == 2
