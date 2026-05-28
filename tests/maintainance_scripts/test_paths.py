"""Tests for ``maintainance_scripts.paths``.

The path helpers are the single source of truth for translating between local
paths and ``gs://`` blob names. Both sides of the pipeline (Cloud Run
container and local workstation) depend on this contract being symmetric, so
the tests assert ``to_gcs_blob_name`` and ``to_local_path`` round-trip cleanly
across all three top-level trees.

Pure unit tests. No GCP, no real filesystem outside ``tmp_path``.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from maintainance_scripts import paths


# ---------------------------------------------------------------------------
# Local path helpers
# ---------------------------------------------------------------------------


def test_local_paths_default_to_project_root_when_root_omitted():
    """When ``root=None`` the helpers must use ``settings.PROJECT_ROOT`` --
    that's the documented default for production scripts."""
    from config import settings
    expected_root = settings.PROJECT_ROOT
    assert paths.local_catalog_dir() == expected_root / "catalog"
    assert paths.local_historical_dir() == expected_root / "historical"
    assert paths.local_daily_dir() == expected_root / "daily"
    assert paths.local_transformed_dir() == expected_root / "transformed"


def test_local_paths_respect_custom_root(tmp_path):
    """Tests and integration scripts pass an explicit root (e.g.
    ``tests/integration_tests/database/``); the helpers must not silently
    rebase to PROJECT_ROOT."""
    assert paths.local_catalog_dir(tmp_path) == tmp_path / "catalog"
    assert paths.local_historical_dir(tmp_path) == tmp_path / "historical"
    assert paths.local_daily_dir(tmp_path) == tmp_path / "daily"
    assert paths.local_transformed_dir(tmp_path) == tmp_path / "transformed"


def test_local_daily_date_dir_isoformats_date(tmp_path):
    out = paths.local_daily_date_dir(date(2026, 4, 18), tmp_path)
    assert out == tmp_path / "daily" / "2026-04-18"


# ---------------------------------------------------------------------------
# Configured local roots (secrets/dir_location.txt)
# ---------------------------------------------------------------------------


def test_configured_paths_fall_back_when_file_missing(monkeypatch, tmp_path):
    """No ``dir_location.txt`` -> database defaults to PROJECT_ROOT and
    the transformation dir defaults to ``<PROJECT_ROOT>/transformed/``."""
    from config import settings
    monkeypatch.setattr(paths.settings, "DIR_LOCATION_FILE", tmp_path / "absent.txt")
    assert paths.configured_database_dir() == settings.PROJECT_ROOT
    assert paths.configured_transformed_dir() == settings.PROJECT_ROOT / "transformed"


def test_configured_paths_read_both_keys(monkeypatch, tmp_path):
    db = tmp_path / "db"
    tr = tmp_path / "tr"
    cfg = tmp_path / "dir_location.txt"
    cfg.write_text(
        "# user paths\n"
        f"database_dir={db}\n"
        f"transformation_dir={tr}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(paths.settings, "DIR_LOCATION_FILE", cfg)
    assert paths.configured_database_dir() == db
    assert paths.configured_transformed_dir() == tr


def test_configured_paths_partial_file_falls_back_per_key(monkeypatch, tmp_path):
    """Only one key set -> that one is honored; the other falls back."""
    from config import settings
    db = tmp_path / "db"
    cfg = tmp_path / "dir_location.txt"
    cfg.write_text(f"database_dir={db}\n", encoding="utf-8")
    monkeypatch.setattr(paths.settings, "DIR_LOCATION_FILE", cfg)
    assert paths.configured_database_dir() == db
    assert paths.configured_transformed_dir() == settings.PROJECT_ROOT / "transformed"


def test_configured_paths_ignore_comments_blanks_and_unknown_keys(monkeypatch, tmp_path):
    from config import settings
    db = tmp_path / "db"
    cfg = tmp_path / "dir_location.txt"
    cfg.write_text(
        "# comment line\n"
        "\n"
        "  \n"
        f"  database_dir = {db}  \n"
        "unknown_key=/some/path\n"
        "malformed_line_without_eq\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(paths.settings, "DIR_LOCATION_FILE", cfg)
    assert paths.configured_database_dir() == db
    assert paths.configured_transformed_dir() == settings.PROJECT_ROOT / "transformed"


# ---------------------------------------------------------------------------
# GCS prefix helpers
# ---------------------------------------------------------------------------


def test_gcs_daily_prefix_appends_iso_date():
    """A bare ``daily`` prefix points at the whole tree; with a date it
    points at the day-folder. The integration tests rely on both forms."""
    from config.gcp import GCS_DAILY_PREFIX
    assert paths.gcs_daily_prefix() == GCS_DAILY_PREFIX
    assert paths.gcs_daily_prefix(date(2026, 4, 18)) == f"{GCS_DAILY_PREFIX}/2026-04-18"


def test_gcs_catalog_and_historical_prefixes_match_config():
    from config.gcp import GCS_CATALOG_PREFIX, GCS_HISTORICAL_PREFIX
    assert paths.gcs_catalog_prefix() == GCS_CATALOG_PREFIX
    assert paths.gcs_historical_prefix() == GCS_HISTORICAL_PREFIX


# ---------------------------------------------------------------------------
# Local <-> GCS translation
# ---------------------------------------------------------------------------


def test_to_gcs_blob_name_uses_posix_separators_on_windows(tmp_path):
    """Blob names are POSIX paths even when the local OS is Windows -- GCS
    treats backslashes as literal characters, so a Windows path joined with
    'historical/...' would produce an unfetchable blob name."""
    historical = tmp_path / "historical" / "stocks" / "prices_daily"
    historical.mkdir(parents=True)
    target = historical / "stocks_AAPL.parquet"
    target.write_bytes(b"x")

    blob = paths.to_gcs_blob_name(target, root=tmp_path)
    assert blob == "historical/stocks/prices_daily/stocks_AAPL.parquet"
    assert "\\" not in blob


def test_to_gcs_blob_name_rejects_paths_outside_known_trees(tmp_path):
    """Paths under ``transformed/`` or ``secrets/`` must not be translatable
    -- they don't go to the algo-trading bucket."""
    transformed = tmp_path / "transformed" / "AAPL.parquet"
    transformed.parent.mkdir(parents=True)
    transformed.write_bytes(b"x")

    with pytest.raises(ValueError, match="not under catalog/ historical/ daily/"):
        paths.to_gcs_blob_name(transformed, root=tmp_path)


def test_to_gcs_blob_name_rejects_root_itself(tmp_path):
    """The project root is not a valid blob source -- only files inside one
    of the three known top-level trees are translatable."""
    with pytest.raises(ValueError):
        paths.to_gcs_blob_name(tmp_path, root=tmp_path)


def test_to_local_path_strips_blob_name_into_root(tmp_path):
    out = paths.to_local_path("daily/2026-04-18/stocks/prices/stocks_AAPL.parquet",
                              root=tmp_path)
    assert out == tmp_path / "daily" / "2026-04-18" / "stocks" / "prices" / "stocks_AAPL.parquet"


def test_to_local_path_rejects_unknown_top_level():
    with pytest.raises(ValueError, match="not under catalog/ historical/ daily/"):
        paths.to_local_path("transformed/AAPL.parquet", root=Path("/tmp"))


@pytest.mark.parametrize("rel_blob", [
    "catalog/stocks.parquet",
    "catalog/yield_status.parquet",
    "historical/stocks/prices/stocks_AAPL.parquet",
    "historical/etfs/etf_profile/etfs_SPY.parquet",
    "historical/.setup_started_at",
    "daily/2026-04-18/ingestion_report.parquet",
    "daily/2026-04-18/stocks/income_statement/stocks_AAPL_annual.parquet",
])
def test_round_trip_local_to_gcs_to_local(tmp_path, rel_blob):
    """Every blob under one of the three trees must round-trip cleanly:
    ``to_local_path`` -> ``to_gcs_blob_name`` returns the original blob name.
    This is the symmetry guarantee callers rely on for sync logic."""
    local = paths.to_local_path(rel_blob, root=tmp_path)
    # The actual file does not need to exist for to_gcs_blob_name; but
    # ``Path.resolve().relative_to`` on non-existent paths still works on the
    # absolute parts. Touch it to keep the test resilient on all OSes.
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_bytes(b"")
    assert paths.to_gcs_blob_name(local, root=tmp_path) == rel_blob
