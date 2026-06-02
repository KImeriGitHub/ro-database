"""Unit tests for scheduled_scripts.sync_gcs_to_local.

Two behaviours matter: the ``--from-date`` argument parser (which must reject
non-ISO input with an argparse error, not a bare ValueError), and the
per-tree download dispatch -- each requested tree maps to the right GCS prefix
and local destination, and only the ``daily/`` tree gets the date cutoff
``name_filter``.
"""

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import scheduled_scripts.sync_gcs_to_local as mod


# ---------------------------------------------------------------------------
# _parse_date
# ---------------------------------------------------------------------------

def test_parse_date_valid():
    assert mod._parse_date("2026-06-01") == date(2026, 6, 1)


def test_parse_date_invalid_raises_argparse_error():
    import argparse
    with pytest.raises(argparse.ArgumentTypeError):
        mod._parse_date("06/01/2026")


# ---------------------------------------------------------------------------
# sync
# ---------------------------------------------------------------------------

@pytest.fixture
def captured_downloads(monkeypatch):
    calls = []

    def _download_tree(prefix, dest, workers=2, name_filter=None):
        calls.append({
            "prefix": prefix, "dest": Path(dest),
            "workers": workers, "name_filter": name_filter,
        })
        return []  # no files written

    monkeypatch.setattr(mod.gcs_client, "download_tree", _download_tree)
    return calls


def test_sync_all_trees_dispatches_each(tmp_path, captured_downloads):
    mod.sync(tmp_path, ["catalog", "historical", "daily"], workers=3)
    by_prefix = {c["prefix"]: c for c in captured_downloads}
    assert by_prefix["catalog"]["dest"] == tmp_path / "catalog"
    assert by_prefix["historical"]["dest"] == tmp_path / "historical"
    assert by_prefix["daily"]["dest"] == tmp_path / "daily"
    # workers flow through to every tree.
    assert all(c["workers"] == 3 for c in captured_downloads)


def test_sync_subset_only_requested_trees(tmp_path, captured_downloads):
    mod.sync(tmp_path, ["catalog"])
    assert [c["prefix"] for c in captured_downloads] == ["catalog"]


def test_sync_without_from_date_has_no_name_filter(tmp_path, captured_downloads):
    mod.sync(tmp_path, ["daily"])
    assert captured_downloads[0]["name_filter"] is None


def test_sync_daily_from_date_applies_cutoff_filter(tmp_path, captured_downloads):
    mod.sync(tmp_path, ["daily"], from_date=date(2026, 6, 1))
    nf = captured_downloads[0]["name_filter"]
    assert nf is not None
    # The filter keeps days on or after the cutoff, dropping earlier ones.
    assert nf("2026-06-01/stocks/prices/AAPL.parquet") is True
    assert nf("2026-06-02/stocks/prices/AAPL.parquet") is True
    assert nf("2026-05-31/stocks/prices/AAPL.parquet") is False


def test_sync_from_date_ignored_for_non_daily_trees(tmp_path, captured_downloads):
    mod.sync(tmp_path, ["catalog", "daily"], from_date=date(2026, 6, 1))
    by_prefix = {c["prefix"]: c for c in captured_downloads}
    assert by_prefix["catalog"]["name_filter"] is None
    assert by_prefix["daily"]["name_filter"] is not None
