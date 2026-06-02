"""Unit tests for scheduled_scripts.push_historical_to_gcs.

The script's safety contract is the load-bearing bit: by default it refuses
to overwrite blobs whose size differs from what is already in the bucket
(the historical tree is append-only, so a size mismatch usually means
something is wrong), and only ``--force`` lets it through. The GCS layer is
stubbed so these tests assert the decision logic, not real transfers.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import scheduled_scripts.push_historical_to_gcs as mod


@pytest.fixture
def fake_gcs(monkeypatch):
    """Stub the gcs_client functions the script calls and record uploads."""
    calls = {"upload_tree": [], "diff": None}

    def _upload_tree(local_root, prefix, workers=2):
        calls["upload_tree"].append((Path(local_root), prefix, workers))

    def _diff(local_root, prefix):
        # default: nothing overlaps, everything is new
        return (["historical/stocks/prices/AAPL.parquet"], [], [])

    monkeypatch.setattr(mod.gcs_client, "upload_tree", _upload_tree)
    monkeypatch.setattr(mod.gcs_client, "diff_local_vs_remote", _diff)
    monkeypatch.setattr(mod, "gcs_historical_prefix", lambda: "historical")
    monkeypatch.setattr(mod, "gcs_catalog_prefix", lambda: "catalog")
    return calls


def _make_tree(root: Path, *, with_catalog: bool = True) -> Path:
    (root / "historical" / "stocks" / "prices").mkdir(parents=True)
    (root / "historical" / "stocks" / "prices" / "AAPL.parquet").write_text(
        "x", encoding="utf-8"
    )
    if with_catalog:
        (root / "catalog").mkdir()
        (root / "catalog" / "stocks.parquet").write_text("c", encoding="utf-8")
    return root


def test_missing_historical_dir_raises(tmp_path, fake_gcs):
    with pytest.raises(FileNotFoundError):
        mod.push(tmp_path, include_catalog=False, force=False)


def test_push_uploads_historical_and_catalog(tmp_path, fake_gcs):
    _make_tree(tmp_path)
    mod.push(tmp_path, include_catalog=True, force=False)
    prefixes = [c[1] for c in fake_gcs["upload_tree"]]
    assert "historical" in prefixes
    assert "catalog" in prefixes


def test_skip_catalog_uploads_only_historical(tmp_path, fake_gcs):
    _make_tree(tmp_path)
    mod.push(tmp_path, include_catalog=False, force=False)
    prefixes = [c[1] for c in fake_gcs["upload_tree"]]
    assert prefixes == ["historical"]


def test_size_mismatch_blocks_without_force(tmp_path, monkeypatch, fake_gcs):
    _make_tree(tmp_path, with_catalog=False)
    monkeypatch.setattr(
        mod.gcs_client, "diff_local_vs_remote",
        lambda local_root, prefix: ([], [], ["historical/stocks/prices/AAPL.parquet"]),
    )
    with pytest.raises(RuntimeError, match="already exist with different sizes"):
        mod.push(tmp_path, include_catalog=False, force=False)
    # Nothing uploaded once the guard tripped.
    assert fake_gcs["upload_tree"] == []


def test_force_skips_diff_and_uploads(tmp_path, monkeypatch, fake_gcs):
    _make_tree(tmp_path, with_catalog=False)

    def _boom(*a, **k):
        raise AssertionError("diff_local_vs_remote must not run under --force")

    monkeypatch.setattr(mod.gcs_client, "diff_local_vs_remote", _boom)
    mod.push(tmp_path, include_catalog=False, force=True)
    assert [c[1] for c in fake_gcs["upload_tree"]] == ["historical"]


def test_missing_catalog_dir_warns_not_raises(tmp_path, fake_gcs, caplog):
    _make_tree(tmp_path, with_catalog=False)
    import logging
    with caplog.at_level(logging.WARNING):
        mod.push(tmp_path, include_catalog=True, force=True)
    assert "catalog/ not found" in caplog.text
    # historical still uploaded.
    assert [c[1] for c in fake_gcs["upload_tree"]] == ["historical"]
