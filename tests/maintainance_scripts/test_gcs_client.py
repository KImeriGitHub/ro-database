"""Tests for ``maintainance_scripts.gcs_client``.

The wrapper is small but every other module in the repo depends on its
contract: a cached client, POSIX-only blob names, lossless metadata round-
trips into ``BlobInfo``, and predictable upload/download tree behaviour
(hidden-file inclusion, size-based skip, size-only diff).

The ``google.cloud.storage`` client is stubbed with a minimal in-memory
fake -- the tests assert behaviour through that fake's recorded calls and
its blob registry, not by hitting any real network or filesystem-backed
GCS emulator.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from maintainance_scripts import gcs_client


# ---------------------------------------------------------------------------
# Minimal in-memory GCS fake
# ---------------------------------------------------------------------------


class _FakeBlob:
    def __init__(self, bucket: "_FakeBucket", name: str):
        self.bucket = bucket
        self.name = name
        self.size: int | None = None
        self.md5_hash: str | None = None
        self.updated: datetime | None = None
        self._content: bytes | None = None

    def upload_from_filename(self, path: str, content_type: str | None = None):
        data = Path(path).read_bytes()
        self._content = data
        self.size = len(data)
        self.updated = datetime(2026, 4, 18, 12, 0, 0, tzinfo=timezone.utc)
        self.bucket.uploads.append(
            {"blob": self.name, "path": path, "content_type": content_type}
        )
        self.bucket.blobs[self.name] = self

    def download_to_filename(self, path: str):
        if self._content is None:
            raise FileNotFoundError(self.name)
        local = Path(path)
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(self._content)
        self.bucket.downloads.append({"blob": self.name, "path": path})

    def exists(self) -> bool:
        return self.bucket.has_blob(self.name)


class _FakeBucket:
    def __init__(self, name: str):
        self.name = name
        self.blobs: dict[str, _FakeBlob] = {}
        self.uploads: list[dict] = []
        self.downloads: list[dict] = []

    def blob(self, name: str) -> _FakeBlob:
        existing = self.blobs.get(name)
        return existing if existing is not None else _FakeBlob(self, name)

    def list_blobs(self, prefix: str | None = None):
        for blob in self.blobs.values():
            if prefix is None or blob.name.startswith(prefix):
                yield blob

    def has_blob(self, name: str) -> bool:
        return name in self.blobs

    def add(self, name: str, content: bytes, *,
            md5: str | None = None,
            updated: datetime | None = None) -> _FakeBlob:
        blob = _FakeBlob(self, name)
        blob._content = content
        blob.size = len(content)
        blob.md5_hash = md5
        blob.updated = updated
        self.blobs[name] = blob
        return blob


class _FakeClient:
    def __init__(self, project=None, credentials=None):
        self.project = project
        self.credentials = credentials
        self.buckets: dict[str, _FakeBucket] = {}

    def bucket(self, name: str) -> _FakeBucket:
        return self.buckets.setdefault(name, _FakeBucket(name))


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_cached_client():
    """Reset the module-level client cache so each test starts cleanly."""
    gcs_client._client = None
    yield
    gcs_client._client = None


@pytest.fixture
def fake_storage(monkeypatch):
    """Install a fake storage.Client factory and a no-op credential resolver.
    Returns the single ``_FakeClient`` instance that gcs_client will cache."""
    created: list[_FakeClient] = []

    def client_factory(project=None, credentials=None):
        c = _FakeClient(project=project, credentials=credentials)
        created.append(c)
        return c

    monkeypatch.setattr(gcs_client.storage, "Client", client_factory)
    monkeypatch.setattr(gcs_client, "get_gcp_credentials", lambda: "FAKE_CREDS")
    # GCP_PROJECT_ID is read at call-time inside get_client(), so this patch
    # takes effect. GCS_BUCKET, in contrast, is captured as a function default
    # at import time -- patching it here would not propagate; tests instead
    # pass the bucket name explicitly or rely on the consistent default value
    # across calls within a single test.
    monkeypatch.setattr(gcs_client, "GCP_PROJECT_ID", "test-project")
    return created


# ---------------------------------------------------------------------------
# Client / bucket plumbing
# ---------------------------------------------------------------------------


def test_get_client_caches_and_threads_creds(fake_storage):
    """First call instantiates ``storage.Client`` with the resolved creds and
    GCP_PROJECT_ID; later calls reuse the cached instance so the underlying
    HTTPS session is pooled."""
    c1 = gcs_client.get_client()
    c2 = gcs_client.get_client()
    assert c1 is c2
    assert len(fake_storage) == 1
    assert fake_storage[0].project == "test-project"
    assert fake_storage[0].credentials == "FAKE_CREDS"


def test_get_bucket_returns_named_bucket(fake_storage):
    """``get_bucket(name)`` must thread the name into ``client.bucket(name)``.
    The default-bucket value comes from ``GCS_BUCKET`` and is captured at
    function-definition time in the module signature -- not retested here
    because mutating function defaults from a test would be more fragile
    than reading the source."""
    bucket = gcs_client.get_bucket("explicit-bucket")
    assert isinstance(bucket, _FakeBucket)
    assert bucket.name == "explicit-bucket"


# ---------------------------------------------------------------------------
# list_blobs / BlobInfo
# ---------------------------------------------------------------------------


def test_list_blobs_yields_blob_info_with_metadata(fake_storage):
    """``list_blobs`` must NOT leak the underlying ``google.cloud.storage.Blob``
    -- callers depend on the lightweight ``BlobInfo`` dataclass for diffs
    and sync decisions."""
    bucket = gcs_client.get_bucket()
    bucket.add(
        "catalog/stocks.parquet",
        b"abc",
        md5="DEAD",
        updated=datetime(2026, 4, 18, 9, 30, tzinfo=timezone.utc),
    )

    out = list(gcs_client.list_blobs("catalog/"))

    assert len(out) == 1
    info = out[0]
    assert isinstance(info, gcs_client.BlobInfo)
    assert info.name == "catalog/stocks.parquet"
    assert info.size == 3
    assert info.md5_hash == "DEAD"
    assert info.updated_iso == "2026-04-18T09:30:00+00:00"


def test_list_blobs_filters_by_prefix(fake_storage):
    bucket = gcs_client.get_bucket()
    bucket.add("catalog/stocks.parquet", b"x")
    bucket.add("historical/stocks/prices/AAPL.parquet", b"y")

    names = [b.name for b in gcs_client.list_blobs("catalog/")]
    assert names == ["catalog/stocks.parquet"]


def test_list_blobs_handles_missing_metadata(fake_storage):
    """A freshly-uploaded blob may not have ``updated`` yet; the wrapper
    must still produce a valid BlobInfo (None instead of crashing on
    ``.isoformat()``)."""
    bucket = gcs_client.get_bucket()
    bucket.add("k", b"v")  # updated=None, md5=None

    [info] = list(gcs_client.list_blobs(""))
    assert info.updated_iso is None
    assert info.md5_hash is None
    assert info.size == 1


# ---------------------------------------------------------------------------
# upload_file / download_file / blob_exists
# ---------------------------------------------------------------------------


def test_upload_file_writes_to_named_blob(fake_storage, tmp_path):
    src = tmp_path / "x.parquet"
    src.write_bytes(b"hello")

    gcs_client.upload_file(src, "historical/x.parquet")

    bucket = gcs_client.get_bucket()
    assert bucket.uploads == [
        {"blob": "historical/x.parquet", "path": str(src), "content_type": None}
    ]
    assert bucket.has_blob("historical/x.parquet")


def test_upload_file_passes_content_type(fake_storage, tmp_path):
    src = tmp_path / "x.json"
    src.write_bytes(b"{}")

    gcs_client.upload_file(src, "blob.json", content_type="application/json")

    bucket = gcs_client.get_bucket()
    assert bucket.uploads[0]["content_type"] == "application/json"


def test_download_file_creates_parent_dirs(fake_storage, tmp_path):
    """``download_file`` must mkdir the parent so callers can target a
    fresh tree (e.g. a fresh container workdir)."""
    bucket = gcs_client.get_bucket()
    bucket.add("daily/2026-04-18/stocks/prices/x.parquet", b"payload")
    dest = tmp_path / "fresh" / "subdir" / "x.parquet"

    out = gcs_client.download_file(
        "daily/2026-04-18/stocks/prices/x.parquet", dest
    )

    assert out == dest
    assert dest.read_bytes() == b"payload"


def test_blob_exists_delegates_to_bucket(fake_storage):
    bucket = gcs_client.get_bucket()
    bucket.add("present", b"x")

    assert gcs_client.blob_exists("present") is True
    assert gcs_client.blob_exists("absent") is False


# ---------------------------------------------------------------------------
# upload_tree
# ---------------------------------------------------------------------------


def test_upload_tree_walks_recursively_with_posix_blob_names(fake_storage, tmp_path):
    """Blob names must use ``/`` even on Windows. A literal backslash in a
    blob name produces an unfetchable object on GCS."""
    root = tmp_path / "historical"
    (root / "stocks" / "prices").mkdir(parents=True)
    (root / "stocks" / "prices" / "AAPL.parquet").write_bytes(b"a")
    (root / "stocks" / "prices" / "MSFT.parquet").write_bytes(b"b")
    (root / "etfs").mkdir()
    (root / "etfs" / "SPY.parquet").write_bytes(b"c")

    uploaded = gcs_client.upload_tree(root, "historical")

    assert sorted(uploaded) == [
        "historical/etfs/SPY.parquet",
        "historical/stocks/prices/AAPL.parquet",
        "historical/stocks/prices/MSFT.parquet",
    ]
    assert all("\\" not in name for name in uploaded)


def test_upload_tree_includes_hidden_by_default(fake_storage, tmp_path):
    """``.setup_started_at`` is a hidden marker file whose mtime must
    survive resumes of the historical setup -- it MUST be included by
    default."""
    root = tmp_path / "historical"
    root.mkdir()
    (root / ".setup_started_at").write_bytes(b"")
    (root / "data.parquet").write_bytes(b"x")

    uploaded = gcs_client.upload_tree(root, "historical")

    assert "historical/.setup_started_at" in uploaded
    assert "historical/data.parquet" in uploaded


def test_upload_tree_excludes_hidden_when_requested(fake_storage, tmp_path):
    root = tmp_path / "historical"
    root.mkdir()
    (root / ".setup_started_at").write_bytes(b"")
    (root / "data.parquet").write_bytes(b"x")

    uploaded = gcs_client.upload_tree(root, "historical", include_hidden=False)

    assert uploaded == ["historical/data.parquet"]


def test_upload_tree_rejects_non_directory(fake_storage, tmp_path):
    not_a_dir = tmp_path / "file"
    not_a_dir.write_bytes(b"x")
    with pytest.raises(NotADirectoryError):
        gcs_client.upload_tree(not_a_dir, "historical")


# ---------------------------------------------------------------------------
# download_tree
# ---------------------------------------------------------------------------


def test_download_tree_skips_same_size_local_files(fake_storage, tmp_path):
    """Default ``skip_if_same_size=True`` is the cheap append-only sync
    semantic: do not re-download a parquet the local mirror already has."""
    bucket = gcs_client.get_bucket()
    bucket.add("historical/a.parquet", b"abc")  # size 3
    bucket.add("historical/b.parquet", b"defgh")  # size 5

    local = tmp_path / "mirror"
    (local / "a.parquet").parent.mkdir(parents=True)
    (local / "a.parquet").write_bytes(b"XYZ")  # same size as remote -> skip
    # b.parquet does not exist locally -> must be downloaded

    written = gcs_client.download_tree("historical", local)

    assert written == [local / "b.parquet"]
    assert (local / "a.parquet").read_bytes() == b"XYZ"  # untouched
    assert (local / "b.parquet").read_bytes() == b"defgh"


def test_download_tree_strips_prefix(fake_storage, tmp_path):
    bucket = gcs_client.get_bucket()
    bucket.add("daily/2026-04-18/stocks/prices/x.parquet", b"x")

    local = tmp_path / "mirror"
    written = gcs_client.download_tree("daily/2026-04-18", local)

    expected = local / "stocks" / "prices" / "x.parquet"
    assert written == [expected]
    assert expected.read_bytes() == b"x"


def test_download_tree_skip_disabled_overwrites(fake_storage, tmp_path):
    """With ``skip_if_same_size=False`` the local file is overwritten even
    if sizes match -- used by callers who need byte-level freshness."""
    bucket = gcs_client.get_bucket()
    bucket.add("historical/a.parquet", b"new")

    local = tmp_path / "mirror"
    local.mkdir()
    (local / "a.parquet").write_bytes(b"old")  # same size, but stale

    written = gcs_client.download_tree(
        "historical", local, skip_if_same_size=False
    )

    assert written == [local / "a.parquet"]
    assert (local / "a.parquet").read_bytes() == b"new"


# ---------------------------------------------------------------------------
# diff_local_vs_remote
# ---------------------------------------------------------------------------


def test_diff_buckets_files_into_three_lists(fake_storage, tmp_path):
    """The diff is size-only by design (good enough to spot missing
    uploads); it returns ``(only_local, only_remote, size_mismatch)``
    as sorted blob-name lists."""
    bucket = gcs_client.get_bucket()
    bucket.add("historical/keep.parquet", b"same")     # 4 bytes both sides
    bucket.add("historical/remote_only.parquet", b"y")
    bucket.add("historical/mismatch.parquet", b"xxxxxx")  # 6 bytes remote

    local = tmp_path / "historical"
    local.mkdir()
    (local / "keep.parquet").write_bytes(b"same")          # matches
    (local / "local_only.parquet").write_bytes(b"z")
    (local / "mismatch.parquet").write_bytes(b"xx")        # 2 bytes local

    only_local, only_remote, mismatch = gcs_client.diff_local_vs_remote(
        local, "historical"
    )

    assert only_local == ["historical/local_only.parquet"]
    assert only_remote == ["historical/remote_only.parquet"]
    assert mismatch == ["historical/mismatch.parquet"]


def test_diff_returns_empty_lists_when_in_sync(fake_storage, tmp_path):
    bucket = gcs_client.get_bucket()
    bucket.add("historical/a.parquet", b"abc")

    local = tmp_path / "historical"
    local.mkdir()
    (local / "a.parquet").write_bytes(b"abc")

    assert gcs_client.diff_local_vs_remote(local, "historical") == (
        [], [], []
    )
