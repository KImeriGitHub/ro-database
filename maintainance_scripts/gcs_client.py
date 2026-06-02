"""Thin wrapper around ``google.cloud.storage`` used by every script that
touches the project bucket.

A single client is enough for most workloads; reuse the module-level
``get_client()`` helper so the underlying HTTP session is pooled.

The helpers here are intentionally small: upload/download a file, list a
prefix, and diff local vs remote trees. Anything more complex (e.g. parallel
transfers or lifecycle management) belongs in its own module.
"""

from __future__ import annotations

import base64
import hashlib
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Iterator

from google.cloud import storage
from google.cloud.storage import Blob, Bucket, Client
from google.cloud.storage.retry import DEFAULT_RETRY
from requests.adapters import HTTPAdapter

from config.gcp import GCS_BUCKET, GCP_PROJECT_ID
from maintainance_scripts.gcp_credentials import get_gcp_credentials

logger = logging.getLogger(__name__)

# Per-request ceiling (seconds) and total retry budget for a single blob
# transfer. The library defaults (60s request, 120s retry deadline) are too
# tight for large parquets on a slow uplink: a stalled chunk write blows the
# 120s deadline and aborts the whole tree. These give a slow file room to land.
_TRANSFER_TIMEOUT = 300.0
_TRANSFER_RETRY = DEFAULT_RETRY.with_timeout(600.0)

_client: Client | None = None
_pool_size = 0


def get_client() -> Client:
    """Return a cached GCS client authenticated via ``get_gcp_credentials``."""
    global _client
    if _client is None:
        creds = get_gcp_credentials()
        _client = storage.Client(project=GCP_PROJECT_ID, credentials=creds)
    return _client


def _ensure_pool_size(size: int) -> None:
    """Grow the shared client's HTTPS connection pool to hold *size* sockets.

    ``storage.Client`` mounts urllib3's default adapter (``pool_maxsize=10``).
    Running more upload/download workers than that overflows the pool, so every
    extra transfer discards and re-opens a TLS connection on each request. That
    churn shows up as repeated "Connection pool is full" warnings and
    destabilises the sockets into write timeouts. Mounting a right-sized adapter
    lets each worker keep a warm connection.

    Called from the main thread before any worker pool starts, so no locking is
    needed. The floor of 10 keeps us at or above the library default.
    """
    global _pool_size
    size = max(size, 10)
    if size <= _pool_size:
        return
    # ``_http`` is a private google-cloud attribute (a requests.Session). Guard
    # against it being absent or unmountable so a stubbed client stays usable.
    http = getattr(get_client(), "_http", None)
    if http is None or not hasattr(http, "mount"):
        return
    adapter = HTTPAdapter(pool_connections=size, pool_maxsize=size)
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    _pool_size = size


def get_bucket(bucket_name: str | None = None) -> Bucket:
    """Return the ``Bucket`` handle for *bucket_name* (defaults to ``GCS_BUCKET``).

    The default is resolved at call time, not at import time, so an unset
    ``GCS_BUCKET`` surfaces as a loud ``RuntimeError`` from here rather than
    a confusing ``ValueError`` deep inside the Google SDK when it sees a
    ``None`` bucket name. Tests that monkeypatch ``gcs_client.GCS_BUCKET``
    have their patch honoured.
    """
    name = bucket_name or GCS_BUCKET
    if not name:
        raise RuntimeError(
            "GCS_BUCKET is not configured. Set the GCS_BUCKET environment "
            "variable or add 'gcs_bucket' to secrets/gcs_credentials.json."
        )
    return get_client().bucket(name)


@dataclass(frozen=True)
class BlobInfo:
    """Minimal metadata for diff and sync decisions."""
    name: str
    size: int
    md5_hash: str | None
    updated_iso: str | None


def list_blobs(prefix: str, bucket_name: str | None = None) -> Iterator[BlobInfo]:
    """Iterate blobs under *prefix* (non-recursive-aware; GCS has no dirs)."""
    bucket = get_bucket(bucket_name)
    for blob in bucket.list_blobs(prefix=prefix):
        yield BlobInfo(
            name=blob.name,
            size=blob.size or 0,
            md5_hash=blob.md5_hash,
            updated_iso=blob.updated.isoformat() if blob.updated else None,
        )


def upload_file(
    local_path: Path,
    blob_name: str,
    bucket_name: str | None = None,
    content_type: str | None = None,
) -> Blob:
    """Upload *local_path* to ``gs://bucket/blob_name``. Returns the blob."""
    bucket = get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(
        str(local_path),
        content_type=content_type,
        timeout=_TRANSFER_TIMEOUT,
        retry=_TRANSFER_RETRY,
    )
    logger.info(f"Uploaded {local_path} to gs://{bucket.name}/{blob_name}")
    return blob


def download_file(
    blob_name: str,
    local_path: Path,
    bucket_name: str | None = None,
) -> Path:
    """Download ``gs://bucket/blob_name`` into *local_path*. Creates parents."""
    bucket = get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(
        str(local_path), timeout=_TRANSFER_TIMEOUT, retry=_TRANSFER_RETRY
    )
    return local_path


def blob_exists(blob_name: str, bucket_name: str | None = None) -> bool:
    return get_bucket(bucket_name).blob(blob_name).exists()


def upload_tree(
    local_root: Path,
    prefix: str,
    bucket_name: str | None = None,
    include_hidden: bool = True,
    workers: int = 2,
    skip_if_same_md5: bool = True,
) -> list[str]:
    """Recursively upload *local_root* under bucket ``prefix/``.

    Returns the list of blob names actually uploaded. Files are never
    downloaded first; this is a push-only helper. For hidden files
    (``.setup_started_at``), pass ``include_hidden=True`` so the marker's mtime
    is preserved on the next resume of a historical run.

    When *skip_if_same_md5* is True (the default), a blob that already exists
    with a matching MD5 is left untouched. This makes the push resumable: if a
    large run is interrupted (network blip, write timeout), re-running uploads
    only what is missing instead of re-pushing every file from scratch. Mirrors
    ``download_tree``'s skip logic. Existing blob metadata is fetched in one
    ``list_blobs`` pass rather than a per-file ``exists()`` round-trip.

    *workers* controls how many files are uploaded in parallel. Same shape
    as ``download_tree``: many small parquet files are latency-bound, so a
    small thread pool gives a multi-x speedup. Default is 2. The shared
    client's connection pool is grown to match so workers do not contend over
    too few sockets.
    """
    if not local_root.is_dir():
        raise NotADirectoryError(local_root)
    if workers < 1:
        raise ValueError(f"workers must be >= 1, got {workers}")

    get_bucket(bucket_name)
    pairs: list[tuple[Path, str]] = []
    for path in local_root.rglob("*"):
        if not path.is_file():
            continue
        if not include_hidden and path.name.startswith("."):
            continue
        rel = path.relative_to(local_root).as_posix()
        pairs.append((path, f"{prefix}/{rel}"))

    remote_md5: dict[str, str | None] = {}
    if skip_if_same_md5:
        remote_md5 = {
            info.name: info.md5_hash
            for info in list_blobs(prefix, bucket_name=bucket_name)
        }

    _ensure_pool_size(workers)

    def _process(item: tuple[Path, str]) -> str | None:
        path, blob_name = item
        remote = remote_md5.get(blob_name)
        if remote is not None and _local_md5_b64(path) == remote:
            return None
        upload_file(path, blob_name, bucket_name=bucket_name)
        return blob_name

    if workers == 1:
        results: Iterable[str | None] = (_process(p) for p in pairs)
    else:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            results = list(ex.map(_process, pairs))
    return [b for b in results if b is not None]


def _local_md5_b64(path: Path) -> str:
    """Return the base64-encoded MD5 of *path* in the same format GCS reports.

    Streamed in 1 MiB chunks so the helper stays cheap on large parquets.
    GCS' ``Blob.md5_hash`` is base64-encoded; this mirrors that encoding so
    a single ``==`` comparison settles the freshness question.
    """
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return base64.b64encode(h.digest()).decode("ascii")


def download_tree(
    prefix: str,
    local_root: Path,
    bucket_name: str | None = None,
    skip_if_same_md5: bool = True,
    workers: int = 2,
    name_filter: Callable[[str], bool] | None = None,
) -> list[Path]:
    """Recursively download every blob under ``prefix/`` into *local_root*.

    When *skip_if_same_md5* is True, a local file whose MD5 matches the
    remote blob's ``md5_hash`` is left untouched. MD5 over size avoids the
    rare-but-real case where a rewritten file (e.g. a refreshed
    ``ingestion_report.parquet``) lands at the same byte count but with
    different content. A blob whose ``md5_hash`` is missing (composite
    objects don't expose one) is always re-downloaded since there is no
    way to verify freshness from the metadata alone.

    *workers* controls how many blobs are fetched in parallel. For trees
    with many small parquet files (the historical and daily layouts), total
    runtime is dominated by per-request latency, so a small thread pool
    typically gives a multi-x speedup. Default is 2 to stay gentle on the
    GCS quota and local IO; raise it on a fast link.

    *name_filter*, when given, receives each blob's path relative to *prefix*
    and returns whether to download it. Used to restrict the ``daily/`` tree
    to a date range without listing every date folder separately.
    """
    if workers < 1:
        raise ValueError(f"workers must be >= 1, got {workers}")
    local_root.mkdir(parents=True, exist_ok=True)
    get_bucket(bucket_name)
    _ensure_pool_size(workers)
    infos = list(list_blobs(prefix, bucket_name=bucket_name))

    def _process(info: BlobInfo) -> Path | None:
        rel = info.name[len(prefix) + 1:] if info.name.startswith(prefix + "/") else info.name
        if not rel:
            return None
        if name_filter is not None and not name_filter(rel):
            return None
        dest = local_root / rel
        if (
            skip_if_same_md5
            and dest.exists()
            and info.md5_hash is not None
            and _local_md5_b64(dest) == info.md5_hash
        ):
            return None
        download_file(info.name, dest, bucket_name=bucket_name)
        return dest

    if workers == 1:
        results: Iterable[Path | None] = (_process(info) for info in infos)
    else:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            results = list(ex.map(_process, infos))
    return [p for p in results if p is not None]


def diff_local_vs_remote(
    local_root: Path,
    prefix: str,
    bucket_name: str | None = None,
) -> tuple[list[str], list[str], list[str]]:
    """Return ``(only_local, only_remote, size_mismatch)`` as blob names.

    Size-only diff. Good enough to spot missing uploads/downloads before a
    sync. For content-level checks use a parquet-aware comparator.
    """
    local_by_blob: dict[str, int] = {}
    for path in local_root.rglob("*"):
        if path.is_file():
            rel = path.relative_to(local_root).as_posix()
            local_by_blob[f"{prefix}/{rel}"] = path.stat().st_size

    remote_by_blob: dict[str, int] = {
        info.name: info.size for info in list_blobs(prefix, bucket_name=bucket_name)
    }

    only_local = sorted(local_by_blob.keys() - remote_by_blob.keys())
    only_remote = sorted(remote_by_blob.keys() - local_by_blob.keys())
    size_mismatch = sorted(
        name for name in local_by_blob.keys() & remote_by_blob.keys()
        if local_by_blob[name] != remote_by_blob[name]
    )
    return only_local, only_remote, size_mismatch
