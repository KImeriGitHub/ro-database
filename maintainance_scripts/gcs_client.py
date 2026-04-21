"""Thin wrapper around ``google.cloud.storage`` used by every script that
touches the project bucket.

A single client is enough for most workloads; reuse the module-level
``get_client()`` helper so the underlying HTTP session is pooled.

The helpers here are intentionally small: upload/download a file, list a
prefix, and diff local vs remote trees. Anything more complex (e.g. parallel
transfers or lifecycle management) belongs in its own module.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator

from google.cloud import storage
from google.cloud.storage import Blob, Bucket, Client

from config.gcp import GCS_BUCKET, GCP_PROJECT_ID
from maintainance_scripts.gcp_credentials import get_gcp_credentials

logger = logging.getLogger(__name__)

_client: Client | None = None


def get_client() -> Client:
    """Return a cached GCS client authenticated via ``get_gcp_credentials``."""
    global _client
    if _client is None:
        creds = get_gcp_credentials()
        _client = storage.Client(project=GCP_PROJECT_ID, credentials=creds)
    return _client


def get_bucket(bucket_name: str = GCS_BUCKET) -> Bucket:
    return get_client().bucket(bucket_name)


@dataclass(frozen=True)
class BlobInfo:
    """Minimal metadata for diff and sync decisions."""
    name: str
    size: int
    md5_hash: str | None
    updated_iso: str | None


def list_blobs(prefix: str, bucket_name: str = GCS_BUCKET) -> Iterator[BlobInfo]:
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
    bucket_name: str = GCS_BUCKET,
    content_type: str | None = None,
) -> Blob:
    """Upload *local_path* to ``gs://bucket/blob_name``. Returns the blob."""
    bucket = get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(str(local_path), content_type=content_type)
    logger.info(f"Uploaded {local_path} to gs://{bucket_name}/{blob_name}")
    return blob


def download_file(
    blob_name: str,
    local_path: Path,
    bucket_name: str = GCS_BUCKET,
) -> Path:
    """Download ``gs://bucket/blob_name`` into *local_path*. Creates parents."""
    bucket = get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(str(local_path))
    return local_path


def blob_exists(blob_name: str, bucket_name: str = GCS_BUCKET) -> bool:
    return get_bucket(bucket_name).blob(blob_name).exists()


def upload_tree(
    local_root: Path,
    prefix: str,
    bucket_name: str = GCS_BUCKET,
    include_hidden: bool = True,
) -> list[str]:
    """Recursively upload *local_root* under bucket ``prefix/``.

    Returns the list of uploaded blob names. Files are never downloaded first;
    this is a push-only helper. For hidden files (``.setup_started_at``),
    pass ``include_hidden=True`` so the marker's mtime is preserved on the
    next resume of a historical run.
    """
    if not local_root.is_dir():
        raise NotADirectoryError(local_root)

    uploaded: list[str] = []
    for path in local_root.rglob("*"):
        if not path.is_file():
            continue
        if not include_hidden and path.name.startswith("."):
            continue
        rel = path.relative_to(local_root).as_posix()
        blob_name = f"{prefix}/{rel}"
        upload_file(path, blob_name, bucket_name=bucket_name)
        uploaded.append(blob_name)
    return uploaded


def download_tree(
    prefix: str,
    local_root: Path,
    bucket_name: str = GCS_BUCKET,
    skip_if_same_size: bool = True,
) -> list[Path]:
    """Recursively download every blob under ``prefix/`` into *local_root*.

    When *skip_if_same_size* is True, a local file that already matches the
    remote blob's size is left untouched. This is cheap and good enough for
    the append-only parquet layout.
    """
    local_root.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for info in list_blobs(prefix, bucket_name=bucket_name):
        rel = info.name[len(prefix) + 1:] if info.name.startswith(prefix + "/") else info.name
        if not rel:
            continue
        dest = local_root / rel
        if skip_if_same_size and dest.exists() and dest.stat().st_size == info.size:
            continue
        download_file(info.name, dest, bucket_name=bucket_name)
        written.append(dest)
    return written


def diff_local_vs_remote(
    local_root: Path,
    prefix: str,
    bucket_name: str = GCS_BUCKET,
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
