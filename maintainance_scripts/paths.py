"""Single source of truth for the ``catalog/``, ``historical/`` and
``daily/YYYY-MM-DD/`` paths and for translating between local paths and
``gs://`` URIs.

Both sides of the pipeline (GCP container and local workstation) use the
same directory layout, so translating one to the other is a prefix swap.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from config import settings
from config.gcp import (
    GCS_BUCKET,
    GCS_CATALOG_PREFIX,
    GCS_DAILY_PREFIX,
    GCS_HISTORICAL_PREFIX,
)


# ---------------------------------------------------------------------------
# Local paths
# ---------------------------------------------------------------------------

def local_catalog_dir(root: Path | None = None) -> Path:
    return (root or settings.PROJECT_ROOT) / "catalog"


def local_historical_dir(root: Path | None = None) -> Path:
    return (root or settings.PROJECT_ROOT) / "historical"


def local_daily_dir(root: Path | None = None) -> Path:
    return (root or settings.PROJECT_ROOT) / "daily"


def local_daily_date_dir(folder_date: date, root: Path | None = None) -> Path:
    return local_daily_dir(root) / folder_date.isoformat()


# ---------------------------------------------------------------------------
# GCS URIs
# ---------------------------------------------------------------------------

def gcs_uri(blob_name: str, bucket: str = GCS_BUCKET) -> str:
    return f"gs://{bucket}/{blob_name}"


def gcs_catalog_prefix() -> str:
    return GCS_CATALOG_PREFIX


def gcs_historical_prefix() -> str:
    return GCS_HISTORICAL_PREFIX


def gcs_daily_prefix(folder_date: date | None = None) -> str:
    if folder_date is None:
        return GCS_DAILY_PREFIX
    return f"{GCS_DAILY_PREFIX}/{folder_date.isoformat()}"


# ---------------------------------------------------------------------------
# Local <-> GCS translation
# ---------------------------------------------------------------------------

_PREFIX_BY_TOP = {
    "catalog": GCS_CATALOG_PREFIX,
    "historical": GCS_HISTORICAL_PREFIX,
    "daily": GCS_DAILY_PREFIX,
}


def to_gcs_blob_name(local_path: Path, root: Path | None = None) -> str:
    """Convert a local project-tree path to a GCS blob name.

    ``<root>/historical/stocks/prices/AAPL.parquet`` becomes
    ``historical/stocks/prices/AAPL.parquet``. Raises ``ValueError`` if
    *local_path* is not under one of the three known trees.
    """
    root = root or settings.PROJECT_ROOT
    rel = local_path.resolve().relative_to(root.resolve())
    parts = rel.parts
    if not parts or parts[0] not in _PREFIX_BY_TOP:
        raise ValueError(
            f"{local_path} is not under catalog/ historical/ daily/ of {root}"
        )
    return rel.as_posix()


def to_local_path(blob_name: str, root: Path | None = None) -> Path:
    """Convert a GCS blob name back to its local project-tree path."""
    root = root or settings.PROJECT_ROOT
    top = blob_name.split("/", 1)[0]
    if top not in _PREFIX_BY_TOP:
        raise ValueError(
            f"blob {blob_name!r} is not under catalog/ historical/ daily/"
        )
    return root / Path(blob_name)
