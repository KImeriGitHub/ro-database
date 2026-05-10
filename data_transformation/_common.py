"""Shared helpers for data_transformation: source-file enumeration, sector
lookup, schema-strict casting, and the transformation report.

Per-frame builders under ``data_transformation/frames/`` and the orchestrator
in ``transform.py`` import everything they need from here.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

from historical_data_setup._common import (
    ASSET_TYPE_FILE_PREFIX,
    fs_symbol,
    unfs_symbol,
)

from data_transformation.AssetData import CANONICAL_SECTORS

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Asset-type metadata
# ---------------------------------------------------------------------------

ASSET_TYPES: tuple[str, ...] = (
    "stocks",
    "etfs",
    "forex",
    "indices",
    "cryptocurrencies",
    "commodities",
    "economic",
)

# Asset types whose source data lives directly under <root>/<asset_type>/,
# with no nested endpoint subfolder. The other types (stocks, etfs) use
# <root>/<asset_type>/<endpoint>/ (e.g. prices_daily, prices, etf_profile).
FLAT_ASSET_TYPES: frozenset[str] = frozenset({
    "forex", "indices", "cryptocurrencies", "commodities", "economic",
})


# ---------------------------------------------------------------------------
# Sector lookup
# ---------------------------------------------------------------------------

_SECTOR_TO_INDEX: dict[str, int] = {name: i for i, name in enumerate(CANONICAL_SECTORS)}
_OTHER_SECTOR_INDEX: int = _SECTOR_TO_INDEX["Other"]


def sector_to_index(name: str | None) -> int:
    """Return the int index of *name* in ``CANONICAL_SECTORS``.

    Empty string, None, or any unknown value falls through to ``Other``.
    """
    if not name:
        return _OTHER_SECTOR_INDEX
    return _SECTOR_TO_INDEX.get(name, _OTHER_SECTOR_INDEX)


# ---------------------------------------------------------------------------
# Per-symbol output directory naming
# ---------------------------------------------------------------------------

def symbol_dirname(symbol: str) -> str:
    """Per-symbol output directory name (``data_<SYMBOL>``).

    Mirrors the per-symbol filename prefix scheme in ``historical/`` and
    ``daily/``. Without it, a ticker like ``CON``/``PRN``/``NUL`` would
    create a directory whose name is reserved by Windows. The symbol is
    additionally routed through ``fs_symbol`` so slash-class tickers like
    ``BC/PB`` collapse to a single component (``data_BC%2FPB``) rather
    than splitting into a ``data_BC/`` parent and a ``PB`` child.
    """
    name = f"data_{fs_symbol(symbol)}"
    if "/" in name or "\\" in name:
        raise ValueError(
            f"symbol_dirname produced unsafe directory name {name!r} "
            f"for symbol={symbol!r}"
        )
    return name


def symbol_dest_dir(dest_root: Path, asset_type: str, symbol: str) -> Path:
    return dest_root / asset_type / symbol_dirname(symbol)


def is_already_transformed(dest_root: Path, asset_type: str, symbol: str) -> bool:
    return (symbol_dest_dir(dest_root, asset_type, symbol) / "metadata.json").exists()


# ---------------------------------------------------------------------------
# Source-file enumeration
# ---------------------------------------------------------------------------

_DATE_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def enumerate_daily_dates(daily_dir: Path) -> list[date]:
    """Sorted list of ``YYYY-MM-DD`` subfolders under ``daily/``."""
    if not daily_dir.is_dir():
        return []
    out: list[date] = []
    for entry in daily_dir.iterdir():
        if not entry.is_dir() or not _DATE_DIR_RE.match(entry.name):
            continue
        try:
            out.append(date.fromisoformat(entry.name))
        except ValueError:
            continue
    out.sort()
    return out


def _endpoint_dir(root: Path, asset_type: str, endpoint: str | None) -> Path:
    if asset_type in FLAT_ASSET_TYPES:
        return root / asset_type
    if endpoint is None:
        raise ValueError(
            f"endpoint is required for nested asset_type={asset_type!r}"
        )
    return root / asset_type / endpoint


def build_source_index(
    historical_dir: Path,
    daily_dir: Path,
    asset_type: str,
    endpoint: str | None,
    suffix: str = "",
) -> dict[str, list[Path]]:
    """Return ``{symbol -> [historical_path?, daily_path_1, daily_path_2, ...]}``
    for one ``(asset_type, endpoint)`` by scanning the source tree once.

    The historical path (when present) comes first; daily paths follow,
    sorted by folder date ascending. Symbols with no source files at all
    do not appear in the dict.

    *suffix* is passed through to the filename match (e.g. ``"_annual"``
    or ``"_quarterly"`` for fundamentals; default ``""`` for prices).
    """
    prefix = ASSET_TYPE_FILE_PREFIX[asset_type]
    suffix_with_ext = f"{suffix}.parquet"
    out: dict[str, list[Path]] = {}

    hist_dir = _endpoint_dir(historical_dir, asset_type, endpoint)
    if hist_dir.is_dir():
        for entry in hist_dir.iterdir():
            symbol = _symbol_from_filename(entry.name, prefix, suffix_with_ext)
            if symbol is not None:
                out.setdefault(symbol, []).append(entry)

    for d in enumerate_daily_dates(daily_dir):
        day_root = daily_dir / d.isoformat()
        day_endpoint_dir = _endpoint_dir(day_root, asset_type, endpoint)
        if not day_endpoint_dir.is_dir():
            continue
        for entry in day_endpoint_dir.iterdir():
            symbol = _symbol_from_filename(entry.name, prefix, suffix_with_ext)
            if symbol is not None:
                out.setdefault(symbol, []).append(entry)

    return out


def _symbol_from_filename(name: str, prefix: str, suffix_with_ext: str) -> str | None:
    if not name.startswith(prefix) or not name.endswith(suffix_with_ext):
        return None
    encoded = name[len(prefix) : -len(suffix_with_ext)]
    if not encoded:
        return None
    # Files are written via ``symbol_parquet_name`` which percent-encodes
    # path-unsafe characters via ``fs_symbol``. Reverse it here so callers
    # see the canonical ticker (``BC/PB``) rather than the on-disk form
    # (``BC%2FPB``); the result is then used as a dict key downstream.
    return unfs_symbol(encoded)


# ---------------------------------------------------------------------------
# Schema-strict casting
# ---------------------------------------------------------------------------

def cast_to_schema(
    df: pl.DataFrame,
    schema: dict[str, Any],
    frame_name: str,
) -> pl.DataFrame:
    """Project *df* onto *schema* exactly: same columns, same order, same
    dtypes. Required columns missing from *df* raise ``ValueError``; extra
    columns are dropped silently (the schema is the source of truth).
    Polars-side cast failures propagate (``strict=True``) so dtype drift
    surfaces immediately rather than being papered over.
    """
    missing = [c for c in schema if c not in df.columns]
    if missing:
        raise ValueError(
            f"frame {frame_name!r} is missing required columns: {missing}"
        )
    return df.select(
        [pl.col(name).cast(dtype, strict=True) for name, dtype in schema.items()]
    )


# ---------------------------------------------------------------------------
# Transformation report
# ---------------------------------------------------------------------------

REPORT_SCHEMA: dict[str, Any] = {
    "symbol": pl.Utf8,
    "asset_type": pl.Utf8,
    "frame": pl.Utf8,
    "issue_type": pl.Utf8,
    "count": pl.UInt32,
    "relative": pl.Float32,
    "detail": pl.Utf8,
    "timestamp": pl.Datetime("us", time_zone="UTC"),
}

ISSUE_TYPES = frozenset({
    "dedup_value_discrepancy_under_1pct",
    "dedup_value_discrepancy_over_1pct",
    "dedup_dropped_null_row",
    "intraday_orphan_date_dropped",
    "intraday_null_field",
    "schema_cast_failure",
    "financials_reportedDate_mismatch",
    "financials_fiscalDateEnding_offcycle",
    "financials_snapshot_fallback",
    "financials_estimate_offcycle",
    "financials_no_earnings_file",
    "financials_annual_no_quarterly_match",
})


class TransformationReport:
    """In-memory accumulator of per-symbol transformation issues, flushed
    once per run to ``<dest>/transformation_report.parquet``.

    The on-disk parquet is overwritten each run (it reflects the current
    transformation, not a cumulative log).
    """

    def __init__(self) -> None:
        self._rows: list[dict[str, Any]] = []

    def record(
        self,
        symbol: str,
        asset_type: str,
        frame: str,
        issue_type: str,
        count: int,
        relative: float | None = None,
        detail: str = "",
    ) -> None:
        if issue_type not in ISSUE_TYPES:
            raise ValueError(f"unknown issue_type: {issue_type!r}")
        self._rows.append(
            {
                "symbol": symbol,
                "asset_type": asset_type,
                "frame": frame,
                "issue_type": issue_type,
                "count": int(count),
                "relative": float(relative) if relative is not None else None,
                "detail": detail,
                "timestamp": datetime.now(tz=timezone.utc),
            }
        )
        logger.info(
            "transformation issue: %s/%s/%s/%s count=%d relative=%s detail=%s",
            asset_type, symbol, frame, issue_type,
            count,
            f"{relative:.4f}" if relative is not None else "n/a",
            detail,
        )

    def to_frame(self) -> pl.DataFrame:
        if not self._rows:
            return pl.DataFrame(schema=REPORT_SCHEMA)
        return pl.DataFrame(self._rows, schema=REPORT_SCHEMA)

    def flush(self, dest_root: Path) -> Path:
        path = dest_root / "transformation_report.parquet"
        dest_root.mkdir(parents=True, exist_ok=True)
        self.to_frame().write_parquet(path)
        return path
