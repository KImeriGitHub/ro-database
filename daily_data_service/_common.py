"""Service-specific helpers for the daily pull: folder-date resolution from
a top-level start marker, previous-date lookup from yield_status, and date-
window filter utilities used across endpoints."""

import logging
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

logger = logging.getLogger(__name__)

ET = ZoneInfo("America/New_York")


def compute_folder_date(started_at_et: datetime) -> date:
    """Last fully-traded ET date at *started_at_et*.

    Weekend -> start date.
    Weekday, time >= 20:00 ET -> start date.
    Weekday, time <  20:00 ET -> start date minus one day.
    """
    start_date = started_at_et.date()
    if started_at_et.weekday() >= 5:
        return start_date
    if started_at_et.time() >= time(20, 0):
        return start_date
    return start_date - timedelta(days=1)


def resolve_start_marker(daily_dir: Path) -> tuple[datetime, date, Path]:
    """Return ``(started_at_et, folder_date, marker_path)``.

    If ``daily_dir/.setup_started_at`` exists, recover the start time from
    its mtime (resume path). Otherwise create the marker and return the
    current ET time. The folder-date is always derived from the returned
    start time via :func:`compute_folder_date`.
    """
    daily_dir.mkdir(parents=True, exist_ok=True)
    marker = daily_dir / ".setup_started_at"
    if marker.exists():
        started_at = datetime.fromtimestamp(marker.stat().st_mtime, tz=ET)
        logger.info(f"Resuming run with start marker mtime {started_at.isoformat()}")
    else:
        marker.touch()
        started_at = datetime.fromtimestamp(marker.stat().st_mtime, tz=ET)
        logger.info(f"Created start marker at {marker} ({started_at.isoformat()})")
    return started_at, compute_folder_date(started_at), marker


def read_previous_date(catalog_dir: Path) -> date:
    """Return the ``date`` value from ``catalog/yield_status.parquet``.

    All rows share the same date; the first row is sufficient.
    """
    path = catalog_dir / "yield_status.parquet"
    if not path.exists():
        raise FileNotFoundError(
            f"yield_status.parquet not found at {path}; run historical setup first"
        )
    df = pl.read_parquet(path, columns=["date"])
    if df.height == 0:
        raise ValueError(f"yield_status.parquet at {path} is empty")
    return df["date"][0]


def window_expr(col: str, previous_date: date, folder_date: date) -> pl.Expr:
    """Polars expression for ``previous_date < col <= folder_date``.

    Works for both ``pl.Date`` and ``pl.Datetime`` columns -- polars casts
    the literal ``date`` to the column's dtype on the fly.
    """
    return (pl.col(col) > previous_date) & (pl.col(col) <= folder_date)


def since_expr(col: str, since: date) -> pl.Expr:
    """Polars expression for ``col >= since``."""
    return pl.col(col) >= since


def years_before(d: date, years: int) -> date:
    """Return ``d - years``, clamping Feb 29 to Feb 28 in non-leap years."""
    try:
        return d.replace(year=d.year - years)
    except ValueError:
        return d.replace(month=2, day=28, year=d.year - years)
