"""Tests for ``daily_data_service._common`` and ``ensure_folders``.

Covers folder-date computation, the ``.setup_started_at`` marker lifecycle,
``yield_status.parquet`` lookups (previous-date and per-endpoint False set),
the date-window polars expressions, and the day-tree creator.

Pure unit tests -- no network, no real catalog or daily folder.
"""

from __future__ import annotations

import os
import sys
import time
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from daily_data_service import _common as dc
from daily_data_service.ensure_folders import DAILY_TREE, ensure_daily_folders


# ---------------------------------------------------------------------------
# compute_folder_date
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "wall_time, expected_offset",
    [
        # Weekday mornings/afternoons: roll back one day (market still open or
        # post-close cleanup not yet finished).
        (datetime(2026, 4, 15, 9, 0, tzinfo=dc.ET), -1),     # Wed 09:00
        (datetime(2026, 4, 15, 19, 59, tzinfo=dc.ET), -1),   # Wed 19:59
        # Exactly 20:00 ET on a weekday: today's date is fully traded.
        (datetime(2026, 4, 15, 20, 0, tzinfo=dc.ET), 0),     # Wed 20:00 sharp
        (datetime(2026, 4, 15, 23, 30, tzinfo=dc.ET), 0),    # Wed 23:30
    ],
)
def test_compute_folder_date_weekday_threshold(wall_time, expected_offset):
    """20:00 ET is the inclusive threshold that flips folder-date forward."""
    out = dc.compute_folder_date(wall_time)
    expected = wall_time.date()
    if expected_offset == -1:
        from datetime import timedelta
        expected = expected - timedelta(days=1)
    assert out == expected


def test_compute_folder_date_weekend_uses_start_date_regardless_of_time():
    """Saturday 03:00 -> Saturday; Sunday 23:30 -> Sunday. The 20:00 cutoff
    only matters on weekdays."""
    sat_morning = datetime(2026, 4, 18, 3, 0, tzinfo=dc.ET)
    sun_late    = datetime(2026, 4, 19, 23, 30, tzinfo=dc.ET)
    assert dc.compute_folder_date(sat_morning) == date(2026, 4, 18)
    assert dc.compute_folder_date(sun_late)    == date(2026, 4, 19)


def test_compute_folder_date_weekday_pre_open_rolls_to_previous_calendar_day():
    """Friday 02:00 -> Thursday (still pre-cutoff). This is the
    cross-calendar-day case the start marker is designed to keep stable."""
    out = dc.compute_folder_date(datetime(2026, 4, 17, 2, 0, tzinfo=dc.ET))
    assert out == date(2026, 4, 16)


def test_compute_folder_date_monday_rolls_back_to_sunday():
    """Monday 10:00 ET -> Sunday. The folder-date is whatever was the last
    fully-traded ET date as of the start time, even if that's a weekend."""
    out = dc.compute_folder_date(datetime(2026, 4, 13, 10, 0, tzinfo=dc.ET))  # Mon
    assert out == date(2026, 4, 12)  # Sun


# ---------------------------------------------------------------------------
# resolve_start_marker
# ---------------------------------------------------------------------------


def test_resolve_start_marker_creates_marker_on_first_run(tmp_path):
    """A fresh daily/ has no marker; ``resolve_start_marker`` must create it
    and return a start time consistent with the marker's mtime."""
    daily = tmp_path / "daily"  # missing on purpose
    started_at, folder_date, marker = dc.resolve_start_marker(daily)

    assert daily.exists()
    assert marker == daily / ".setup_started_at"
    assert marker.exists()
    # The reported start time must match what mtime says (no clock drift inside
    # the helper).
    expected = datetime.fromtimestamp(marker.stat().st_mtime, tz=dc.ET)
    assert started_at == expected
    assert folder_date == dc.compute_folder_date(started_at)


def test_resolve_start_marker_resumes_from_existing_mtime(tmp_path):
    """A second call must read the mtime, not stamp it again. The folder-date
    must be derived from the original mtime even if the calendar day has
    advanced since then."""
    daily = tmp_path / "daily"
    daily.mkdir()
    marker = daily / ".setup_started_at"
    marker.touch()
    # Pin mtime to a specific Wed 21:00 ET (post-cutoff -> folder-date == start
    # date). 2026-04-15 21:00 ET == 2026-04-16 01:00 UTC.
    pinned = datetime(2026, 4, 15, 21, 0, tzinfo=dc.ET).timestamp()
    os.utime(marker, (pinned, pinned))

    started_at, folder_date, returned_marker = dc.resolve_start_marker(daily)

    assert returned_marker == marker
    assert started_at.date() == date(2026, 4, 15)
    assert started_at.hour == 21
    # Wed 21:00 ET >= 20:00 -> folder-date == start date.
    assert folder_date == date(2026, 4, 15)


def test_resolve_start_marker_two_consecutive_calls_agree(tmp_path):
    """Calling twice in a row must return the same start time both times --
    that's the whole point of a persistent marker."""
    daily = tmp_path / "daily"
    first_started, first_fd, _ = dc.resolve_start_marker(daily)
    # Sleep just enough to guarantee a different "now"; mtime must still pin.
    time.sleep(0.05)
    second_started, second_fd, _ = dc.resolve_start_marker(daily)
    assert first_started == second_started
    assert first_fd == second_fd


# ---------------------------------------------------------------------------
# read_previous_date
# ---------------------------------------------------------------------------


def _write_yield_status(catalog_dir: Path, rows: list[dict]) -> Path:
    """Create catalog/yield_status.parquet from *rows*. Each row must include
    'symbol' and 'date'; arbitrary extra endpoint columns are passed through.
    """
    catalog_dir.mkdir(parents=True, exist_ok=True)
    path = catalog_dir / "yield_status.parquet"
    df = pl.DataFrame(rows)
    df.write_parquet(path)
    return path


def _make_prior_daily_subdir(daily_dir: Path, d: date) -> Path:
    """Create a YYYY-MM-DD subdir so read_previous_date leaves the bootstrap branch."""
    sub = daily_dir / d.isoformat()
    sub.mkdir(parents=True, exist_ok=True)
    return sub


def test_read_previous_date_returns_first_row_value(tmp_path):
    catalog = tmp_path / "catalog"
    daily = tmp_path / "daily"
    folder_date = date(2026, 4, 15)
    _make_prior_daily_subdir(daily, date(2026, 4, 14))
    _write_yield_status(catalog, [
        {"symbol": "AAPL", "date": date(2026, 4, 14), "prices_daily": True},
        {"symbol": "MSFT", "date": date(2026, 4, 14), "prices_daily": True},
    ])
    assert dc.read_previous_date(catalog, daily, folder_date) == date(2026, 4, 14)


def test_read_previous_date_missing_file_raises(tmp_path):
    daily = tmp_path / "daily"
    folder_date = date(2026, 4, 15)
    # Prior subdir present -> bootstrap branch skipped -> file lookup -> raise.
    _make_prior_daily_subdir(daily, date(2026, 4, 14))
    with pytest.raises(FileNotFoundError, match="yield_status.parquet"):
        dc.read_previous_date(tmp_path / "catalog", daily, folder_date)


def test_read_previous_date_empty_file_raises(tmp_path):
    catalog = tmp_path / "catalog"
    catalog.mkdir()
    daily = tmp_path / "daily"
    folder_date = date(2026, 4, 15)
    _make_prior_daily_subdir(daily, date(2026, 4, 14))
    pl.DataFrame(
        {"symbol": [], "date": []},
        schema={"symbol": pl.Utf8, "date": pl.Date},
    ).write_parquet(catalog / "yield_status.parquet")
    with pytest.raises(ValueError, match="empty"):
        dc.read_previous_date(catalog, daily, folder_date)


def test_read_previous_date_bootstrap_no_daily_subdirs(tmp_path):
    """No prior daily/<date>/ subdir -> fall back to folder_date - 7 without
    touching yield_status.parquet (file absent entirely)."""
    daily = tmp_path / "daily"
    daily.mkdir()
    folder_date = date(2026, 4, 15)
    assert dc.read_previous_date(
        tmp_path / "catalog_missing", daily, folder_date
    ) == folder_date - timedelta(days=dc.PRICE_WINDOW_DAYS)


def test_read_previous_date_bootstrap_only_folder_date_subdir(tmp_path):
    """Resume case: only daily/<folder_date>/ exists (created mid-run). Still
    bootstrap -> folder_date - 7, even if yield_status.parquet has a date set."""
    catalog = tmp_path / "catalog"
    daily = tmp_path / "daily"
    folder_date = date(2026, 4, 15)
    _make_prior_daily_subdir(daily, folder_date)  # same date as folder_date
    _write_yield_status(catalog, [
        {"symbol": "AAPL", "date": folder_date, "prices_daily": True},
    ])
    assert dc.read_previous_date(catalog, daily, folder_date) == \
        folder_date - timedelta(days=dc.PRICE_WINDOW_DAYS)


def test_read_previous_date_bootstrap_daily_dir_missing(tmp_path):
    """daily/ directory itself does not exist -> bootstrap."""
    folder_date = date(2026, 4, 15)
    assert dc.read_previous_date(
        tmp_path / "catalog", tmp_path / "daily_missing", folder_date
    ) == folder_date - timedelta(days=dc.PRICE_WINDOW_DAYS)


# ---------------------------------------------------------------------------
# read_yield_skip_set
# ---------------------------------------------------------------------------


def test_read_yield_skip_set_only_explicit_false(tmp_path):
    """True / False / Null cells must map to: keep / skip / keep. The skip set
    is the contract that lets weekday runs avoid wasted API calls; null cells
    are NEW symbols that haven't been scored yet, so they MUST stay queryable."""
    catalog = tmp_path / "catalog"
    _write_yield_status(catalog, [
        {"symbol": "AAPL", "date": date(2026, 4, 14), "income_statement": True},
        {"symbol": "TSLA", "date": date(2026, 4, 14), "income_statement": False},
        {"symbol": "NEWCO", "date": date(2026, 4, 14), "income_statement": None},
        {"symbol": "GOOG", "date": date(2026, 4, 14), "income_statement": False},
    ])
    skip = dc.read_yield_skip_set(catalog, "income_statement")
    assert skip == {"TSLA", "GOOG"}


def test_read_yield_skip_set_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError, match="yield_status.parquet"):
        dc.read_yield_skip_set(tmp_path / "catalog", "income_statement")


def test_read_yield_skip_set_unknown_endpoint_column_raises(tmp_path):
    """Polars raises if a requested column doesn't exist; this is the right
    behaviour because a typo'd endpoint name would otherwise silently return
    an empty skip set and waste API calls."""
    catalog = tmp_path / "catalog"
    _write_yield_status(catalog, [
        {"symbol": "AAPL", "date": date(2026, 4, 14), "income_statement": True},
    ])
    with pytest.raises(Exception):
        dc.read_yield_skip_set(catalog, "nonexistent_endpoint")


# ---------------------------------------------------------------------------
# window_expr / since_expr filters
# ---------------------------------------------------------------------------


def test_window_expr_excludes_previous_date_includes_folder_date():
    """Truncation contract: ``(previous-date, folder-date]`` -- left open,
    right closed. Verifies both edges with a Date column."""
    df = pl.DataFrame({
        "Date": [date(2026, 4, 13), date(2026, 4, 14), date(2026, 4, 15),
                 date(2026, 4, 16), date(2026, 4, 17)],
        "x": [1, 2, 3, 4, 5],
    })
    out = df.filter(dc.window_expr("Date", date(2026, 4, 14), date(2026, 4, 16)))
    assert out["Date"].to_list() == [date(2026, 4, 15), date(2026, 4, 16)]
    assert out["x"].to_list() == [3, 4]


def test_window_expr_works_on_datetime_column_via_implicit_cast():
    """Datetime columns accept date-typed bounds (polars casts the literal up
    to Datetime at midnight). The cast is strict at both edges:

      - lower bound ``> previous_date``  -> ``> previous_date 00:00``
      - upper bound ``<= folder_date``   -> ``<= folder_date 00:00``

    Result: any timestamp strictly between previous_date 00:00 and
    folder_date 00:00 (inclusive) is kept. Intraday endpoints rely on this
    when filtering 1-min bars in ``(previous-date, folder-date]``.
    """
    df = pl.DataFrame({
        "Date": [
            datetime(2026, 4, 14, 0, 0),    # excluded (== previous_date midnight)
            datetime(2026, 4, 14, 23, 59),  # included (> previous_date midnight)
            datetime(2026, 4, 15, 12, 0),   # included
            datetime(2026, 4, 16, 0, 0),    # included (== folder_date midnight)
            datetime(2026, 4, 16, 0, 1),    # excluded (> folder_date midnight)
        ],
    })
    out = df.filter(dc.window_expr("Date", date(2026, 4, 14), date(2026, 4, 16)))
    times = out["Date"].to_list()
    assert datetime(2026, 4, 14, 0, 0) not in times
    assert datetime(2026, 4, 14, 23, 59) in times
    assert datetime(2026, 4, 15, 12, 0) in times
    assert datetime(2026, 4, 16, 0, 0) in times
    assert datetime(2026, 4, 16, 0, 1) not in times


def test_since_expr_inclusive_on_lower_bound():
    """The 5-year fundamentals cutoff must keep the cutoff date itself."""
    df = pl.DataFrame({
        "fiscalDateEnding": [date(2020, 12, 31), date(2021, 4, 15),
                             date(2021, 4, 16), date(2021, 5, 1)],
    })
    out = df.filter(dc.since_expr("fiscalDateEnding", date(2021, 4, 16)))
    assert out["fiscalDateEnding"].to_list() == [date(2021, 4, 16), date(2021, 5, 1)]


# ---------------------------------------------------------------------------
# years_before
# ---------------------------------------------------------------------------


def test_years_before_normal_case():
    assert dc.years_before(date(2026, 4, 15), 5) == date(2021, 4, 15)
    assert dc.years_before(date(2026, 4, 15), 1) == date(2025, 4, 15)


def test_years_before_leap_day_clamps_to_feb28_in_non_leap():
    """Feb 29 minus 1 year falls in a non-leap year -- must clamp to Feb 28
    rather than raising. 2024 was a leap year, 2023 wasn't."""
    assert dc.years_before(date(2024, 2, 29), 1) == date(2023, 2, 28)


def test_years_before_leap_day_to_leap_day_unchanged():
    """Leap day to leap day (4 years apart) stays Feb 29."""
    assert dc.years_before(date(2024, 2, 29), 4) == date(2020, 2, 29)


# ---------------------------------------------------------------------------
# ensure_daily_folders
# ---------------------------------------------------------------------------


def test_ensure_daily_folders_creates_full_subtree(tmp_path):
    daily = tmp_path / "daily"
    folder_date = date(2026, 4, 18)

    day_root = ensure_daily_folders(daily, folder_date)

    assert day_root == daily / "2026-04-18"
    assert day_root.is_dir()
    for leaf in DAILY_TREE:
        assert (day_root / leaf).is_dir(), f"missing {leaf}"


def test_ensure_daily_folders_idempotent_and_preserves_existing_files(tmp_path):
    """A re-run must not wipe parquet files that already exist in the tree --
    that's the entire premise of resume."""
    daily = tmp_path / "daily"
    folder_date = date(2026, 4, 18)
    day_root = ensure_daily_folders(daily, folder_date)

    # Drop a sentinel file in one of the leaves.
    sentinel = day_root / "stocks" / "prices_daily" / "stocks_AAPL.parquet"
    sentinel.write_bytes(b"resume me")

    # Calling again must be a no-op for existing dirs and must NOT touch the
    # sentinel file.
    day_root_again = ensure_daily_folders(daily, folder_date)
    assert day_root_again == day_root
    assert sentinel.read_bytes() == b"resume me"


def test_ensure_daily_folders_per_date_isolation(tmp_path):
    """Each folder-date gets its own subtree -- past dates must not be touched
    when ensure_daily_folders is called for a new date."""
    daily = tmp_path / "daily"
    earlier = ensure_daily_folders(daily, date(2026, 4, 17))
    (earlier / "stocks" / "prices_daily" / "stocks_AAPL.parquet").write_bytes(b"old")

    later = ensure_daily_folders(daily, date(2026, 4, 18))
    assert later != earlier
    assert (earlier / "stocks" / "prices_daily" / "stocks_AAPL.parquet").read_bytes() == b"old"
