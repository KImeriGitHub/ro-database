"""Tests for ``historical_data_setup.earnings_calendar.fetch_earnings_calendar``.

The full fetch is HTTP-bound; the unit-level checks here cover:

1. The ``str.to_date`` pattern with ``exact=False`` tolerates trailing
   timezone offsets while still nulling genuinely malformed strings via
   ``strict=False`` (so the ``cast_issues`` column flags them).
2. The skip-if-exists guard: if ``earnings_calendar.parquet`` already
   exists in the target folder, no HTTP call is made and the file is left
   untouched.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from unittest.mock import patch

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from historical_data_setup.earnings_calendar import fetch_earnings_calendar


def test_calendar_date_casts_handle_trailing_offset_and_still_null_garbage():
    """A ``-04:00`` suffix must parse to the leading date (``exact=False``);
    a non-date string must still become null so the existing cast_issues
    column flags it (``strict=False``)."""
    df = pl.DataFrame({
        "reportedDate":     ["2025-02-14-04:00", "2025-03-01", "garbage"],
        "fiscalDateEnding": ["2024-12-31-04:00", "2024-09-30", "2024-12-31"],
    })
    out = df.with_columns(
        pl.col("reportedDate")
        .str.to_date("%Y-%m-%d", strict=False, exact=False)
        .alias("reportedDate_parsed"),
        pl.col("fiscalDateEnding")
        .str.to_date("%Y-%m-%d", strict=False, exact=False)
        .alias("fiscalDateEnding_parsed"),
    )

    assert out["reportedDate_parsed"].to_list() == [
        date(2025, 2, 14), date(2025, 3, 1), None,
    ]
    assert out["fiscalDateEnding_parsed"].to_list() == [
        date(2024, 12, 31), date(2024, 9, 30), date(2024, 12, 31),
    ]


def test_skip_if_exists_short_circuits_before_http(tmp_path):
    """When ``earnings_calendar.parquet`` already exists in *out_dir*, the
    function must return without invoking ``fetch_text``. This is what
    makes setup_historical/setup_daily resumes safe."""
    existing = tmp_path / "earnings_calendar.parquet"
    pl.DataFrame({"symbol": ["AAPL"]}).write_parquet(existing)

    with patch(
        "historical_data_setup.earnings_calendar.fetch_text"
    ) as mock_fetch:
        fetch_earnings_calendar("fake-key", tmp_path)
        mock_fetch.assert_not_called()

    # File untouched.
    df = pl.read_parquet(existing)
    assert df["symbol"].to_list() == ["AAPL"]
