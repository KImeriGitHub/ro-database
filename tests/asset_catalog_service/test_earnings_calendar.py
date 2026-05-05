"""Tests for the earnings_calendar update's date casts.

The full ``update_earnings_calendar`` is HTTP-bound; what we verify here
is that its ``str.to_date`` pattern (now with ``exact=False``) tolerates
trailing timezone offsets on both ``reportedDate`` and
``fiscalDateEnding`` while still recording a parse failure for genuinely
malformed strings via ``strict=False`` -> null.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))


def test_calendar_date_casts_handle_trailing_offset_and_still_null_garbage():
    """Mirrors asset_catalog_service/updates/earnings_calendar.py:38, 41.

    A ``-04:00`` suffix must parse to the leading date (``exact=False``);
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
