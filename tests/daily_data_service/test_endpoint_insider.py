"""Tests for the daily insider endpoint's date cast.

Same shape as the historical sibling test: ``transactionDate`` strings
sometimes arrive as ``"YYYY-MM-DD-04:00"``. ``exact=False`` allows the
cast to consume the leading date and ignore the offset rather than
discard the symbol's whole frame.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))


def test_transaction_date_cast_handles_trailing_offset():
    """Mirrors daily_data_service/endpoints/insider.py:127."""
    df = pl.DataFrame({"transactionDate": [
        "2020-09-28-04:00",
        "2020-08-04-04:00",
        "2024-12-15",
    ]})
    out = df.with_columns(
        pl.col("transactionDate").str.to_date("%Y-%m-%d", exact=False)
    )
    assert out.schema["transactionDate"] == pl.Date
    assert out["transactionDate"].to_list() == [
        date(2020, 9, 28),
        date(2020, 8, 4),
        date(2024, 12, 15),
    ]
