"""Tests for the historical insider endpoint's date cast.

The full ``fetch_insider`` coroutine is end-to-end (catalog read, rate
limited HTTP, parquet write). What we verify here is the cast pattern it
uses on ``transactionDate`` -- the actual production crash mode was a
strict ``str.to_date`` rejecting AV's occasional ``"YYYY-MM-DD-04:00"``
form and dropping the symbol's entire insider history.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))


def test_transaction_date_cast_handles_trailing_offset():
    """Mirrors historical_data_setup/endpoints/insider.py:127. AV sometimes
    appends a ``-04:00`` timezone offset to ``transactionDate`` for liquid
    names (the GS log line that motivated this change). ``exact=False`` must
    consume the leading YYYY-MM-DD so the cast no longer dies on the whole
    symbol."""
    df = pl.DataFrame({"transactionDate": [
        "2020-09-28-04:00",
        "2020-08-04-04:00",
        "2019-01-15",
    ]})
    out = df.with_columns(
        pl.col("transactionDate").str.to_date("%Y-%m-%d", exact=False)
    )
    assert out.schema["transactionDate"] == pl.Date
    assert out["transactionDate"].to_list() == [
        date(2020, 9, 28),
        date(2020, 8, 4),
        date(2019, 1, 15),
    ]
