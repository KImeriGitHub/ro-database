"""Tests for shared helpers in ``historical_data_setup._common``.

Covers the pure pieces that ``test_rate_limiter.py`` and
``test_cross_endpoint.py`` don't touch: month generation, the issue tracker,
filename prefixing, FRD CSV lookup, ``Meta Data`` validation, the
fundamental-DataFrame builder, the AV-call counter, and ``fetch_av_json``'s
throttle/retry behaviour.

Plus a couple of tests for ``ensure_historical_folders`` that mirror the
equivalent daily checks.
"""

from __future__ import annotations

import asyncio
import sys
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from historical_data_setup import _common as hc
from historical_data_setup._common import (
    AVResponseError,
    IssueTracker,
    RateLimiter,
    _build_fundamental_df,
    fetch_av_json,
    frd_csv_path,
    generate_months,
    get_av_call_count,
    read_catalog_symbols,
    reset_av_call_count,
    symbol_parquet_name,
    validate_meta_data,
)
from historical_data_setup.ensure_folders import HISTORICAL_TREE, ensure_historical_folders


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# generate_months
# ---------------------------------------------------------------------------


def test_generate_months_clamps_to_2000_when_ipo_predates_it():
    """ipo_date earlier than 2000-01 must clamp to 2000-01 (the AV intraday
    horizon) -- otherwise we'd issue calls for months AV will not serve."""
    months = generate_months(date(1980, 5, 15), date(2000, 4, 30))
    assert months[0] == "2000-01"
    assert months[-1] == "2000-04"
    assert months == ["2000-01", "2000-02", "2000-03", "2000-04"]


def test_generate_months_uses_first_of_month_for_ipo():
    """An IPO mid-month should still include that month's start. The first
    output entry is the IPO's *month*, not its day."""
    months = generate_months(date(2024, 3, 17), date(2024, 5, 2))
    assert months == ["2024-03", "2024-04", "2024-05"]


def test_generate_months_ipo_after_delisting_returns_empty():
    """Bogus catalog rows where ipo > delisting should produce an empty range,
    not an exception."""
    assert generate_months(date(2024, 6, 1), date(2024, 3, 1)) == []


def test_generate_months_handles_year_boundary():
    """December -> January transition must increment the year."""
    months = generate_months(date(2023, 11, 15), date(2024, 2, 10))
    assert months == ["2023-11", "2023-12", "2024-01", "2024-02"]


def test_generate_months_invalid_string_raises():
    """A malformed string is a structure error: the caller must record and
    skip the symbol rather than silently using a default range."""
    with pytest.raises(ValueError, match="ipoDate"):
        generate_months("not-a-date", date(2024, 1, 1))
    with pytest.raises(ValueError, match="delistingDate"):
        generate_months(date(2024, 1, 1), "also-bad")


def test_generate_months_string_inputs_warn_and_coerce(caplog):
    """Strings are accepted for back-compat but emit a warning; the catalog
    is supposed to store pl.Date."""
    import logging
    with caplog.at_level(logging.WARNING, logger="historical_data_setup._common"):
        months = generate_months("2024-03-17", "2024-05-02")
    assert months == ["2024-03", "2024-04", "2024-05"]
    assert any(
        "ipoDate" in r.getMessage() and "str" in r.getMessage()
        for r in caplog.records
    )


def test_generate_months_none_inputs_use_defaults():
    """``None``/``None`` -> 2000-01 to today, per the README spec
    ``max(ipoDate, 2000-01)`` / ``min(delistingDate, today)``. Active stocks
    normally have ``delistingDate=None``."""
    months = generate_months(None, None)
    assert months[0] == "2000-01"
    assert all(len(m) == 7 and m[4] == "-" for m in months)


# ---------------------------------------------------------------------------
# IssueTracker
# ---------------------------------------------------------------------------


def test_issue_tracker_save_writes_parquet_with_expected_schema(tmp_path):
    """Round-trip through parquet must preserve the documented column order
    and dtypes -- monitoring_service reads this file and expects them."""
    t = IssueTracker()
    t.record("AAPL", "stocks", "prices_daily", "structure_error", "missing key")
    t.record("MSFT", "stocks", "prices_daily", "av_throttle",     "rate-limited")
    assert t.count == 2

    out = tmp_path / "ingestion_report.parquet"
    t.save(out)

    df = pl.read_parquet(out)
    assert df.height == 2
    assert df.columns == [
        "symbol", "asset_type", "endpoint", "issue_type", "detail", "timestamp",
    ]
    # Dtypes contract: every column except timestamp is Utf8.
    for col in df.columns[:-1]:
        assert df.schema[col] == pl.Utf8, f"{col} dtype drifted: {df.schema[col]}"
    assert df.schema["timestamp"] in (pl.Datetime, pl.Datetime("us"))


def test_issue_tracker_save_appends_to_existing_file(tmp_path):
    """A second save() must merge with what's already on disk -- the daily
    pipeline writes after each endpoint task completes."""
    out = tmp_path / "ingestion_report.parquet"
    t1 = IssueTracker()
    t1.record("AAPL", "stocks", "prices_daily", "empty_content", "no rows")
    t1.save(out)
    assert pl.read_parquet(out).height == 1

    t2 = IssueTracker()
    t2.record("MSFT", "stocks", "prices_daily", "av_throttle", "throttled")
    t2.save(out)

    df = pl.read_parquet(out)
    assert df.height == 2
    assert sorted(df["symbol"].to_list()) == ["AAPL", "MSFT"]


def test_issue_tracker_empty_save_does_not_create_file(tmp_path):
    """An IssueTracker with zero rows must not write any file -- otherwise the
    monitoring report would over-report the previous run's issues."""
    out = tmp_path / "ingestion_report.parquet"
    IssueTracker().save(out)
    assert not out.exists()


# ---------------------------------------------------------------------------
# symbol_parquet_name
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("asset_type, prefix", [
    ("stocks", "stocks_"),
    ("etfs", "etfs_"),
    ("forex", "forex_"),
    ("indices", "indices_"),
    ("cryptocurrencies", "cryptocurrencies_"),
    ("commodities", "commodities_"),
    ("economic", "economic_"),
])
def test_symbol_parquet_name_prefix_per_asset_type(asset_type, prefix):
    name = symbol_parquet_name(asset_type, "AAPL")
    assert name == f"{prefix}AAPL.parquet"


def test_symbol_parquet_name_handles_windows_reserved_ticker():
    """``PRN`` is a reserved Windows device name; the prefix must save the
    file from being unwritable. This is the entire reason the prefix exists."""
    assert symbol_parquet_name("etfs", "PRN") == "etfs_PRN.parquet"
    assert symbol_parquet_name("stocks", "CON", "_annual") == "stocks_CON_annual.parquet"


def test_symbol_parquet_name_appends_suffix_before_extension():
    assert symbol_parquet_name("stocks", "AAPL", "_quarterly") == "stocks_AAPL_quarterly.parquet"


def test_symbol_parquet_name_unknown_asset_type_raises():
    with pytest.raises(KeyError):
        symbol_parquet_name("bonds", "AAPL")


# ---------------------------------------------------------------------------
# frd_csv_path
# ---------------------------------------------------------------------------


def test_frd_csv_path_returns_path_when_file_exists(tmp_path):
    (tmp_path / "AAPL_1min.csv").write_text("dummy")
    assert frd_csv_path(tmp_path, "AAPL", "1min") == tmp_path / "AAPL_1min.csv"


def test_frd_csv_path_returns_none_for_missing_file(tmp_path):
    assert frd_csv_path(tmp_path, "AAPL", "1min") is None


def test_frd_csv_path_none_dir_returns_none(tmp_path):
    """``frd_dir=None`` must short-circuit before any filesystem access -- the
    historical setup runs without FRD when the user didn't pass --stocks-dir."""
    assert frd_csv_path(None, "AAPL", "1min") is None


# ---------------------------------------------------------------------------
# read_catalog_symbols
# ---------------------------------------------------------------------------


def test_read_catalog_symbols_returns_full_dataframe(tmp_path):
    """No filtering by status -- every row in the parquet is returned.
    Endpoints decide what to do with delisted/inactive entries themselves."""
    pl.DataFrame({
        "symbol": ["AAPL", "MSFT", "DEAD"],
        "status": ["Active", "Active", "Delisted"],
    }).write_parquet(tmp_path / "stocks.parquet")

    df = read_catalog_symbols(tmp_path, "stocks")
    assert df.height == 3
    assert df["symbol"].to_list() == ["AAPL", "MSFT", "DEAD"]


def test_read_catalog_symbols_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError, match="Catalog file not found"):
        read_catalog_symbols(tmp_path, "stocks")


# ---------------------------------------------------------------------------
# validate_meta_data
# ---------------------------------------------------------------------------


def test_validate_meta_data_missing_records_structure_error():
    t = IssueTracker()
    ok = validate_meta_data({}, "AAPL", "stocks", "prices", t)
    assert ok is False
    assert t.count == 1
    assert t._rows[0]["issue_type"] == "structure_error"
    assert "Meta Data" in t._rows[0]["detail"]


def test_validate_meta_data_correct_timezone_records_no_issue():
    t = IssueTracker()
    data = {"Meta Data": {
        "1. Information": "Intraday",
        "5. Time Zone": "US/Eastern",
    }}
    assert validate_meta_data(data, "AAPL", "stocks", "prices", t) is True
    assert t.count == 0


def test_validate_meta_data_wrong_timezone_records_mismatch_but_returns_true():
    """A timezone mismatch is a soft error: the data is still saved, but the
    issue is logged so finalize_yield_status can decide what to do.

    Returning True here is the contract that lets the endpoint keep parsing.
    """
    t = IssueTracker()
    data = {"Meta Data": {
        "1. Information": "Intraday",
        "6. Time Zone": "UTC",  # numbered keys vary across endpoints
    }}
    assert validate_meta_data(data, "AAPL", "stocks", "prices", t) is True
    assert t.count == 1
    assert t._rows[0]["issue_type"] == "timezone_mismatch"
    assert "tz=UTC" in t._rows[0]["detail"]


def test_validate_meta_data_custom_expected_tz():
    """FX endpoints expect ``UTC``; the helper accepts a custom expected_tz."""
    t = IssueTracker()
    data = {"Meta Data": {"5. Time Zone": "UTC"}}
    assert validate_meta_data(data, "EURUSD", "forex", "forex", t,
                              expected_tz="UTC") is True
    assert t.count == 0


def test_validate_meta_data_only_first_timezone_key_consumed():
    """The regex breaks on the first ``\\d+\\.\\s*Time Zone`` match. A response
    with a stray non-matching timezone-like key shouldn't get the helper
    confused; first numbered timezone wins."""
    t = IssueTracker()
    data = {"Meta Data": {
        "5. Time Zone": "US/Eastern",
        "Time Zone Comment": "ignored",
    }}
    assert validate_meta_data(data, "AAPL", "stocks", "prices", t) is True
    assert t.count == 0


# ---------------------------------------------------------------------------
# _build_fundamental_df
# ---------------------------------------------------------------------------


def test_build_fundamental_df_replaces_null_sentinels_with_null():
    """``"None"``, ``""``, ``"."``, and Python ``None`` all map to nulls --
    a single source of truth lets fundamental endpoints share one cleanup
    path. Sentinel-only values must NOT count as cast failures."""
    t = IssueTracker()
    records = [
        {"fiscalDateEnding": "2024-12-31", "reportedCurrency": "USD",
         "totalRevenue": "1000", "operatingIncome": "None"},
        {"fiscalDateEnding": "2023-12-31", "reportedCurrency": "USD",
         "totalRevenue": ".", "operatingIncome": ""},
        {"fiscalDateEnding": "2022-12-31", "reportedCurrency": "USD",
         "totalRevenue": None, "operatingIncome": "200"},
    ]
    df = _build_fundamental_df(records, "AAPL", "stocks", "income_statement",
                               "annual", t)
    assert df is not None
    assert df.height == 3
    assert df.schema["fiscalDateEnding"] == pl.Date
    assert df.schema["reportedCurrency"] == pl.Utf8
    assert df.schema["totalRevenue"] == pl.Float32
    assert df.schema["operatingIncome"] == pl.Float32
    # Sorted ascending by fiscalDateEnding -> 2022, 2023, 2024.
    assert df["totalRevenue"].to_list() == [None, None, 1000.0]
    assert df["operatingIncome"].to_list() == [200.0, None, None]
    assert t.count == 0  # null sentinels are not cast failures


def test_build_fundamental_df_records_cast_failure_for_bad_numeric():
    """A genuinely non-numeric value must trigger a ``cast_failure`` issue
    AND force-null the offending cell -- the column stays Float32, never
    silently demoted to String. The other rows must survive intact."""
    t = IssueTracker()
    records = [
        {"fiscalDateEnding": "2024-12-31", "reportedCurrency": "USD",
         "totalRevenue": "1000"},
        {"fiscalDateEnding": "2023-12-31", "reportedCurrency": "USD",
         "totalRevenue": "not-a-number"},
    ]
    df = _build_fundamental_df(records, "AAPL", "stocks", "income_statement",
                               "annual", t)
    assert df is not None
    assert df.schema["totalRevenue"] == pl.Float32
    # Sorted ascending: 2023 row first, with its bad value forced to null.
    assert df["totalRevenue"].to_list() == [None, 1000.0]
    cast_failures = [r for r in t._rows if r["issue_type"] == "cast_failure"]
    assert len(cast_failures) == 1
    assert "totalRevenue" in cast_failures[0]["detail"]


def test_build_fundamental_df_missing_fiscal_date_records_structure_error():
    """No fiscalDateEnding column at all -> structure_error and a None df."""
    t = IssueTracker()
    records = [{"reportedCurrency": "USD", "totalRevenue": "100"}]
    df = _build_fundamental_df(records, "AAPL", "stocks", "income_statement",
                               "annual", t)
    assert df is None
    structure = [r for r in t._rows if r["issue_type"] == "structure_error"]
    assert len(structure) == 1
    assert "fiscalDateEnding" in structure[0]["detail"]


def test_build_fundamental_df_empty_input_returns_none():
    """Empty input -> ``None``. Callers are responsible for logging
    ``empty_content`` themselves; the builder must not double-log."""
    t = IssueTracker()
    assert _build_fundamental_df([], "AAPL", "stocks", "income_statement",
                                 "annual", t) is None
    assert t.count == 0


def test_build_fundamental_df_sorts_by_fiscal_date_ending_ascending():
    """Output ordering matters for downstream PIT joins -- always ascending."""
    t = IssueTracker()
    records = [
        {"fiscalDateEnding": "2023-12-31", "reportedCurrency": "USD"},
        {"fiscalDateEnding": "2021-12-31", "reportedCurrency": "USD"},
        {"fiscalDateEnding": "2024-12-31", "reportedCurrency": "USD"},
    ]
    df = _build_fundamental_df(records, "AAPL", "stocks", "income_statement",
                               "annual", t)
    assert df is not None
    assert df["fiscalDateEnding"].to_list() == [
        date(2021, 12, 31), date(2023, 12, 31), date(2024, 12, 31),
    ]


def test_build_fundamental_df_accepts_fiscal_date_with_trailing_offset():
    """AV occasionally returns ``YYYY-MM-DD-04:00`` style strings for date
    fields. ``str.to_date(..., exact=False)`` must consume only the leading
    YYYY-MM-DD and ignore the timezone offset, otherwise the whole report
    is dropped (a single bad row used to throw away every fundamental for
    the symbol)."""
    t = IssueTracker()
    records = [
        {"fiscalDateEnding": "2024-12-31-04:00", "reportedCurrency": "USD",
         "totalRevenue": "1000"},
        {"fiscalDateEnding": "2023-12-31", "reportedCurrency": "USD",
         "totalRevenue": "900"},
    ]
    df = _build_fundamental_df(records, "AAPL", "stocks", "income_statement",
                               "annual", t)
    assert df is not None
    assert df.schema["fiscalDateEnding"] == pl.Date
    assert df["fiscalDateEnding"].to_list() == [date(2023, 12, 31), date(2024, 12, 31)]
    assert t.count == 0


def test_build_fundamental_df_accepts_reported_date_with_trailing_offset():
    """Same lax handling as fiscalDateEnding: a ``-04:00`` suffix on
    ``reportedDate`` must parse, not record a cast_failure."""
    t = IssueTracker()
    records = [
        {"fiscalDateEnding": "2024-12-31", "reportedDate": "2025-02-14-04:00",
         "reportedCurrency": "USD", "totalRevenue": "1000"},
    ]
    df = _build_fundamental_df(records, "AAPL", "stocks", "income_statement",
                               "annual", t)
    assert df is not None
    assert df.schema["reportedDate"] == pl.Date
    assert df["reportedDate"].to_list() == [date(2025, 2, 14)]
    assert t.count == 0


def test_coerce_date_accepts_string_with_trailing_offset(caplog):
    """``_coerce_date`` (via ``generate_months``) uses a custom strptime that
    mirrors polars ``exact=False``: the leading YYYY-MM-DD is consumed and
    any trailing offset is ignored. Truly malformed strings still raise."""
    import logging
    with caplog.at_level(logging.WARNING, logger="historical_data_setup._common"):
        months = generate_months("2024-03-17-04:00", "2024-05-02-04:00")
    assert months == ["2024-03", "2024-04", "2024-05"]

    with pytest.raises(ValueError, match="ipoDate"):
        generate_months("garbage", date(2024, 1, 1))


# ---------------------------------------------------------------------------
# AV call counter
# ---------------------------------------------------------------------------


def test_av_call_counter_resets_to_zero():
    """Each daily/historical run resets the counter so the monitoring report's
    ``api_calls.total`` reflects only that run's HTTP traffic."""
    # Bump the counter via a direct fetch_av_json with a stub session to be
    # representative of real usage.
    class _Resp:
        status = 200
        async def json(self, content_type=None): return {"ok": True}
        def raise_for_status(self): return None
    class _Ctx:
        async def __aenter__(self): return _Resp()
        async def __aexit__(self, *exc): return False
    class _Session:
        def get(self, url, timeout=None): return _Ctx()

    async def runner():
        rl = RateLimiter(calls_per_minute=1000, window=1.0, min_gap=0.0)
        await fetch_av_json("https://fake/x", _Session(), rl)
        await fetch_av_json("https://fake/y", _Session(), rl)

    _run(runner())
    assert get_av_call_count() >= 2

    reset_av_call_count()
    assert get_av_call_count() == 0


# ---------------------------------------------------------------------------
# fetch_av_json: throttle / retry behaviour
# ---------------------------------------------------------------------------


class _ScriptedResp:
    """Returns a fixed JSON payload; raise_for_status is a no-op."""
    status = 200

    def __init__(self, data: dict):
        self._data = data

    def raise_for_status(self) -> None:
        return None

    async def json(self, content_type=None):
        return self._data


class _ScriptedCtx:
    def __init__(self, data: dict):
        self._data = data

    async def __aenter__(self):
        return _ScriptedResp(self._data)

    async def __aexit__(self, *exc):
        return False


class _ScriptedSession:
    """Pops payloads off ``responses`` for each successive ``get`` call."""

    def __init__(self, responses: list[dict]):
        self._responses = list(responses)
        self.requests: list[str] = []

    def get(self, url, timeout=None):
        self.requests.append(url)
        if not self._responses:
            raise AssertionError("MockSession ran out of scripted responses")
        return _ScriptedCtx(self._responses.pop(0))


def test_fetch_av_json_returns_payload_on_clean_response():
    """A response with no Note/Information key short-circuits the retry loop
    on the first attempt."""
    reset_av_call_count()
    sess = _ScriptedSession([{"Time Series (Daily)": {"2026-04-15": {}}}])
    rl = RateLimiter(calls_per_minute=1000, window=1.0, min_gap=0.0)

    out = _run(fetch_av_json("https://fake", sess, rl))

    assert out == {"Time Series (Daily)": {"2026-04-15": {}}}
    assert len(sess.requests) == 1
    assert get_av_call_count() == 1


def test_fetch_av_json_retries_on_throttle_then_succeeds():
    """A throttle response (``Note`` key) triggers a 60s sleep + retry. Patch
    ``asyncio.sleep`` to avoid the real wait while still verifying the call."""
    reset_av_call_count()
    sess = _ScriptedSession([
        {"Note": "Thank you for using Alpha Vantage..."},
        {"Time Series (Daily)": {"2026-04-15": {}}},
    ])
    rl = RateLimiter(calls_per_minute=1000, window=1.0, min_gap=0.0)

    sleeps: list[float] = []
    real_sleep = asyncio.sleep

    async def fast_sleep(seconds):
        sleeps.append(seconds)
        # Yield control without actually waiting -- a 0-duration real sleep
        # keeps the event loop happy.
        await real_sleep(0)

    with patch("historical_data_setup._common.asyncio.sleep", side_effect=fast_sleep):
        out = _run(fetch_av_json("https://fake", sess, rl))

    assert out == {"Time Series (Daily)": {"2026-04-15": {}}}
    assert len(sess.requests) == 2
    # The 60s throttle backoff must have been requested at least once.
    assert 60 in sleeps
    assert get_av_call_count() == 2


def test_fetch_av_json_raises_after_exhausting_retries():
    """3 throttles in a row -> AVResponseError. Default ``max_retries=3``."""
    reset_av_call_count()
    sess = _ScriptedSession([
        {"Note": "Throttled"},
        {"Information": "Throttled again"},
        {"Note": "Still throttled"},
    ])
    rl = RateLimiter(calls_per_minute=1000, window=1.0, min_gap=0.0)

    real_sleep = asyncio.sleep

    async def fast_sleep(seconds):
        await real_sleep(0)

    with patch("historical_data_setup._common.asyncio.sleep", side_effect=fast_sleep):
        with pytest.raises(AVResponseError, match="AV throttle"):
            _run(fetch_av_json("https://fake", sess, rl))

    assert len(sess.requests) == 3
    assert get_av_call_count() == 3


def test_fetch_av_json_information_key_also_treated_as_throttle():
    """AV signals throttle via either ``Note`` or ``Information``; the helper
    must not differentiate. Single retry, then succeed."""
    reset_av_call_count()
    sess = _ScriptedSession([
        {"Information": "API rate limit hit"},
        {"Time Series (Daily)": {}},
    ])
    rl = RateLimiter(calls_per_minute=1000, window=1.0, min_gap=0.0)

    real_sleep = asyncio.sleep

    async def fast_sleep(seconds):
        await real_sleep(0)

    with patch("historical_data_setup._common.asyncio.sleep", side_effect=fast_sleep):
        out = _run(fetch_av_json("https://fake", sess, rl))

    assert out == {"Time Series (Daily)": {}}
    assert len(sess.requests) == 2


# ---------------------------------------------------------------------------
# ensure_historical_folders
# ---------------------------------------------------------------------------


def test_ensure_historical_folders_creates_full_subtree(tmp_path):
    historical = tmp_path / "historical"
    out = ensure_historical_folders(historical)
    assert out == historical
    for leaf in HISTORICAL_TREE:
        assert (historical / leaf).is_dir(), f"missing {leaf}"


def test_ensure_historical_folders_idempotent_preserves_files(tmp_path):
    """Re-running must not wipe symbol parquets that are already on disk;
    the resume contract depends on it."""
    historical = tmp_path / "historical"
    ensure_historical_folders(historical)
    sentinel = historical / "stocks" / "prices_daily" / "stocks_AAPL.parquet"
    sentinel.write_bytes(b"keep me")

    ensure_historical_folders(historical)
    assert sentinel.read_bytes() == b"keep me"
