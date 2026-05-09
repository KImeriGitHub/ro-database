"""Tests for ``daily_data_service.adjust_weekly``.

Covers the pure helpers (folder-date resolution, retry-plan grouping,
sentiment file rename, ingestion report merge) plus four integration tests
that drive the full ``adjust_weekly`` orchestrator with every endpoint
coroutine replaced by a stub. The stubs never touch the network; the real
``aiohttp.ClientSession`` is created and immediately torn down.
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

from daily_data_service import adjust_weekly as aw
from historical_data_setup._common import IssueTracker


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def workdir(tmp_path: Path) -> Path:
    """Isolated workdir with empty ``catalog/`` and ``daily/`` subtrees."""
    (tmp_path / "catalog").mkdir()
    (tmp_path / "daily").mkdir()
    return tmp_path


def _make_date_dirs(daily_dir: Path, iso_names: list[str]) -> None:
    for n in iso_names:
        (daily_dir / n).mkdir()


_REPORT_COLS = ("symbol", "asset_type", "endpoint", "issue_type", "detail", "timestamp")


def _write_report(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    defaults = {
        "issue_type": "empty_content",
        "detail": "x",
        "timestamp": datetime(2026, 4, 18, 9, 0),
    }
    filled = [{**defaults, **r} for r in rows]
    if not filled:
        pl.DataFrame([], schema=aw._REPORT_SCHEMA).write_parquet(path, compression="zstd")
        return
    pl.DataFrame(filled, schema=aw._REPORT_SCHEMA).write_parquet(path, compression="zstd")


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# resolve_dates
# ---------------------------------------------------------------------------


def test_resolve_dates_full_week_picks_latest_and_prior_week_folder(workdir: Path):
    """With a full Mon-Sat set of folders and one prior-week folder, the
    weekend pass anchors on Saturday and previous_date hits the prior-week
    folder -- because cutoff = Sat - 6 = Sun, and Apr-11 is strictly < Apr-12."""
    daily = workdir / "daily"
    _make_date_dirs(daily, [
        "2026-04-11",  # prior-week Sat (should become previous_date)
        "2026-04-13", "2026-04-14", "2026-04-15",
        "2026-04-16", "2026-04-17", "2026-04-18",  # this week Sat (folder_date)
    ])

    folder_date, previous_date = aw.resolve_dates(daily, look_back_days=6)

    assert folder_date == date(2026, 4, 18)
    assert previous_date == date(2026, 4, 11)


def test_resolve_dates_falls_back_when_no_folder_before_cutoff(workdir: Path):
    """If every folder lies inside the look-back window, fall back to the
    arithmetic ``folder_date - (look_back_days + 1)`` rule."""
    daily = workdir / "daily"
    _make_date_dirs(daily, [
        "2026-04-15", "2026-04-16", "2026-04-17", "2026-04-18",
    ])

    folder_date, previous_date = aw.resolve_dates(daily, look_back_days=6)

    assert folder_date == date(2026, 4, 18)
    # No folder < cutoff (2026-04-12) -> arithmetic fallback: 18 - 7 = 11.
    assert previous_date == date(2026, 4, 11)


def test_resolve_dates_ignores_non_date_entries(workdir: Path):
    """Files, hidden markers, and non-ISO-named dirs are not folder-dates."""
    daily = workdir / "daily"
    _make_date_dirs(daily, ["2026-04-17", "2026-04-18"])
    (daily / ".setup_started_at").touch()
    (daily / "README.md").touch()
    (daily / "not-a-date").mkdir()
    (daily / "2026-13-01").mkdir()  # invalid month -- date.fromisoformat rejects

    folder_date, previous_date = aw.resolve_dates(daily, look_back_days=6)
    assert folder_date == date(2026, 4, 18)
    assert previous_date == date(2026, 4, 11)  # fallback (only 17 and 18 are dates)


def test_resolve_dates_raises_on_empty_daily_dir(workdir: Path):
    with pytest.raises(FileNotFoundError):
        aw.resolve_dates(workdir / "daily", look_back_days=6)


# ---------------------------------------------------------------------------
# _build_retry_plan
# ---------------------------------------------------------------------------


def _write_catalog(catalog_dir: Path, asset_type: str, symbols: list[str]) -> None:
    """Minimal catalog parquet sufficient to drive ``_load_symbol_asset_type_map``."""
    catalog_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": symbols}).write_parquet(
        catalog_dir / f"{asset_type}.parquet", compression="zstd",
    )


def _write_yield_status(
    catalog_dir: Path, rows: list[dict], cols: list[str],
) -> None:
    """Write a yield_status.parquet from rows like
    ``{"symbol": "AAPL", "prices": False, ...}``. Missing values default to None.
    """
    catalog_dir.mkdir(parents=True, exist_ok=True)
    schema: dict = {"symbol": pl.Utf8}
    for c in cols:
        schema[c] = pl.Boolean
    schema["date"] = pl.Date

    data: dict = {"symbol": [r["symbol"] for r in rows]}
    for c in cols:
        data[c] = [r.get(c) for r in rows]
    data["date"] = [date(2026, 4, 18)] * len(rows)

    pl.DataFrame(data, schema=schema).write_parquet(
        catalog_dir / "yield_status.parquet", compression="zstd",
    )


def test_build_retry_plan_unions_reports_and_yield_status(workdir: Path):
    """Plan = union of (in-window ingestion reports) and (yield_status False
    cells). Out-of-window reports are ignored."""
    daily = workdir / "daily"
    catalog = workdir / "catalog"

    _write_report(daily / "2026-04-15" / "ingestion_report.parquet", [
        {"symbol": "AAPL", "asset_type": "stocks", "endpoint": "prices_daily"},
    ])
    _write_report(daily / "2026-04-18" / "ingestion_report.parquet", [
        {"symbol": "MSFT", "asset_type": "stocks", "endpoint": "income_statement"},
    ])
    # Out-of-window (== previous_date) report must be ignored.
    _write_report(daily / "2026-04-11" / "ingestion_report.parquet", [
        {"symbol": "NVDA", "asset_type": "stocks", "endpoint": "prices_daily"},
    ])

    _write_catalog(catalog, "stocks", ["AAPL", "MSFT", "GOOG"])
    _write_yield_status(catalog, [
        {"symbol": "AAPL", "insider": False, "prices": True},
        {"symbol": "GOOG", "prices": False},
    ], cols=["prices", "insider"])

    plan, fd_report, full_rerun = aw._build_retry_plan(
        catalog, daily,
        previous_date=date(2026, 4, 11), folder_date=date(2026, 4, 18),
    )

    assert plan == {
        ("stocks", "prices_daily"):     {"AAPL"},
        ("stocks", "income_statement"): {"MSFT"},
        ("stocks", "insider"):          {"AAPL"},
        ("stocks", "prices"):           {"GOOG"},
    }
    assert full_rerun is False
    assert fd_report.height == 1  # only the folder_date report


def test_build_retry_plan_global_sentiment_row_sets_full_rerun(workdir: Path):
    """A GLOBAL row in any in-window report triggers ``sentiment_full_rerun``;
    sentiment never appears as a per-symbol plan entry."""
    daily = workdir / "daily"
    catalog = workdir / "catalog"
    _write_report(daily / "2026-04-15" / "ingestion_report.parquet", [
        {"symbol": "GLOBAL", "asset_type": "stocks", "endpoint": "sentiment"},
    ])

    plan, _fd, full_rerun = aw._build_retry_plan(
        catalog, daily,
        previous_date=date(2026, 4, 11), folder_date=date(2026, 4, 18),
    )

    assert full_rerun is True
    assert ("stocks", "sentiment") not in plan


def test_build_retry_plan_per_symbol_sentiment_row_is_dropped(workdir: Path):
    """Non-GLOBAL sentiment rows in reports are silently dropped: sentiment
    is global, per-symbol fetches don't exist."""
    daily = workdir / "daily"
    catalog = workdir / "catalog"
    _write_report(daily / "2026-04-18" / "ingestion_report.parquet", [
        {"symbol": "NVDA", "asset_type": "stocks", "endpoint": "sentiment"},
    ])

    plan, _fd, full_rerun = aw._build_retry_plan(
        catalog, daily,
        previous_date=date(2026, 4, 11), folder_date=date(2026, 4, 18),
    )

    assert plan == {}
    assert full_rerun is False


def test_build_retry_plan_yield_status_sentiment_column_excluded(workdir: Path):
    """yield_status ``sentiment`` False cells are NOT a trigger source --
    the global rerun is gated only on GLOBAL ingestion-report rows."""
    daily = workdir / "daily"
    catalog = workdir / "catalog"
    _write_catalog(catalog, "stocks", ["AAPL"])
    _write_yield_status(catalog, [
        {"symbol": "AAPL", "sentiment": False},
    ], cols=["sentiment"])

    plan, _fd, full_rerun = aw._build_retry_plan(
        catalog, daily,
        previous_date=date(2026, 4, 11), folder_date=date(2026, 4, 18),
    )

    assert plan == {}
    assert full_rerun is False


def test_build_retry_plan_direct_column_maps_to_asset_type_endpoint(workdir: Path):
    """yield_status ``direct=False`` on a forex/indices symbol becomes a
    plan entry whose endpoint name is the symbol's asset_type."""
    daily = workdir / "daily"
    catalog = workdir / "catalog"
    _write_catalog(catalog, "forex", ["EURUSD"])
    _write_catalog(catalog, "indices", ["DJI"])
    _write_yield_status(catalog, [
        {"symbol": "EURUSD", "direct": False},
        {"symbol": "DJI",    "direct": False},
    ], cols=["direct"])

    plan, _fd, _full = aw._build_retry_plan(
        catalog, daily,
        previous_date=date(2026, 4, 11), folder_date=date(2026, 4, 18),
    )

    assert plan == {
        ("forex",   "forex"):   {"EURUSD"},
        ("indices", "indices"): {"DJI"},
    }


def test_build_retry_plan_missing_inputs_returns_empty(workdir: Path):
    """No yield_status and no in-window reports -> empty plan, no rerun."""
    plan, fd, full_rerun = aw._build_retry_plan(
        workdir / "catalog", workdir / "daily",
        previous_date=date(2026, 4, 11), folder_date=date(2026, 4, 18),
    )
    assert plan == {}
    assert fd.height == 0
    assert full_rerun is False


# ---------------------------------------------------------------------------
# _rename_sentiment_files
# ---------------------------------------------------------------------------


def test_rename_sentiment_files_renames_parquet_only(workdir: Path):
    sent_dir = workdir / "daily" / "2026-04-18" / "stocks" / "sentiment"
    sent_dir.mkdir(parents=True)
    (sent_dir / "ALL_MESSAGES.parquet").write_bytes(b"A")
    (sent_dir / "AAPL.parquet").write_bytes(b"B")
    (sent_dir / "MSFT.parquet").write_bytes(b"C")
    # Non-parquet files must be left untouched.
    (sent_dir / "README.txt").write_bytes(b"keep")
    (sent_dir / "stale.pre_weekly").write_bytes(b"keep-too")

    renamed = aw._rename_sentiment_files(sent_dir)

    assert renamed == 3
    assert (sent_dir / "ALL_MESSAGES.parquet.pre_weekly").exists()
    assert (sent_dir / "AAPL.parquet.pre_weekly").exists()
    assert (sent_dir / "MSFT.parquet.pre_weekly").exists()
    # Originals gone, non-parquet preserved.
    assert not (sent_dir / "ALL_MESSAGES.parquet").exists()
    assert (sent_dir / "README.txt").read_bytes() == b"keep"
    assert (sent_dir / "stale.pre_weekly").read_bytes() == b"keep-too"


def test_rename_sentiment_files_overwrites_prior_pre_weekly(workdir: Path):
    """A second weekend pass must not trip over a prior ``.pre_weekly``
    sibling -- the old one is replaced by the current week's state."""
    sent_dir = workdir / "daily" / "2026-04-18" / "stocks" / "sentiment"
    sent_dir.mkdir(parents=True)
    (sent_dir / "AAPL.parquet").write_bytes(b"new")
    (sent_dir / "AAPL.parquet.pre_weekly").write_bytes(b"old")

    renamed = aw._rename_sentiment_files(sent_dir)

    assert renamed == 1
    assert (sent_dir / "AAPL.parquet.pre_weekly").read_bytes() == b"new"
    assert not (sent_dir / "AAPL.parquet").exists()


def test_rename_sentiment_files_missing_dir_is_noop(workdir: Path):
    assert aw._rename_sentiment_files(workdir / "does" / "not" / "exist") == 0


# ---------------------------------------------------------------------------
# _merge_report
# ---------------------------------------------------------------------------


def _build_report(rows: list[dict]) -> pl.DataFrame:
    defaults = {
        "issue_type": "empty_content",
        "detail": "x",
        "timestamp": datetime(2026, 4, 18, 9, 0),
    }
    return pl.DataFrame([{**defaults, **r} for r in rows], schema=aw._REPORT_SCHEMA)


def _tracker_with(rows: list[dict]) -> IssueTracker:
    t = IssueTracker()
    for r in rows:
        t.record(
            r["symbol"], r["asset_type"], r["endpoint"],
            r.get("issue_type", "empty_content"),
            r.get("detail", "fresh"),
        )
    return t


def test_merge_report_drops_retried_triples_and_appends_fresh():
    """Only the (symbol, asset_type, endpoint) triples explicitly listed as
    retried should be removed; other rows (including rows for the same
    symbol under a different endpoint) must survive."""
    old = _build_report([
        {"symbol": "AAPL", "asset_type": "stocks", "endpoint": "prices_daily",    "detail": "old-1"},
        {"symbol": "AAPL", "asset_type": "stocks", "endpoint": "income_statement", "detail": "old-2"},
        {"symbol": "MSFT", "asset_type": "stocks", "endpoint": "prices_daily",    "detail": "old-3"},
    ])
    retried = {
        ("AAPL", "stocks", "prices_daily"),  # succeeded on retry -> row dropped, no fresh
        ("MSFT", "stocks", "prices_daily"),  # retried, still fails -> dropped and re-added
    }
    fresh = _tracker_with([
        {"symbol": "MSFT", "asset_type": "stocks", "endpoint": "prices_daily",
         "issue_type": "av_throttle", "detail": "still-throttled"},
    ])

    merged = aw._merge_report(old, retried, fresh)

    result = sorted(
        (r["symbol"], r["endpoint"], r["detail"])
        for r in merged.iter_rows(named=True)
    )
    assert result == [
        ("AAPL", "income_statement", "old-2"),        # untouched (different endpoint)
        ("MSFT", "prices_daily",     "still-throttled"),  # replaced by fresh failure
    ]


def test_merge_report_empty_old_returns_fresh_only():
    old = _build_report([])
    fresh = _tracker_with([
        {"symbol": "X", "asset_type": "stocks", "endpoint": "prices_daily"},
    ])
    merged = aw._merge_report(old, set(), fresh)
    assert merged.height == 1
    assert merged["symbol"].to_list() == ["X"]


def test_merge_report_no_retries_and_no_fresh_returns_old_unchanged():
    old = _build_report([
        {"symbol": "AAPL", "asset_type": "stocks", "endpoint": "prices_daily"},
    ])
    merged = aw._merge_report(old, set(), IssueTracker())
    assert merged.height == 1
    assert merged["symbol"].to_list() == ["AAPL"]


# ---------------------------------------------------------------------------
# adjust_weekly (integration, stubbed endpoints)
# ---------------------------------------------------------------------------


def _make_endpoint_stub(
    calls: list[dict],
    on_call=None,  # callable(kwargs) -> Optional[list[issue_dict]]
):
    """Returns a function matching ENDPOINT_MAP's signature that records its
    kwargs and optionally writes fresh issues via the passed-in tracker.

    ``on_call`` receives the full kwargs and may return a list of dicts like
    ``{"symbol": ..., "endpoint": ..., "issue_type": ..., "detail": ...}``;
    each is written to ``issue_tracker`` under the call's ``asset_type``.
    """

    def _stub(**kwargs):
        calls.append({
            "asset_type": kwargs["asset_type"],
            "folder_date": kwargs["folder_date"],
            "previous_date": kwargs["previous_date"],
            "symbols_filter": kwargs.get("symbols_filter"),
            "skip_empty_yield": kwargs.get("skip_empty_yield"),
        })
        tracker: IssueTracker = kwargs["issue_tracker"]
        issues = on_call(kwargs) if on_call else None

        async def _go():
            if issues:
                for i in issues:
                    tracker.record(
                        i["symbol"], kwargs["asset_type"], i["endpoint"],
                        i["issue_type"], i["detail"],
                    )
        return _go()

    return _stub


def test_adjust_weekly_retries_reported_cells_and_merges_fresh_issues(workdir: Path):
    """End-to-end: exactly the (symbol, asset_type, endpoint) triples in the
    ingestion report are retried; the stub drops AAPL and re-fails MSFT;
    the merged report reflects both outcomes; ``finalize_yield_status`` is
    invoked with the folder-date root."""
    daily = workdir / "daily"
    _make_date_dirs(daily, ["2026-04-11", "2026-04-18"])
    day_root = daily / "2026-04-18"
    _write_report(day_root / "ingestion_report.parquet", [
        # retry success -> dropped, no fresh row
        {"symbol": "AAPL", "asset_type": "stocks", "endpoint": "prices_daily"},
        # retry fails -> dropped and re-added with fresh detail
        {"symbol": "MSFT", "asset_type": "stocks", "endpoint": "prices_daily",
         "issue_type": "av_throttle", "detail": "weekday throttle"},
    ])

    calls: list[dict] = []
    stub = _make_endpoint_stub(
        calls,
        on_call=lambda kw: [{
            "symbol": "MSFT", "endpoint": "prices_daily",
            "issue_type": "av_throttle", "detail": "weekend throttle",
        }] if "MSFT" in (kw["symbols_filter"] or set()) else [],
    )

    finalize_calls: list[tuple] = []

    def fake_finalize(catalog_dir, day_root_arg, started_at):
        finalize_calls.append((catalog_dir, day_root_arg, started_at))

    with patch.object(aw, "ENDPOINT_MAP", {"prices_daily": stub}), \
         patch.object(aw, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(aw, "fetch_earnings_calendar"), \
         patch.object(aw, "finalize_yield_status", side_effect=fake_finalize):
        _run(aw.adjust_weekly(
            catalog_dir=workdir / "catalog",
            daily_dir=daily,
            look_back_days=6,
        ))

    # One dispatch covering both symbols with the wider previous_date.
    assert len(calls) == 1
    call = calls[0]
    assert call["asset_type"] == "stocks"
    assert call["symbols_filter"] == {"AAPL", "MSFT"}
    assert call["folder_date"] == date(2026, 4, 18)
    assert call["previous_date"] == date(2026, 4, 11)

    report = pl.read_parquet(day_root / "ingestion_report.parquet").sort("symbol")
    result = list(zip(
        report["symbol"].to_list(),
        report["endpoint"].to_list(),
        report["issue_type"].to_list(),
        report["detail"].to_list(),
    ))
    assert result == [
        ("MSFT", "prices_daily", "av_throttle", "weekend throttle"),
    ]

    assert len(finalize_calls) == 1
    assert finalize_calls[0][1] == day_root


def test_adjust_weekly_preserves_rows_for_endpoints_that_never_dispatch(workdir: Path):
    """Rows for endpoints not in ``ENDPOINT_MAP`` (or not applicable to the
    asset_type) must survive the merge. Only tasks that actually ran should
    cause rows to be dropped."""
    daily = workdir / "daily"
    _make_date_dirs(daily, ["2026-04-18"])
    day_root = daily / "2026-04-18"
    _write_report(day_root / "ingestion_report.parquet", [
        {"symbol": "AAPL", "asset_type": "stocks", "endpoint": "prices_daily"},
        # income_statement will be in the retry plan but NOT in our patched
        # ENDPOINT_MAP -> the task is skipped with a warning, so XYZ's row
        # must NOT be dropped.
        {"symbol": "XYZ",  "asset_type": "stocks", "endpoint": "income_statement",
         "issue_type": "structure_error", "detail": "keep-me"},
    ])

    calls: list[dict] = []
    stub = _make_endpoint_stub(calls)  # no fresh issues -> AAPL retry succeeds

    with patch.object(aw, "ENDPOINT_MAP", {"prices_daily": stub}), \
         patch.object(aw, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(aw, "fetch_earnings_calendar"), \
         patch.object(aw, "finalize_yield_status"):
        _run(aw.adjust_weekly(
            catalog_dir=workdir / "catalog",
            daily_dir=daily,
            look_back_days=6,
        ))

    assert len(calls) == 1  # only prices_daily dispatched
    report = pl.read_parquet(day_root / "ingestion_report.parquet")
    result = sorted(
        (r["symbol"], r["endpoint"], r["detail"])
        for r in report.iter_rows(named=True)
    )
    assert result == [("XYZ", "income_statement", "keep-me")]


def test_adjust_weekly_sentiment_triggers_rename_and_full_rerun(workdir: Path):
    """A GLOBAL (or any) sentiment entry must rename every sentiment parquet
    in the folder-date tree and schedule sentiment with ``symbols_filter=None``
    so the global paginated fetch covers every catalog ticker."""
    daily = workdir / "daily"
    _make_date_dirs(daily, ["2026-04-11", "2026-04-18"])
    day_root = daily / "2026-04-18"
    sent_dir = day_root / "stocks" / "sentiment"
    sent_dir.mkdir(parents=True)
    (sent_dir / "ALL_MESSAGES.parquet").write_bytes(b"old-global")
    (sent_dir / "AAPL.parquet").write_bytes(b"old-aapl")
    (sent_dir / "NVDA.parquet").write_bytes(b"old-nvda")

    _write_report(day_root / "ingestion_report.parquet", [
        {"symbol": "GLOBAL", "asset_type": "stocks", "endpoint": "sentiment",
         "issue_type": "av_throttle", "detail": "paging died"},
        {"symbol": "NVDA",   "asset_type": "stocks", "endpoint": "sentiment",
         "issue_type": "empty_content", "detail": "no rows"},
    ])

    calls: list[dict] = []
    stub = _make_endpoint_stub(calls)  # clean rerun

    with patch.object(aw, "ENDPOINT_MAP", {"sentiment": stub}), \
         patch.object(aw, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(aw, "fetch_earnings_calendar"), \
         patch.object(aw, "finalize_yield_status") as finalize_mock:
        _run(aw.adjust_weekly(
            catalog_dir=workdir / "catalog",
            daily_dir=daily,
            look_back_days=6,
        ))

    # Rename happened before the rerun.
    assert (sent_dir / "ALL_MESSAGES.parquet.pre_weekly").read_bytes() == b"old-global"
    assert (sent_dir / "AAPL.parquet.pre_weekly").read_bytes() == b"old-aapl"
    assert (sent_dir / "NVDA.parquet.pre_weekly").read_bytes() == b"old-nvda"
    assert not (sent_dir / "ALL_MESSAGES.parquet").exists()

    # Sentiment dispatched once with no symbol filter (the global fetch
    # covers every ticker regardless).
    assert len(calls) == 1
    assert calls[0]["symbols_filter"] is None
    assert calls[0]["asset_type"] == "stocks"

    # All sentiment rows were dropped from the merged report -- including
    # GLOBAL, which doesn't map to any catalog symbol but was still listed
    # in the plan.
    report = pl.read_parquet(day_root / "ingestion_report.parquet")
    assert report.filter(pl.col("endpoint") == "sentiment").height == 0

    finalize_mock.assert_called_once()


def test_adjust_weekly_fundamentals_retry_with_skip_empty_yield_false(workdir: Path):
    """Weekend retries must re-query fundamentals flagged False on weekday
    ``skip_empty_yield`` runs -- the stub receives ``skip_empty_yield=False``
    explicitly so ``read_yield_skip_set`` is not consulted on this pass."""
    daily = workdir / "daily"
    _make_date_dirs(daily, ["2026-04-18"])
    day_root = daily / "2026-04-18"
    _write_report(day_root / "ingestion_report.parquet", [
        {"symbol": "AAPL", "asset_type": "stocks", "endpoint": "income_statement",
         "issue_type": "empty_content",
         "detail": "skipped: yield_status False, revalidate on weekend"},
    ])

    calls: list[dict] = []
    stub = _make_endpoint_stub(calls)

    with patch.object(aw, "ENDPOINT_MAP", {"income_statement": stub}), \
         patch.object(aw, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(aw, "fetch_earnings_calendar"), \
         patch.object(aw, "finalize_yield_status"):
        _run(aw.adjust_weekly(
            catalog_dir=workdir / "catalog",
            daily_dir=daily,
            look_back_days=6,
        ))

    assert len(calls) == 1
    assert calls[0]["symbols_filter"] == {"AAPL"}
    # Orchestrator forces skip_empty_yield=False for YIELD_SKIP_ENDPOINTS.
    assert calls[0]["skip_empty_yield"] is False


def test_adjust_weekly_no_report_is_noop(workdir: Path):
    """Missing or empty ingestion report: no dispatch, no finalize."""
    daily = workdir / "daily"
    _make_date_dirs(daily, ["2026-04-18"])
    # No ingestion_report.parquet in the folder at all.

    calls: list[dict] = []
    stub = _make_endpoint_stub(calls)

    with patch.object(aw, "ENDPOINT_MAP", {"prices_daily": stub}), \
         patch.object(aw, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(aw, "fetch_earnings_calendar"), \
         patch.object(aw, "finalize_yield_status") as finalize_mock:
        _run(aw.adjust_weekly(
            catalog_dir=workdir / "catalog",
            daily_dir=daily,
            look_back_days=6,
        ))

    assert calls == []
    finalize_mock.assert_not_called()


# ---------------------------------------------------------------------------
# earnings_calendar refresh
# ---------------------------------------------------------------------------


def test_adjust_weekly_fetches_earnings_calendar_when_missing(workdir: Path):
    """When ``daily/<folder_date>/earnings_calendar.parquet`` is absent the
    weekend pass triggers ``fetch_earnings_calendar`` for the folder."""
    daily = workdir / "daily"
    _make_date_dirs(daily, ["2026-04-18"])
    day_root = daily / "2026-04-18"
    assert not (day_root / "earnings_calendar.parquet").exists()

    with patch.object(aw, "ENDPOINT_MAP", {}), \
         patch.object(aw, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(aw, "fetch_earnings_calendar") as ec_mock, \
         patch.object(aw, "finalize_yield_status"):
        _run(aw.adjust_weekly(
            catalog_dir=workdir / "catalog",
            daily_dir=daily,
            look_back_days=6,
        ))

    ec_mock.assert_called_once_with("fake-key", day_root)


def test_adjust_weekly_skips_earnings_calendar_when_present(workdir: Path):
    """An existing ``earnings_calendar.parquet`` in the folder-date dir
    blocks the weekend refresh -- the daily run already produced one."""
    daily = workdir / "daily"
    _make_date_dirs(daily, ["2026-04-18"])
    day_root = daily / "2026-04-18"
    pl.DataFrame({"symbol": ["AAPL"]}).write_parquet(
        day_root / "earnings_calendar.parquet"
    )

    with patch.object(aw, "ENDPOINT_MAP", {}), \
         patch.object(aw, "get_alpha_vantage_key", return_value="fake-key"), \
         patch.object(aw, "fetch_earnings_calendar") as ec_mock, \
         patch.object(aw, "finalize_yield_status"):
        _run(aw.adjust_weekly(
            catalog_dir=workdir / "catalog",
            daily_dir=daily,
            look_back_days=6,
        ))

    ec_mock.assert_not_called()
