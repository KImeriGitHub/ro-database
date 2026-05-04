"""Unit tests for ``historical_data_setup.endpoints.sentiment``.

Covers the pure helpers (``_parse_time_published``, ``_ceil_to_minute``,
``_safe_float`` / ``_safe_str``, ``_parse_feed``) plus the paginating
``fetch_sentiment`` driver: backward pagination, dedup on (url, ticker),
catalog filtering, per-active-symbol split, and the safety stop when the
oldest article fails to advance ``time_to``.
"""

from __future__ import annotations

import asyncio
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from historical_data_setup._common import IssueTracker, RateLimiter
from historical_data_setup.endpoints import sentiment as ep
from historical_data_setup.endpoints.sentiment import TOPIC_COLUMNS


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def fast_limiter():
    return RateLimiter(calls_per_minute=10000.0, window=1.0, min_gap=0.0)


def _make_stocks_catalog(catalog_dir: Path, rows: list[dict]) -> None:
    """rows = list of {symbol, status} dicts."""
    catalog_dir.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame({
        "symbol": [r["symbol"] for r in rows],
        "name": [r["symbol"] for r in rows],
        "ipoDate": [None] * len(rows),
        "delistingDate": [None] * len(rows),
        "status": [r["status"] for r in rows],
    }).cast({"ipoDate": pl.Date, "delistingDate": pl.Date, "status": pl.Utf8})
    df.write_parquet(catalog_dir / "stocks.parquet", compression="zstd")


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_parse_time_published_round_trip():
    assert ep._parse_time_published("20260410T153926") == datetime(2026, 4, 10, 15, 39, 26)


def test_parse_time_published_invalid_returns_none():
    assert ep._parse_time_published("not-a-time") is None
    assert ep._parse_time_published("") is None
    assert ep._parse_time_published(None) is None  # type: ignore[arg-type]


def test_ceil_to_minute_truncates_seconds_then_adds_one_minute():
    """Truncate to minute, +1 minute. The +1 ensures the next backward query
    still covers the full minute of the boundary article."""
    out = ep._ceil_to_minute(datetime(2026, 4, 10, 15, 39, 47))
    assert out == "20260410T1540"
    # Boundary: exactly on a minute already -> still bumps by one.
    assert ep._ceil_to_minute(datetime(2026, 4, 10, 15, 39, 0)) == "20260410T1540"


def test_safe_float_handles_null_sentinels_and_bad_input():
    assert ep._safe_float("1.5") == 1.5
    for s in ("None", "", ".", None):
        assert ep._safe_float(s) is None
    assert ep._safe_float("not-numeric") is None


def test_safe_str_passthrough_with_null_sentinels():
    assert ep._safe_str("hello") == "hello"
    for s in ("None", "", ".", None):
        assert ep._safe_str(s) is None


def test_parse_feed_emits_one_row_per_ticker_with_topic_pivot():
    """A single article with two ticker_sentiments produces two rows. Topics
    listed in the article's ``topics`` array land in the matching column;
    unmentioned topics are None."""
    tracker = IssueTracker()
    feed = [{
        "time_published": "20260410T153926",
        "title": "headline",
        "url": "https://x/a",
        "authors": ["Alice", "Bob"],
        "summary": "summary",
        "banner_image": ".",
        "source": "Reuters",
        "category_within_source": "n/a",
        "source_domain": "reuters.com",
        "overall_sentiment_score": "0.21",
        "overall_sentiment_label": "Bullish",
        "topics": [
            {"topic": "earnings", "relevance_score": "0.9"},
            {"topic": "technology", "relevance_score": "0.6"},
        ],
        "ticker_sentiment": [
            {"ticker": "AAPL", "relevance_score": "0.8",
             "ticker_sentiment_score": "0.5", "ticker_sentiment_label": "Bullish"},
            {"ticker": "MSFT", "relevance_score": "0.4",
             "ticker_sentiment_score": "0.1", "ticker_sentiment_label": "Neutral"},
        ],
    }]
    rows = ep._parse_feed(feed, tracker)
    assert len(rows) == 2
    aapl = next(r for r in rows if r["ticker"] == "AAPL")
    assert aapl["earnings"] == pytest.approx(0.9)
    assert aapl["technology"] == pytest.approx(0.6)
    # Every TOPIC_COLUMNS entry is present (key set is fixed).
    for t in TOPIC_COLUMNS:
        assert t in aapl
    assert aapl["authors"] == "Alice;Bob"


def test_parse_feed_skips_article_with_unparseable_time():
    """Bad ``time_published`` -> cast_failure issue, article dropped."""
    tracker = IssueTracker()
    rows = ep._parse_feed(
        [{"time_published": "garbage", "ticker_sentiment": [{"ticker": "X"}]}],
        tracker,
    )
    assert rows == []
    assert any(r["issue_type"] == "cast_failure" for r in tracker._rows)


def test_parse_feed_skips_articles_without_ticker_sentiments():
    """Articles with empty ``ticker_sentiment`` produce no rows -- by design,
    we can't attribute the article to any catalog symbol."""
    tracker = IssueTracker()
    rows = ep._parse_feed(
        [{"time_published": "20260410T153926", "ticker_sentiment": []}],
        tracker,
    )
    assert rows == []
    assert tracker.count == 0


# ---------------------------------------------------------------------------
# fetch_sentiment end-to-end
# ---------------------------------------------------------------------------


def _article(time_published: str, tickers: list[str], url: str = None) -> dict:
    return {
        "time_published": time_published,
        "title": f"title-{time_published}",
        "url": url or f"https://news/{time_published}",
        "authors": [],
        "summary": "",
        "banner_image": "",
        "source": "",
        "category_within_source": "",
        "source_domain": "",
        "overall_sentiment_score": "0.1",
        "overall_sentiment_label": "Neutral",
        "topics": [],
        "ticker_sentiment": [
            {"ticker": t, "relevance_score": "0.5",
             "ticker_sentiment_score": "0.0", "ticker_sentiment_label": "Neutral"}
            for t in tickers
        ],
    }


def test_fetch_sentiment_paginates_dedupes_and_splits_per_active_symbol(
    tmp_path, fast_limiter,
):
    """Two backward queries; the second returns articles older than the
    first. After dedup on (url, ticker) and catalog filter, ALL_MESSAGES is
    written and per-active-symbol files split off. Delisted symbols are
    filtered into ALL_MESSAGES (catalog membership only) but get no per-
    symbol file."""
    catalog = tmp_path / "catalog"
    historical = tmp_path / "historical"
    _make_stocks_catalog(catalog, [
        {"symbol": "AAPL", "status": "Active"},
        {"symbol": "MSFT", "status": "Active"},
        {"symbol": "DEAD", "status": "Delisted"},
        # NOTLISTED is intentionally absent from the catalog.
    ])

    responses = [
        {
            "items": "2",
            "feed": [
                _article("20260410T1200", ["AAPL", "NOTLISTED"]),
                _article("20260410T1100", ["MSFT", "DEAD"]),
            ],
        },
        # Second call returns an empty feed -> pagination ends.
        {"items": "0", "feed": []},
    ]
    call_count = {"n": 0}

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        out = responses[call_count["n"]]
        call_count["n"] += 1
        return out

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_sentiment(
            catalog_dir=catalog, historical_dir=historical, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks",
        ))

    all_path = historical / "stocks" / "sentiment" / "ALL_MESSAGES.parquet"
    assert all_path.exists()
    all_df = pl.read_parquet(all_path)
    # NOTLISTED must be filtered out (not in catalog). DEAD stays.
    assert set(all_df["ticker"].to_list()) == {"AAPL", "MSFT", "DEAD"}

    # Per-active-symbol files: AAPL, MSFT only (DEAD is delisted).
    sym_dir = historical / "stocks" / "sentiment"
    assert (sym_dir / "stocks_AAPL.parquet").exists()
    assert (sym_dir / "stocks_MSFT.parquet").exists()
    assert not (sym_dir / "stocks_DEAD.parquet").exists()


def test_fetch_sentiment_skips_global_fetch_when_all_messages_exists(
    tmp_path, fast_limiter,
):
    """If ALL_MESSAGES already exists the global fetch is skipped entirely
    and only the per-symbol split runs (resume behaviour)."""
    catalog = tmp_path / "catalog"
    historical = tmp_path / "historical"
    _make_stocks_catalog(catalog, [{"symbol": "AAPL", "status": "Active"}])

    sent_dir = historical / "stocks" / "sentiment"
    sent_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "time_published": [datetime(2026, 4, 10, 12)],
            "ticker": ["AAPL"],
            "url": ["https://news/x"],
        },
        schema={
            "time_published": pl.Datetime,
            "ticker": pl.String,
            "url": pl.String,
        },
    ).write_parquet(sent_dir / "ALL_MESSAGES.parquet")

    fetch_calls: list[str] = []

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        fetch_calls.append(url)
        return {"items": "0", "feed": []}

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_sentiment(
            catalog_dir=catalog, historical_dir=historical, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks",
        ))

    assert fetch_calls == []
    assert (sent_dir / "stocks_AAPL.parquet").exists()


def test_fetch_sentiment_records_empty_when_no_rows_fetched(tmp_path, fast_limiter):
    """First call returns an empty feed -> no data, ``empty_content`` issue
    recorded against GLOBAL, no ALL_MESSAGES.parquet written."""
    catalog = tmp_path / "catalog"
    historical = tmp_path / "historical"
    _make_stocks_catalog(catalog, [{"symbol": "AAPL", "status": "Active"}])

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        return {"items": "0", "feed": []}

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_sentiment(
            catalog_dir=catalog, historical_dir=historical, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks",
        ))

    assert not (historical / "stocks" / "sentiment" / "ALL_MESSAGES.parquet").exists()


def test_fetch_sentiment_av_throttle_breaks_pagination(tmp_path, fast_limiter):
    """A throttle on the first call records ``av_throttle`` and aborts the
    paginator -- no parquet is written, but the loop does not raise."""
    catalog = tmp_path / "catalog"
    historical = tmp_path / "historical"
    _make_stocks_catalog(catalog, [{"symbol": "AAPL", "status": "Active"}])

    from historical_data_setup._common import AVResponseError

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        raise AVResponseError("paging died")

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_sentiment(
            catalog_dir=catalog, historical_dir=historical, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks",
        ))

    assert any(r["issue_type"] == "av_throttle" for r in tracker._rows)
    assert not (historical / "stocks" / "sentiment" / "ALL_MESSAGES.parquet").exists()


def test_fetch_sentiment_dedupes_on_url_ticker(tmp_path, fast_limiter):
    """Backward pagination overlap: an article reappears at the boundary; the
    final ALL_MESSAGES table has one row per (url, ticker)."""
    catalog = tmp_path / "catalog"
    historical = tmp_path / "historical"
    _make_stocks_catalog(catalog, [{"symbol": "AAPL", "status": "Active"}])

    duplicate = _article("20260410T1200", ["AAPL"], url="https://news/dup")
    responses = [
        {"items": "1", "feed": [duplicate]},
        {"items": "1", "feed": [duplicate]},  # same article -> dedup'd
        {"items": "0", "feed": []},
    ]
    idx = {"i": 0}

    async def fake_fetch(url, session, rate_limiter, max_retries=3):
        out = responses[idx["i"]] if idx["i"] < len(responses) else {"feed": []}
        idx["i"] += 1
        return out

    tracker = IssueTracker()
    with patch.object(ep, "fetch_av_json", side_effect=fake_fetch):
        _run(ep.fetch_sentiment(
            catalog_dir=catalog, historical_dir=historical, api_key="fake",
            session=None, rate_limiter=fast_limiter, issue_tracker=tracker,
            asset_type="stocks",
        ))

    df = pl.read_parquet(historical / "stocks" / "sentiment" / "ALL_MESSAGES.parquet")
    assert df.height == 1
