"""Tests for Phase 6b: sentiment_df for stocks.

Covers the time_published rename, concat across historical + multiple
daily folders, (Datetime, url) dedup with same-minute distinct-url
preservation, discrepancy logging across the 18 Float32 score columns,
the defensive ticker filter, source files without a ticker column,
missing topic columns, schema exactness (drop of all string source
columns), and the source-enumeration skip of ALL_MESSAGES.parquet.
"""

from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from data_transformation._common import (
    TransformationReport,
    build_source_index,
)
from data_transformation.AssetDataService import SCHEMAS
from data_transformation.frames.sentiment import (
    _SENTIMENT_FLOAT_COLS,
    build_sentiment_df,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

_TOPIC_COLS = (
    "blockchain", "earnings", "ipo", "mergers_and_acquisitions",
    "financial_markets", "economy_fiscal", "economy_monetary",
    "economy_macro", "energy_transportation", "finance",
    "life_sciences", "manufacturing", "real_estate",
    "retail_wholesale", "technology",
)

# Full source schema mirroring NEWS_SENTIMENT response.
_SENT_SOURCE_SCHEMA: dict = {
    "time_published": pl.Datetime("us"),
    "ticker": pl.Utf8,
    "ticker_relevance_score": pl.Float32,
    "ticker_sentiment_score": pl.Float32,
    "ticker_sentiment_label": pl.Utf8,
    "title": pl.Utf8,
    "url": pl.Utf8,
    "authors": pl.Utf8,
    "summary": pl.Utf8,
    "banner_image": pl.Utf8,
    "source": pl.Utf8,
    "category_within_source": pl.Utf8,
    "source_domain": pl.Utf8,
    "overall_sentiment_score": pl.Float32,
    "overall_sentiment_label": pl.Utf8,
    **{t: pl.Float32 for t in _TOPIC_COLS},
}


def _row(
    dt: datetime,
    *,
    ticker: str = "AAPL",
    url: str = "https://example.com/a",
    rel: float = 0.5,
    sent: float = 0.1,
    overall: float = 0.05,
    technology: float = 0.7,
) -> dict:
    base = {
        "time_published": dt,
        "ticker": ticker,
        "ticker_relevance_score": rel,
        "ticker_sentiment_score": sent,
        "ticker_sentiment_label": "Neutral",
        "title": "An article",
        "url": url,
        "authors": "Some Author",
        "summary": "A summary.",
        "banner_image": None,
        "source": "Reuters",
        "category_within_source": "n/a",
        "source_domain": "reuters.com",
        "overall_sentiment_score": overall,
        "overall_sentiment_label": "Neutral",
    }
    for t in _TOPIC_COLS:
        base[t] = 0.0
    base["technology"] = technology
    return base


def _write_sentiment(
    path: Path, rows: list[dict], schema: dict | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, schema=schema or _SENT_SOURCE_SCHEMA).write_parquet(path)


# ── 1. Empty inputs ───────────────────────────────────────────────────────────

def test_empty_paths_returns_schema_correct_empty():
    out = build_sentiment_df("AAPL", [], TransformationReport())
    assert out.height == 0
    assert dict(out.schema) == SCHEMAS["sentiment_df"]


# ── 2. time_published rename ──────────────────────────────────────────────────

def test_time_published_renamed_to_datetime(tmp_path):
    p = tmp_path / "p.parquet"
    _write_sentiment(p, [_row(datetime(2026, 4, 15, 9, 30))])
    out = build_sentiment_df("AAPL", [p], TransformationReport())
    assert "Datetime" in out.columns
    assert "time_published" not in out.columns
    assert out["Datetime"][0] == datetime(2026, 4, 15, 9, 30)


# ── 3. Concat of historical + multiple daily folders ──────────────────────────

def test_concat_historical_plus_multiple_daily_sorted(tmp_path):
    h = tmp_path / "h.parquet"
    d1 = tmp_path / "d1.parquet"
    d2 = tmp_path / "d2.parquet"
    _write_sentiment(h,  [_row(datetime(2026, 4, 14, 10), url="u1")])
    _write_sentiment(d1, [_row(datetime(2026, 4, 15, 11), url="u2")])
    _write_sentiment(d2, [_row(datetime(2026, 4, 16, 12), url="u3")])
    out = build_sentiment_df("AAPL", [h, d1, d2], TransformationReport())
    assert out.height == 3
    dts = out["Datetime"].to_list()
    assert dts == sorted(dts)


# ── 4. (Datetime, url) dedup ──────────────────────────────────────────────────

def test_same_minute_different_urls_kept_as_two_rows(tmp_path):
    """Two articles at the same Datetime with different urls survive as
    two distinct rows."""
    p = tmp_path / "p.parquet"
    same_dt = datetime(2026, 4, 15, 9, 30)
    _write_sentiment(p, [
        _row(same_dt, url="https://a.example.com/article-1"),
        _row(same_dt, url="https://b.example.com/article-2"),
    ])
    out = build_sentiment_df("AAPL", [p], TransformationReport())
    assert out.height == 2


def test_same_datetime_url_collapses_with_recent_winning(tmp_path):
    """Repeated (Datetime, url) across sources collapses to one; daily
    snapshot wins on field values."""
    h = tmp_path / "h.parquet"
    d = tmp_path / "d.parquet"
    dt = datetime(2026, 4, 15, 9, 30)
    _write_sentiment(h, [_row(dt, url="u-shared", overall=0.10)])
    _write_sentiment(d, [_row(dt, url="u-shared", overall=0.50)])
    out = build_sentiment_df("AAPL", [h, d], TransformationReport())
    assert out.height == 1
    assert pytest.approx(0.50, rel=1e-4) == out["overall_sentiment_score"][0]


# ── 5. Discrepancy logging across score columns ───────────────────────────────

def test_discrepancy_logging_under_and_over_1pct(tmp_path):
    """Two sources collide on (Datetime, url): ticker_sentiment_score
    differs by <1% (under) and technology differs by >=1% (over).
    Per the dedup helper's per-key max-relative classification, the
    larger discrepancy wins -> a single over_1pct log row."""
    h = tmp_path / "h.parquet"
    d = tmp_path / "d.parquet"
    dt = datetime(2026, 4, 15, 9, 30)
    _write_sentiment(h, [_row(dt, url="u", sent=0.100, technology=0.50)])
    _write_sentiment(d, [_row(dt, url="u", sent=0.1005, technology=0.80)])
    report = TransformationReport()
    build_sentiment_df("AAPL", [h, d], report)
    rep = report.to_frame()
    issues = set(rep["issue_type"].to_list())
    assert "dedup_value_discrepancy_over_1pct" in issues


def test_discrepancy_under_1pct_when_only_small_diffs(tmp_path):
    """Two sources collide on (Datetime, url) with all numeric differences
    strictly under 1% -> exactly one under_1pct log row."""
    h = tmp_path / "h.parquet"
    d = tmp_path / "d.parquet"
    dt = datetime(2026, 4, 15, 9, 30)
    _write_sentiment(h, [_row(dt, url="u", sent=0.100,  technology=0.700)])
    _write_sentiment(d, [_row(dt, url="u", sent=0.1003, technology=0.7035)])
    report = TransformationReport()
    build_sentiment_df("AAPL", [h, d], report)
    rep = report.to_frame()
    assert rep.filter(
        pl.col("issue_type") == "dedup_value_discrepancy_under_1pct"
    ).height == 1
    assert rep.filter(
        pl.col("issue_type") == "dedup_value_discrepancy_over_1pct"
    ).height == 0


# ── 6. Defensive ticker filter ────────────────────────────────────────────────

def test_ticker_filter_drops_unrelated_symbol(tmp_path):
    p = tmp_path / "p.parquet"
    _write_sentiment(p, [
        _row(datetime(2026, 4, 15, 9, 30), ticker="AAPL", url="u1"),
        _row(datetime(2026, 4, 15, 9, 31), ticker="MSFT", url="u2"),
    ])
    out = build_sentiment_df("AAPL", [p], TransformationReport())
    assert out.height == 1
    assert out["Datetime"][0] == datetime(2026, 4, 15, 9, 30)


# ── 7. Source without ticker column ───────────────────────────────────────────

def test_source_without_ticker_column_accepted(tmp_path):
    """The source omits the ticker column entirely. The builder trusts
    the per-symbol filename and keeps every row."""
    schema_no_ticker = {
        k: v for k, v in _SENT_SOURCE_SCHEMA.items() if k != "ticker"
    }
    p = tmp_path / "p.parquet"
    rows = [_row(datetime(2026, 4, 15, 9, 30), url="u1"),
            _row(datetime(2026, 4, 15, 9, 31), url="u2")]
    for r in rows:
        r.pop("ticker")
    _write_sentiment(p, rows, schema=schema_no_ticker)
    out = build_sentiment_df("AAPL", [p], TransformationReport())
    assert out.height == 2


# ── 8. Missing topic columns filled with null ─────────────────────────────────

def test_missing_topic_columns_filled_with_null(tmp_path):
    """A source missing 'blockchain' and 'ipo' columns is accepted; those
    columns appear as null Float32 in the output."""
    sparse_schema = {
        k: v for k, v in _SENT_SOURCE_SCHEMA.items()
        if k not in ("blockchain", "ipo")
    }
    p = tmp_path / "p.parquet"
    base = _row(datetime(2026, 4, 15, 9, 30), url="u1")
    base.pop("blockchain")
    base.pop("ipo")
    _write_sentiment(p, [base], schema=sparse_schema)
    out = build_sentiment_df("AAPL", [p], TransformationReport())
    assert out.height == 1
    assert out["blockchain"][0] is None
    assert out["ipo"][0] is None


# ── 9. Output schema exact (string source columns dropped) ────────────────────

def test_output_schema_exact_strings_dropped(tmp_path):
    p = tmp_path / "p.parquet"
    _write_sentiment(p, [_row(datetime(2026, 4, 15, 9, 30), url="u")])
    out = build_sentiment_df("AAPL", [p], TransformationReport())
    assert dict(out.schema) == SCHEMAS["sentiment_df"]
    expected_dropped = {
        "url", "title", "summary", "authors", "banner_image", "source",
        "category_within_source", "source_domain", "ticker_sentiment_label",
        "overall_sentiment_label", "ticker", "time_published",
    }
    assert expected_dropped.isdisjoint(set(out.columns))
    # All 18 Float32 score columns are present.
    for col in _SENTIMENT_FLOAT_COLS:
        assert col in out.columns


# ── 10. ALL_MESSAGES.parquet ignored by source enumeration ────────────────────

def test_all_messages_parquet_skipped_by_source_enumeration(tmp_path):
    """build_source_index requires the per-asset-type prefix
    ('stocks_'); ALL_MESSAGES.parquet does not start with the prefix and
    is therefore not picked up."""
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    sent_dir = historical / "stocks" / "sentiment"
    sent_dir.mkdir(parents=True, exist_ok=True)
    _write_sentiment(
        sent_dir / "stocks_AAPL.parquet",
        [_row(datetime(2026, 4, 15, 9, 30), url="u")],
    )
    # Decoy with no asset-type prefix.
    _write_sentiment(
        sent_dir / "ALL_MESSAGES.parquet",
        [_row(datetime(2026, 4, 15, 9, 30), url="u-decoy")],
    )

    idx = build_source_index(historical, daily, "stocks", "sentiment")
    assert "AAPL" in idx
    # No spurious symbol from ALL_MESSAGES.
    assert "ALL_MESSAGES" not in idx
    assert "MESSAGES" not in idx


# ── 11. Empty source rows ─────────────────────────────────────────────────────

def test_source_with_no_matching_ticker_yields_empty(tmp_path):
    """A source that contains only rows for an unrelated ticker yields an
    empty schema-correct frame after filtering."""
    p = tmp_path / "p.parquet"
    _write_sentiment(p, [
        _row(datetime(2026, 4, 15, 9, 30), ticker="MSFT", url="u1"),
    ])
    out = build_sentiment_df("AAPL", [p], TransformationReport())
    assert out.height == 0
    assert dict(out.schema) == SCHEMAS["sentiment_df"]
