"""Download historical news sentiment data (NEWS_SENTIMENT).

Global query (no per-ticker filtering). Paginates backward from current UTC
time to 2010-01-01, 1000 articles per call. Produces:
  - ALL_MESSAGES.parquet   (all articles with tickers matching the catalog)
  - {SYMBOL}.parquet       (per active-stock filtered view)
"""

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp
import polars as pl

from historical_data_setup._common import (
    AV_BASE,
    AVResponseError,
    IssueTracker,
    RateLimiter,
    fetch_av_json,
    read_catalog_symbols,
    symbol_parquet_name,
)

logger = logging.getLogger(__name__)

# Fixed topic columns -- API topic name doubles as the DataFrame column name
TOPIC_COLUMNS = [
    "blockchain",
    "earnings",
    "ipo",
    "mergers_and_acquisitions",
    "financial_markets",
    "economy_fiscal",
    "economy_monetary",
    "economy_macro",
    "energy_transportation",
    "finance",
    "life_sciences",
    "manufacturing",
    "real_estate",
    "retail_wholesale",
    "technology",
]

TIME_FROM = "20100101T0000"

SCHEMA = {
    "time_published": pl.Datetime,
    "ticker": pl.String,
    "ticker_relevance_score": pl.Float32,
    "ticker_sentiment_score": pl.Float32,
    "ticker_sentiment_label": pl.String,
    "title": pl.String,
    "url": pl.String,
    "authors": pl.String,
    "summary": pl.String,
    "banner_image": pl.String,
    "source": pl.String,
    "category_within_source": pl.String,
    "source_domain": pl.String,
    "overall_sentiment_score": pl.Float32,
    "overall_sentiment_label": pl.String,
    **{topic: pl.Float32 for topic in TOPIC_COLUMNS},
}


def _parse_time_published(raw: str) -> datetime | None:
    """Parse '20260410T153926' to datetime. Returns None on failure."""
    try:
        return datetime.strptime(raw, "%Y%m%dT%H%M%S")
    except (ValueError, TypeError):
        return None


def _ceil_to_minute(dt: datetime) -> str:
    """Truncate to minute, add one minute, format as YYYYMMDDTHHMM.

    This ensures the next backward query still covers the full minute of
    the boundary article, so no messages are missed.  Deduplication
    removes the resulting overlap.
    """
    truncated = dt.replace(second=0, microsecond=0)
    bumped = truncated + timedelta(minutes=1)
    return bumped.strftime("%Y%m%dT%H%M")


_NULL_SENTINELS = {None, "None", "", "."}


def _safe_float(val) -> float | None:
    """Convert to float, treating null sentinels / failures as null."""
    if val in _NULL_SENTINELS:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_str(val) -> str | None:
    """Convert to str, treating null sentinels as null."""
    if val in _NULL_SENTINELS:
        return None
    return str(val)


def _parse_feed(feed: list[dict], issue_tracker: IssueTracker) -> list[dict]:
    """Parse the 'feed' list into flat row dicts (one per ticker per article)."""
    rows: list[dict] = []

    for article in feed:
        time_raw = article.get("time_published")
        time_dt = _parse_time_published(time_raw)
        if time_dt is None:
            issue_tracker.record(
                "GLOBAL", "stocks", "sentiment",
                "cast_failure", f"time_published parse failed: {time_raw}",
            )
            continue

        # Article-level fields
        authors_list = article.get("authors") or []
        authors = ";".join(authors_list) if authors_list else ""

        overall_score = _safe_float(article.get("overall_sentiment_score"))

        # Topics -> dict of relevance scores
        topic_scores: dict[str, float | None] = {}
        for t in article.get("topics") or []:
            name = t.get("topic")
            if name:
                topic_scores[name] = _safe_float(t.get("relevance_score"))

        # Per-ticker rows
        ticker_sentiments = article.get("ticker_sentiment") or []
        if not ticker_sentiments:
            continue

        for ts in ticker_sentiments:
            row = {
                "time_published": time_dt,
                "ticker": _safe_str(ts.get("ticker")),
                "ticker_relevance_score": _safe_float(ts.get("relevance_score")),
                "ticker_sentiment_score": _safe_float(ts.get("ticker_sentiment_score")),
                "ticker_sentiment_label": _safe_str(ts.get("ticker_sentiment_label")),
                "title": _safe_str(article.get("title")),
                "url": _safe_str(article.get("url")),
                "authors": authors,
                "summary": _safe_str(article.get("summary")),
                "banner_image": _safe_str(article.get("banner_image")),
                "source": _safe_str(article.get("source")),
                "category_within_source": _safe_str(article.get("category_within_source")),
                "source_domain": _safe_str(article.get("source_domain")),
                "overall_sentiment_score": overall_score,
                "overall_sentiment_label": _safe_str(article.get("overall_sentiment_label")),
            }

            for topic_col in TOPIC_COLUMNS:
                row[topic_col] = topic_scores.get(topic_col)

            rows.append(row)

    return rows


async def fetch_sentiment(
    catalog_dir: Path,
    historical_dir: Path,
    api_key: str,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    issue_tracker: IssueTracker,
    asset_type: str = "stocks",
) -> None:
    """Download historical news sentiment for all catalog symbols.

    Uses a single global query (no per-ticker filter) and paginates backward
    from the current UTC time to 2010-01-01.  After fetching, rows are filtered
    to tickers present in the catalog, deduplicated on (url, ticker), and saved
    as ALL_MESSAGES.parquet.  Per-symbol files are then split from this master
    table for every active symbol.
    """
    output_dir = historical_dir / asset_type / "sentiment"
    output_dir.mkdir(parents=True, exist_ok=True)
    all_path = output_dir / "ALL_MESSAGES.parquet"

    # Load catalog symbols for filtering
    catalog = read_catalog_symbols(catalog_dir, asset_type)
    all_catalog_symbols: set[str] = set(catalog["symbol"].to_list())
    active_symbols: set[str] = set(
        catalog.filter(pl.col("status") == "Active")["symbol"].to_list()
    )

    if all_path.exists():
        logger.info("sentiment: ALL_MESSAGES.parquet exists, skipping global fetch")
        all_df = pl.read_parquet(all_path)
    else:
        # -- Backward pagination from now to TIME_FROM -------------------------
        time_to = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M")
        all_rows: list[dict] = []
        query_count = 0

        while time_to > TIME_FROM:
            url = (
                f"{AV_BASE}/query?function=NEWS_SENTIMENT"
                f"&time_from={TIME_FROM}&time_to={time_to}"
                f"&limit=1000&apikey={api_key}"
            )

            try:
                data = await fetch_av_json(url, session, rate_limiter)
            except AVResponseError as e:
                issue_tracker.record(
                    "GLOBAL", asset_type, "sentiment", "av_throttle", str(e),
                )
                break
            except Exception as e:
                issue_tracker.record(
                    "GLOBAL", asset_type, "sentiment",
                    "structure_error", f"fetch failed: {e}",
                )
                break

            query_count += 1

            # Validate response structure
            if "feed" not in data:
                issue_tracker.record(
                    "GLOBAL", asset_type, "sentiment",
                    "structure_error",
                    f"missing 'feed' key (time_to={time_to})",
                )
                del data
                break

            feed = data["feed"]
            items_str = data.get("items", "0")
            del data

            if not feed:
                logger.info(f"sentiment: empty feed at time_to={time_to}, done")
                break

            rows = _parse_feed(feed, issue_tracker)
            del feed

            if not rows:
                logger.info(
                    f"sentiment: no ticker rows at time_to={time_to}, done"
                )
                break

            all_rows.extend(rows)

            # Find oldest time_published for next pagination step
            oldest_dt = min(r["time_published"] for r in rows)
            del rows

            new_time_to = _ceil_to_minute(oldest_dt)

            logger.info(
                f"  query {query_count}: {items_str} articles, "
                f"oldest={oldest_dt.strftime('%Y%m%dT%H%M%S')}, "
                f"next time_to={new_time_to}, "
                f"accumulated {len(all_rows)} rows"
            )

            # Safety: if no progress, force a step back to avoid infinite loop
            if new_time_to >= time_to:
                logger.warning(
                    f"sentiment: no progress at time_to={time_to}, "
                    f"forcing step back"
                )
                parsed = _parse_time_published(time_to + "00")
                if parsed is None:
                    break
                new_time_to = (
                    parsed - timedelta(minutes=10)
                ).strftime("%Y%m%dT%H%M")

            time_to = new_time_to

        if not all_rows:
            issue_tracker.record(
                "GLOBAL", asset_type, "sentiment",
                "empty_content", "no sentiment data fetched",
            )
            return

        logger.info(
            f"sentiment: {query_count} API calls, "
            f"{len(all_rows)} total rows before filtering"
        )

        # Build DataFrame, filter, deduplicate, sort
        all_df = pl.DataFrame(all_rows, schema=SCHEMA)
        del all_rows

        all_df = all_df.filter(pl.col("ticker").is_in(list(all_catalog_symbols)))
        all_df = all_df.unique(subset=["url", "ticker"])
        all_df = all_df.sort("time_published")

        all_df.write_parquet(all_path, compression="zstd")
        logger.info(
            f"sentiment: saved ALL_MESSAGES.parquet ({all_df.height} rows)"
        )

    # -- Per-symbol split (active symbols only) --------------------------------
    saved = 0
    for symbol in sorted(active_symbols):
        sym_path = output_dir / symbol_parquet_name(asset_type, symbol)
        if sym_path.exists():
            continue

        sym_df = all_df.filter(pl.col("ticker") == symbol)
        if sym_df.height == 0:
            continue

        sym_df.write_parquet(sym_path, compression="zstd")
        saved += 1

    logger.info(f"sentiment: saved {saved} per-symbol files")
    del all_df
