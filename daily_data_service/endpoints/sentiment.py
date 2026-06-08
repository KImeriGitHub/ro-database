"""Daily pull of news sentiment (NEWS_SENTIMENT).

Global backward pagination from the current UTC time down to
``min(previous_date, folder_date - PRICE_WINDOW_DAYS) 00:00 UTC``
(inclusive). The 7-day floor mirrors the price endpoints so a
successful run recovers the last week of articles even when
intermediate runs failed, at the cost of overlap between adjacent
``daily/<date>/`` folders; downstream consumers must dedup on
``(url, ticker)``. Produces:
  - ALL_MESSAGES.parquet   (all articles with tickers matching the catalog)
  - {SYMBOL}.parquet       (per active-stock filtered view)
"""

import logging
from datetime import date, datetime, timedelta, timezone
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
from historical_data_setup.endpoints.sentiment import (
    SCHEMA,
    _ceil_to_minute,
    _parse_feed,
    _parse_time_published,
)
from daily_data_service._common import price_window_lower

logger = logging.getLogger(__name__)

# Number of times a single pagination window is re-fetched before its empty /
# error response is accepted as terminal. Alpha Vantage occasionally returns an
# empty (or malformed) NEWS_SENTIMENT payload for a window that does have data;
# retrying the same time_to a couple of times absorbs those transient blips.
MAX_SENTIMENT_RETRIES = 2


async def fetch_sentiment(
    catalog_dir: Path,
    daily_dir: Path,
    api_key: str,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    issue_tracker: IssueTracker,
    asset_type: str,
    folder_date: date,
    previous_date: date,
    symbols_filter: set[str] | None = None,
) -> None:
    output_dir = daily_dir / asset_type / "sentiment"
    output_dir.mkdir(parents=True, exist_ok=True)
    all_path = output_dir / "ALL_MESSAGES.parquet"

    catalog = read_catalog_symbols(catalog_dir, asset_type)
    all_catalog_symbols: set[str] = set(catalog["symbol"].to_list())
    active_symbols: set[str] = set(
        catalog.filter(
            pl.col("status").is_in(["Active", "Corrupted"])
        )["symbol"].to_list()
    )
    if symbols_filter is not None:
        active_symbols &= symbols_filter

    lower_date = price_window_lower(previous_date, folder_date)
    time_from = datetime.combine(
        lower_date, datetime.min.time(), tzinfo=timezone.utc,
    ).strftime("%Y%m%dT%H%M")

    if all_path.exists():
        logger.info("sentiment: ALL_MESSAGES.parquet exists, skipping global fetch")
        all_df = pl.read_parquet(all_path)
    else:
        time_to = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M")
        all_rows: list[dict] = []
        query_count = 0
        retries = 0
        pending_issue: tuple[str, str] | None = None

        while time_to > time_from and retries <= MAX_SENTIMENT_RETRIES:
            url = (
                f"{AV_BASE}/query?function=NEWS_SENTIMENT"
                f"&time_from={time_from}&time_to={time_to}"
                f"&limit=1000&apikey={api_key}"
            )

            try:
                data = await fetch_av_json(url, session, rate_limiter)
            except AVResponseError as e:
                retries += 1
                pending_issue = ("av_throttle", str(e))
                logger.warning(
                    f"sentiment: fetch error at time_to={time_to} "
                    f"(retry {retries}/{MAX_SENTIMENT_RETRIES})"
                )
                continue
            except Exception as e:
                retries += 1
                pending_issue = ("structure_error", f"fetch failed: {e}")
                logger.warning(
                    f"sentiment: fetch failed at time_to={time_to} "
                    f"(retry {retries}/{MAX_SENTIMENT_RETRIES})"
                )
                continue

            query_count += 1

            if "feed" not in data:
                retries += 1
                pending_issue = (
                    "structure_error",
                    f"missing 'feed' key (time_to={time_to})",
                )
                logger.warning(
                    f"sentiment: missing 'feed' key at time_to={time_to} "
                    f"(retry {retries}/{MAX_SENTIMENT_RETRIES})"
                )
                del data
                continue

            feed = data["feed"]
            items_str = data.get("items", "0")
            del data

            if not feed:
                retries += 1
                pending_issue = None
                logger.info(
                    f"sentiment: empty feed at time_to={time_to} "
                    f"(retry {retries}/{MAX_SENTIMENT_RETRIES})"
                )
                continue

            rows = _parse_feed(feed, issue_tracker)
            del feed

            if not rows:
                retries += 1
                pending_issue = None
                logger.info(
                    f"sentiment: no ticker rows at time_to={time_to} "
                    f"(retry {retries}/{MAX_SENTIMENT_RETRIES})"
                )
                continue

            # A window returned usable rows: progress was made, so reset the
            # retry budget and clear any pending issue from an earlier stall.
            retries = 0
            pending_issue = None

            all_rows.extend(rows)

            oldest_dt = min(r["time_published"] for r in rows)
            del rows

            new_time_to = _ceil_to_minute(oldest_dt)

            logger.info(
                f"  query {query_count}: {items_str} articles, "
                f"oldest={oldest_dt.strftime('%Y%m%dT%H%M%S')}, "
                f"next time_to={new_time_to}, "
                f"accumulated {len(all_rows)} rows"
            )

            if new_time_to >= time_to:
                logger.info(
                    f"sentiment: no progress at time_to={time_to}, "
                    f"forcing step back"
                )
                parsed = _parse_time_published(time_to + "00")
                if parsed is None:
                    break
                new_time_to = (
                    parsed - timedelta(minutes=1)
                ).strftime("%Y%m%dT%H%M")

            time_to = new_time_to

        if pending_issue is not None:
            # Retries were exhausted on a hard error (throttle / malformed
            # response). Empty-feed and no-rows stalls clear pending_issue, so
            # they exit quietly as a legitimate end-of-window.
            issue_tracker.record(
                "GLOBAL", asset_type, "sentiment",
                pending_issue[0], pending_issue[1],
            )

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

        all_df = pl.DataFrame(all_rows, schema=SCHEMA)
        del all_rows

        all_df = all_df.filter(pl.col("ticker").is_in(list(all_catalog_symbols)))
        all_df = all_df.unique(subset=["url", "ticker"])
        all_df = all_df.sort("time_published")

        all_df.write_parquet(all_path, compression="zstd")
        logger.info(
            f"sentiment: saved ALL_MESSAGES.parquet ({all_df.height} rows)"
        )

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
