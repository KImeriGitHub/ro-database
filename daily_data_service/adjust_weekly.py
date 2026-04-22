"""Daily Data Service - adjust_weekly.py

Weekend re-query pass over the most recent daily folder. Retries only the
``(symbol, asset_type, endpoint)`` cells flagged in that folder's
``ingestion_report.parquet`` and writes their results back into the same
folder with the wider ``(previous_date, folder_date]`` truncation window.

Date resolution:
  - ``folder_date``   = max ``YYYY-MM-DD`` subdir under ``daily_dir``.
  - ``previous_date`` = max folder-date strictly earlier than
    ``folder_date - look_back_days``. If no such folder exists, falls back
    to ``folder_date - (look_back_days + 1)``.

Skip semantics mirror the daily run: each endpoint's per-symbol
``out_path.exists()`` guard means symbols that were written successfully
on ``folder_date`` are not re-queried. For fundamentals, the guard skips
only when BOTH ``SYMBOL_annual.parquet`` and ``SYMBOL_quarterly.parquet``
exist.

Sentiment is all-or-nothing: any sentiment issue (including ``GLOBAL``)
triggers a full rerun. Before ``fetch_sentiment`` is called, every file
under ``stocks/sentiment/`` is renamed to ``*.pre_weekly`` so the endpoint's
existence guards don't short-circuit the global fetch. Any existing
``.pre_weekly`` siblings from a previous weekend pass are overwritten.

After all retries finish the ingestion report is rewritten in place:
rows for retried ``(symbol, asset_type, endpoint)`` triples are dropped,
fresh issues from this pass are appended, and ``yield_status.parquet`` is
recomputed via the same ``finalize_yield_status`` the daily full run uses.

Usage:
    python adjust_weekly.py [--catalog-dir PATH] [--daily-dir PATH]
                            [--look-back-days 6] [--api-tier premium]
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import aiohttp
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from maintainance_scripts.get_api_key import get_alpha_vantage_key

from asset_catalog_service.updates import finalize_yield_status
from daily_data_service._common import ET
from daily_data_service.ensure_folders import ensure_daily_folders
from daily_data_service.setup_daily import (
    ASSET_ENDPOINTS,
    ENDPOINT_MAP,
    YIELD_SKIP_ENDPOINTS,
    _run_endpoint_task,
)
from historical_data_setup._common import IssueTracker, RateLimiter

logger = logging.getLogger(__name__)

_DATE_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SENTINEL_GLOBAL = "GLOBAL"


def _list_folder_dates(daily_dir: Path) -> list[date]:
    """Return every ``YYYY-MM-DD`` subdirectory of *daily_dir* as a date."""
    if not daily_dir.exists():
        return []
    out: list[date] = []
    for child in daily_dir.iterdir():
        if not child.is_dir():
            continue
        if not _DATE_DIR_RE.match(child.name):
            continue
        try:
            out.append(date.fromisoformat(child.name))
        except ValueError:
            continue
    return sorted(out)


def resolve_dates(
    daily_dir: Path, look_back_days: int
) -> tuple[date, date]:
    """Return ``(folder_date, previous_date)`` using the rules above."""
    dates = _list_folder_dates(daily_dir)
    if not dates:
        raise FileNotFoundError(
            f"No YYYY-MM-DD subdirectories under {daily_dir}; "
            f"run daily ingest first."
        )
    folder_date = dates[-1]
    cutoff = folder_date - timedelta(days=look_back_days)
    earlier = [d for d in dates if d < cutoff]
    if earlier:
        previous_date = earlier[-1]
    else:
        previous_date = folder_date - timedelta(days=look_back_days + 1)
    return folder_date, previous_date


def _load_retry_plan(
    report_path: Path,
) -> tuple[dict[tuple[str, str], set[str]], pl.DataFrame]:
    """Return ``(plan, report_df)`` where:

    - ``plan[(asset_type, endpoint)]`` is the set of symbols (including the
      ``GLOBAL`` sentinel for sentiment) that appeared in the ingestion
      report for that pair.
    - ``report_df`` is the full report, returned so the caller can merge it.
    """
    if not report_path.exists():
        logger.info(f"No ingestion report at {report_path}; nothing to retry")
        return {}, pl.DataFrame()

    report = pl.read_parquet(report_path)
    plan: dict[tuple[str, str], set[str]] = {}
    for row in report.iter_rows(named=True):
        key = (row["asset_type"], row["endpoint"])
        plan.setdefault(key, set()).add(row["symbol"])
    return plan, report


def _rename_sentiment_files(sentiment_dir: Path) -> int:
    """Rename every ``*.parquet`` under *sentiment_dir* to ``*.parquet.pre_weekly``.

    Overwrites any pre-existing ``.pre_weekly`` siblings from an earlier
    weekend pass. Returns the count of renamed files.
    """
    if not sentiment_dir.exists():
        return 0
    renamed = 0
    for src in sentiment_dir.iterdir():
        if not src.is_file():
            continue
        if src.suffix != ".parquet":
            continue
        dst = src.with_suffix(".parquet.pre_weekly")
        os.replace(src, dst)
        renamed += 1
    return renamed


_REPORT_SCHEMA = {
    "symbol": pl.Utf8,
    "asset_type": pl.Utf8,
    "endpoint": pl.Utf8,
    "issue_type": pl.Utf8,
    "detail": pl.Utf8,
    "timestamp": pl.Datetime,
}


def _merge_report(
    old_report: pl.DataFrame,
    retried_keys: set[tuple[str, str, str]],
    fresh_tracker: IssueTracker,
) -> pl.DataFrame:
    """Drop retried ``(symbol, asset_type, endpoint)`` triples from *old_report*
    and append whatever *fresh_tracker* recorded this pass."""
    if old_report.is_empty() or not retried_keys:
        kept = old_report
    else:
        retried_rows = pl.DataFrame(
            {
                "symbol": [k[0] for k in retried_keys],
                "asset_type": [k[1] for k in retried_keys],
                "endpoint": [k[2] for k in retried_keys],
            },
            schema={"symbol": pl.Utf8, "asset_type": pl.Utf8, "endpoint": pl.Utf8},
        )
        kept = old_report.join(
            retried_rows,
            on=["symbol", "asset_type", "endpoint"],
            how="anti",
        )

    fresh_df = pl.DataFrame(fresh_tracker._rows, schema=_REPORT_SCHEMA)

    if kept.is_empty():
        return fresh_df
    if fresh_df.is_empty():
        return kept
    return pl.concat([kept, fresh_df], how="vertical_relaxed")


async def adjust_weekly(
    catalog_dir: Path | None = None,
    daily_dir: Path | None = None,
    look_back_days: int = 6,
    api_tier: str = "premium",
) -> None:
    """Retry the ``(symbol, asset_type, endpoint)`` cells flagged in the latest
    daily folder's ingestion report, writing back into that same folder."""
    project_root = Path(__file__).resolve().parent.parent
    if catalog_dir is None:
        catalog_dir = project_root / "catalog"
    if daily_dir is None:
        daily_dir = project_root / "daily"

    folder_date, previous_date = resolve_dates(daily_dir, look_back_days)
    day_root = daily_dir / folder_date.isoformat()
    report_path = day_root / "ingestion_report.parquet"

    logger.info(
        f"Weekly adjust: folder_date={folder_date}, "
        f"previous_date={previous_date}, look_back_days={look_back_days}"
    )

    plan, old_report = _load_retry_plan(report_path)
    if not plan:
        logger.info("Nothing to retry; exiting without changes.")
        return

    ensure_daily_folders(daily_dir, folder_date)

    # Sentiment: any entry (per-symbol or GLOBAL) forces a full rerun.
    sentiment_key = ("stocks", "sentiment")
    if sentiment_key in plan:
        renamed = _rename_sentiment_files(day_root / "stocks" / "sentiment")
        logger.info(
            f"sentiment retry: renamed {renamed} file(s) to *.pre_weekly "
            f"before rerun"
        )
        # Let fetch_sentiment write every active symbol again: the global
        # paginated fetch covers all catalog tickers regardless of filter.
        plan[sentiment_key] = set()

    api_key = get_alpha_vantage_key(api_tier)
    rate_limiter = RateLimiter(74.0)
    issue_tracker = IssueTracker()

    tasks_plan: list[tuple[str, object, str, str, set[str]]] = []
    for (asset_type, ep_name), symbols in plan.items():
        applicable = ASSET_ENDPOINTS.get(asset_type, [])
        if ep_name not in applicable:
            logger.warning(
                f"Skipping ({asset_type}, {ep_name}): not in ASSET_ENDPOINTS"
            )
            continue
        func = ENDPOINT_MAP.get(ep_name)
        if func is None:
            logger.warning(f"Unknown endpoint '{ep_name}', skipping")
            continue
        label = f"{ep_name} ({asset_type}) [retry {len(symbols) or 'all'}]"
        tasks_plan.append((label, func, asset_type, ep_name, symbols))

    if not tasks_plan:
        logger.info("No valid endpoint tasks after filtering; exiting.")
        return

    logger.info(
        f"Scheduling {len(tasks_plan)} retry task(s): "
        f"{', '.join(label for label, *_ in tasks_plan)}"
    )

    connector = aiohttp.TCPConnector(limit=len(tasks_plan))
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for label, func, asset_type, ep_name, symbols in tasks_plan:
            # sentinels / non-catalog rows like GLOBAL are silently dropped
            # by the endpoint's catalog.is_in filter.
            sym_filter: set[str] | None = symbols if symbols else None

            def make_factory(
                f=func, at=asset_type, ep=ep_name, flt=sym_filter,
            ):
                async def _call():
                    extra: dict = {}
                    if ep in YIELD_SKIP_ENDPOINTS:
                        extra["skip_empty_yield"] = False
                    await f(
                        catalog_dir=catalog_dir,
                        daily_dir=day_root,
                        api_key=api_key,
                        session=session,
                        rate_limiter=rate_limiter,
                        issue_tracker=issue_tracker,
                        asset_type=at,
                        folder_date=folder_date,
                        previous_date=previous_date,
                        symbols_filter=flt,
                        **extra,
                    )
                return _call

            tasks.append(_run_endpoint_task(label, make_factory()))

        await asyncio.gather(*tasks, return_exceptions=False)

    # Merge-in-place: drop every retried (symbol, asset_type, endpoint) row
    # from the original report, then append whatever the fresh pass logged.
    # Iterate tasks_plan -- not plan -- so endpoints that never dispatched
    # (unknown endpoint, not in ASSET_ENDPOINTS) don't silently lose rows.
    retried_keys: set[tuple[str, str, str]] = set()
    for _label, _func, asset_type, ep_name, symbols in tasks_plan:
        # If symbols is empty (sentiment full rerun), drop every row matching
        # that (asset_type, endpoint) regardless of symbol.
        if not symbols:
            if not old_report.is_empty():
                matching = old_report.filter(
                    (pl.col("asset_type") == asset_type)
                    & (pl.col("endpoint") == ep_name)
                )["symbol"].to_list()
                for s in matching:
                    retried_keys.add((s, asset_type, ep_name))
        else:
            for s in symbols:
                retried_keys.add((s, asset_type, ep_name))

    merged = _merge_report(old_report, retried_keys, issue_tracker)
    merged.write_parquet(report_path, compression="zstd")
    logger.info(
        f"Merged ingestion report: {old_report.height} old rows, "
        f"{len(retried_keys)} retried triples dropped, "
        f"{issue_tracker.count} new issues, {merged.height} final rows"
    )

    finalize_yield_status(catalog_dir, day_root, datetime.now(tz=ET))


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Weekend retry pass over the latest daily folder"
    )
    parser.add_argument("--catalog-dir", type=Path, default=None)
    parser.add_argument("--daily-dir", type=Path, default=None)
    parser.add_argument("--look-back-days", type=int, default=6)
    parser.add_argument(
        "--api-tier", default="premium", choices=("standard", "premium"),
    )
    args = parser.parse_args()

    asyncio.run(adjust_weekly(
        catalog_dir=args.catalog_dir,
        daily_dir=args.daily_dir,
        look_back_days=args.look_back_days,
        api_tier=args.api_tier,
    ))
