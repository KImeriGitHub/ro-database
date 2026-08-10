"""Daily Data Service - adjust_weekly.py

Weekend re-query pass over the most recent daily folder. The retry plan
is the union of two sources, both keyed on
``(symbol, asset_type, endpoint)``:

  1. Every ``False`` cell in ``catalog/yield_status.parquet`` (asset_type
     recovered by re-joining ``symbol`` against the catalog parquets;
     the ``direct`` column maps to the symbol's asset_type as the
     endpoint name). The ``sentiment`` column is excluded -- see below.
  2. Every row in any ``daily/<d>/ingestion_report.parquet`` for ``d``
     in ``(previous_date, folder_date]``. Older-date reports are read
     but never modified (``daily/`` stays append-only beyond
     ``folder_date``).

All retried results land under ``daily/<folder_date>/``.

Date resolution:
  - ``folder_date``   = max ``YYYY-MM-DD`` subdir under ``daily_dir``.
  - ``previous_date`` = max folder-date strictly earlier than
    ``folder_date - look_back_days``. If no such folder exists, falls back
    to ``folder_date - (look_back_days + 1)``.

Skip semantics mirror the daily run: each endpoint's per-symbol
``out_path.exists()`` guard means symbols that already have a valid file
on ``folder_date`` are not re-queried. For fundamentals, the guard skips
only when BOTH ``SYMBOL_annual.parquet`` and ``SYMBOL_quarterly.parquet``
exist.

Sentiment is all-or-nothing and is triggered ONLY by a ``GLOBAL`` row in
an in-window ingestion report. ``yield_status`` ``sentiment`` False cells
do not trigger the rerun (those cells track coverage of the global pull,
not per-symbol fetch failures). When triggered, every file under
``stocks/sentiment/`` is renamed to ``*.pre_weekly`` before
``fetch_sentiment`` runs so the endpoint's existence guards don't
short-circuit the global fetch; any existing ``.pre_weekly`` siblings
from a previous weekend pass are overwritten.

After all retries finish, only ``daily/<folder_date>/ingestion_report.parquet``
is rewritten: rows for retried triples are dropped (triples sourced
exclusively from ``yield_status`` or older reports won't have a row here,
and that's fine), fresh issues from this pass are appended, and
``yield_status.parquet`` is recomputed via the same
``finalize_yield_status`` the daily full run uses.

Usage:
    python adjust_weekly.py [--catalog-dir PATH] [--daily-dir PATH]
                            [--look-back-days 7] [--api-tier premium]
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
from maintainance_scripts.logging_setup import configure_logging

from asset_catalog_service.updates import finalize_yield_status
from config.settings import AV_RATE_LIMIT_PER_MIN, DISABLED_ASSET_TYPES
from daily_data_service._common import ET
from daily_data_service.ensure_folders import ensure_daily_folders
from daily_data_service.setup_daily import (
    ACTIVE_ONLY_ENDPOINTS,
    ASSET_ENDPOINTS,
    ENDPOINT_MAP,
    FINANCIAL_ENDPOINTS,
    YIELD_SKIP_ENDPOINTS,
    _run_endpoint_task,
)
from historical_data_setup._common import IssueTracker, RateLimiter
from historical_data_setup.earnings_calendar import fetch_earnings_calendar

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


_CATALOG_FILES: tuple[tuple[str, str], ...] = (
    ("stocks.parquet", "stocks"),
    ("etfs.parquet", "etfs"),
    ("forex.parquet", "forex"),
    ("indices.parquet", "indices"),
    ("cryptocurrencies.parquet", "cryptocurrencies"),
    ("commodities.parquet", "commodities"),
    ("economic.parquet", "economic"),
)


def _in_window_report_paths(
    daily_dir: Path, previous_date: date, folder_date: date
) -> list[Path]:
    """Return ingestion_report.parquet paths for every YYYY-MM-DD subdir
    whose date ``d`` satisfies ``previous_date < d <= folder_date``."""
    out: list[Path] = []
    for d in _list_folder_dates(daily_dir):
        if previous_date < d <= folder_date:
            p = daily_dir / d.isoformat() / "ingestion_report.parquet"
            if p.exists():
                out.append(p)
    return out


def _load_symbol_asset_type_map(catalog_dir: Path) -> dict[str, str]:
    """Return ``{symbol: asset_type}`` from every catalog parquet."""
    mapping: dict[str, str] = {}
    for fname, asset_type in _CATALOG_FILES:
        path = catalog_dir / fname
        if not path.exists():
            continue
        for sym in pl.read_parquet(path)["symbol"].to_list():
            mapping.setdefault(sym, asset_type)
    return mapping


def _yield_status_false_triples(
    catalog_dir: Path,
    sym_to_asset: dict[str, str],
) -> list[tuple[str, str, str]]:
    """Return ``(symbol, asset_type, endpoint)`` for every explicit ``False``
    cell in ``yield_status.parquet``.

    The ``sentiment`` column is excluded -- the sentiment full rerun is
    gated on a ``GLOBAL`` row in an in-window ingestion report, not on
    per-symbol yield cells. The ``direct`` column maps to the symbol's
    asset_type as the endpoint name (e.g. forex symbol with
    ``direct=False`` -> endpoint ``forex``).
    """
    path = catalog_dir / "yield_status.parquet"
    if not path.exists():
        return []
    df = pl.read_parquet(path)
    out: list[tuple[str, str, str]] = []
    for col in df.columns:
        if col in ("symbol", "date", "sentiment"):
            continue
        false_syms = df.filter(pl.col(col).eq(False))["symbol"].to_list()
        for sym in false_syms:
            asset_type = sym_to_asset.get(sym)
            if asset_type is None:
                continue
            ep = asset_type if col == "direct" else col
            out.append((sym, asset_type, ep))
    return out


def _build_retry_plan(
    catalog_dir: Path,
    daily_dir: Path,
    previous_date: date,
    folder_date: date,
) -> tuple[dict[tuple[str, str], set[str]], pl.DataFrame, bool]:
    """Build the union retry plan from in-window ingestion reports and
    yield_status False cells.

    Returns ``(plan, folder_date_report, sentiment_full_rerun)``:

    - ``plan[(asset_type, endpoint)]`` -> set of symbols. Sentiment is
      never a per-symbol entry; per-symbol sentiment rows in any report
      are ignored on the way in (sentiment is global -- see below).
    - ``folder_date_report`` is the existing report at ``folder_date``
      (empty DataFrame if missing). Only this report is rewritten on
      merge; older-date reports stay append-only.
    - ``sentiment_full_rerun`` is True iff any in-window ingestion report
      has a row with ``symbol == 'GLOBAL'`` and ``endpoint == 'sentiment'``.
      yield_status ``sentiment`` False cells do not set this flag.
    """
    plan: dict[tuple[str, str], set[str]] = {}
    sentiment_full_rerun = False

    for report_path in _in_window_report_paths(
        daily_dir, previous_date, folder_date
    ):
        report = pl.read_parquet(report_path)
        for row in report.iter_rows(named=True):
            ep = row["endpoint"]
            if ep == "sentiment":
                if row["symbol"] == _SENTINEL_GLOBAL:
                    sentiment_full_rerun = True
                continue
            plan.setdefault((row["asset_type"], ep), set()).add(row["symbol"])

    sym_to_asset = _load_symbol_asset_type_map(catalog_dir)
    for sym, asset_type, ep in _yield_status_false_triples(
        catalog_dir, sym_to_asset
    ):
        plan.setdefault((asset_type, ep), set()).add(sym)

    fd_report_path = (
        daily_dir / folder_date.isoformat() / "ingestion_report.parquet"
    )
    if fd_report_path.exists():
        fd_report = pl.read_parquet(fd_report_path)
    else:
        fd_report = pl.DataFrame()

    return plan, fd_report, sentiment_full_rerun


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
    look_back_days: int = 7,
    api_tier: str = "premium",
) -> None:
    """Retry the union of ``yield_status`` False cells and every
    ``(symbol, asset_type, endpoint)`` row in any ingestion report dated
    in ``(previous_date, folder_date]``, writing all results into
    ``daily/<folder_date>/``."""
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

    # Refresh earnings_calendar if the daily run never produced one for this
    # folder_date (e.g. weekday fetch failed, or this folder predates the
    # endpoint move). When the file already exists we leave it alone so the
    # weekend pass doesn't churn over a healthy calendar.
    ec_path = day_root / "earnings_calendar.parquet"
    if not ec_path.exists():
        api_key = get_alpha_vantage_key(api_tier)
        try:
            fetch_earnings_calendar(api_key, day_root)
        except Exception:
            logger.exception("earnings_calendar fetch failed; continuing")

    plan, old_report, sentiment_full_rerun = _build_retry_plan(
        catalog_dir, daily_dir, previous_date, folder_date,
    )

    # Sentiment is global: triggered only by a GLOBAL row in any in-window
    # ingestion report. yield_status `sentiment` False cells are not a
    # trigger source. Per-symbol sentiment rows (if any) were already
    # filtered out by _build_retry_plan.
    sentiment_key = ("stocks", "sentiment")
    plan.pop(sentiment_key, None)
    if sentiment_full_rerun:
        plan[sentiment_key] = set()

    if not plan:
        logger.info("Nothing to retry; exiting without changes.")
        return

    ensure_daily_folders(daily_dir, folder_date)

    if sentiment_full_rerun:
        renamed = _rename_sentiment_files(day_root / "stocks" / "sentiment")
        logger.info(
            f"sentiment retry: renamed {renamed} file(s) to *.pre_weekly "
            f"before rerun"
        )

    api_key = get_alpha_vantage_key(api_tier)
    rate_limiter = RateLimiter(float(AV_RATE_LIMIT_PER_MIN))
    issue_tracker = IssueTracker()

    tasks_plan: list[tuple[str, object, str, str, set[str]]] = []
    for (asset_type, ep_name), symbols in plan.items():
        # Reports written before the asset type was disabled still carry rows
        # for it; drop them without the misleading ASSET_ENDPOINTS warning.
        if asset_type in DISABLED_ASSET_TYPES:
            logger.info(f"Skipping ({asset_type}, {ep_name}): asset type disabled")
            continue
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
        def build_task(label, func, asset_type, ep_name, symbols):
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
                    if ep in ACTIVE_ONLY_ENDPOINTS:
                        extra["active_only"] = False
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

            return _run_endpoint_task(label, make_factory())

        # Mirror the daily run's ordering: non-financial endpoints first, the
        # fundamental statements (FINANCIAL_ENDPOINTS) last.
        phase_one = [t for t in tasks_plan if t[3] not in FINANCIAL_ENDPOINTS]
        phase_two = [t for t in tasks_plan if t[3] in FINANCIAL_ENDPOINTS]

        if phase_one:
            logger.info(f"Phase 1 (non-financial): running {len(phase_one)} task(s)")
            await asyncio.gather(
                *(build_task(*t) for t in phase_one), return_exceptions=False
            )
            logger.info("Phase 1 (non-financial) complete")
        if phase_two:
            logger.info(f"Phase 2 (financial): running {len(phase_two)} task(s)")
            await asyncio.gather(
                *(build_task(*t) for t in phase_two), return_exceptions=False
            )
            logger.info("Phase 2 (financial) complete")

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

    configure_logging()

    parser = argparse.ArgumentParser(
        description="Weekend retry pass over the latest daily folder"
    )
    parser.add_argument("--catalog-dir", type=Path, default=None)
    parser.add_argument("--daily-dir", type=Path, default=None)
    parser.add_argument("--look-back-days", type=int, default=7)
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
