"""CLI orchestrator for the data_transformation pipeline.

Phases (in order):
  1.  assets_overview.parquet              (overview.py)
  2.  simple price_daily                   (price_daily.py)
  3.  shareprice_daily for stocks/etfs     (price_daily.py)
  4.  shareprice_intraday for stocks/etfs  (price_intraday.py)
  5.  etf_profile for etfs                 (etf_profile.py)
  6a. insider_df for stocks                (insider.py)
  6b. sentiment_df for stocks              (sentiment.py)
  6c. financials_quarterly/_annually       (financials.py)

Phases 3, 4, 5, 6a, 6b, 6c run in a single per-symbol pass driven by
``frames/stocks_etfs.py`` so the factor frame from Phase 3 flows into
Phase 4 in memory, the shareprice_daily Date axis flows into Phase 6c,
and the saved per-symbol folder always carries every implemented frame.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import settings
from maintainance_scripts.logging_setup import configure_logging

import polars as pl

from data_transformation._common import ASSET_TYPES, TransformationReport
from data_transformation.frames.overview import write_assets_overview
from data_transformation.frames.price_daily import (
    _SIMPLE_DATACLASS,
    _STOCK_ETF_DATACLASS,
    transform_simple_price_daily,
)
from data_transformation.frames.stocks_etfs import transform_stocks_or_etfs

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Transform raw historical/ and daily/ parquet files into per-symbol "
            "AssetData instances under <dest>/<asset_type>/data_<SYMBOL>/."
        ),
    )
    p.add_argument("--catalog-dir", type=Path, default=settings.CATALOG_DIR)
    p.add_argument("--historical-dir", type=Path, default=settings.HISTORICAL_DIR)
    p.add_argument("--daily-dir", type=Path, default=settings.DAILY_DIR)
    p.add_argument("--dest-dir", type=Path, default=settings.TRANSFORMED_DIR)
    p.add_argument(
        "--asset-types",
        nargs="+",
        choices=list(ASSET_TYPES),
        default=None,
        help="Restrict per-symbol transformation to these asset types. "
             "Phase 1 (assets_overview) always covers every catalog regardless.",
    )
    p.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="Restrict per-symbol transformation to these symbols.",
    )
    p.add_argument(
        "--rebuild-stocks",
        action="store_true",
        help=(
            "Wipe <dest>/stocks/ before processing for a clean slate. "
            "Resume granularity is per-symbol-folder, so existing "
            "data_<SYM>/ directories with empty placeholders for a "
            "newly-implemented frame would otherwise be silently "
            "skipped. Transformed data is regenerable from raw "
            "historical/ + daily/, so wiping it is cheap. Common uses: "
            "(1) when a new stock-only frame builder lands "
            "(e.g. financials_quarterly / financials_annually), "
            "(2) to drop accumulated noise from earlier partial runs."
        ),
    )
    p.add_argument(
        "--skip-financials",
        action="store_true",
        help=(
            "Skip the financials builder (Phase 6c) for stocks. The "
            "financials_quarterly / financials_annually frames will be "
            "saved as empty schema-only placeholders. Useful for fast "
            "iteration during dev."
        ),
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    args = parse_args(argv)

    args.dest_dir.mkdir(parents=True, exist_ok=True)

    if args.rebuild_stocks:
        stocks_dir = args.dest_dir / "stocks"
        if stocks_dir.exists():
            logger.info("rebuild-stocks: wiping %s", stocks_dir)
            shutil.rmtree(stocks_dir)

    report = TransformationReport()

    # ---- Phase 1: assets_overview.parquet ---------------------------------
    overview_path = write_assets_overview(args.catalog_dir, args.dest_dir)
    overview = pl.read_parquet(overview_path)

    asset_types_filter = set(args.asset_types) if args.asset_types else None
    symbols_filter = set(args.symbols) if args.symbols else None

    def _wanted(asset_type: str) -> bool:
        return asset_types_filter is None or asset_type in asset_types_filter

    # ---- Phase 2: price_daily for the 5 flat asset types ------------------
    for asset_type in _SIMPLE_DATACLASS:
        if not _wanted(asset_type):
            continue
        n = transform_simple_price_daily(
            asset_type,
            args.historical_dir,
            args.daily_dir,
            args.dest_dir,
            overview,
            report,
            symbols_filter=symbols_filter,
        )
        logger.info("phase2 %s: processed %d symbols", asset_type, n)

    # ---- Phases 3 + 4 (+ 5 for etfs): combined per-symbol pipeline -------
    for asset_type in _STOCK_ETF_DATACLASS:
        if not _wanted(asset_type):
            continue
        n = transform_stocks_or_etfs(
            asset_type,
            args.historical_dir,
            args.daily_dir,
            args.dest_dir,
            overview,
            report,
            symbols_filter=symbols_filter,
            skip_financials=args.skip_financials,
        )
        logger.info(
            "stocks/etfs phase %s: processed %d symbols", asset_type, n,
        )

    # ---- Phase 5 (etf_profile) and Phase 6 (stock-only frames): TODO -----

    report_path = report.flush(args.dest_dir)
    logger.info("wrote %s rows=%d", report_path, report.to_frame().height)
    return 0


if __name__ == "__main__":
    sys.exit(main())
