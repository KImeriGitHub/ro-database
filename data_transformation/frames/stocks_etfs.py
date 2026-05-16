"""Combined per-symbol orchestrator for stocks and etfs.

Drives Phase 3 (shareprice_daily) and Phase 4 (shareprice_intraday) - and,
once landed, Phase 5 (etf_profile) - in a single per-symbol pass so the
``shareprice_daily.Date`` axis produced by Phase 3 flows directly into
Phase 4's orphan-date check and Phase 6c without touching disk.

For the simpler asset types (forex, indices, cryptocurrencies, commodities,
economic), see ``frames/price_daily.py``'s
``transform_simple_price_daily`` instead.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import polars as pl

from data_transformation._common import (
    TransformationReport,
    build_source_index,
    paths_for_mode,
    resolve_mode,
    sector_to_index,
    symbol_dest_dir,
)
from data_transformation.AssetData import ETFData, StockData
from data_transformation.frames.etf_profile import build_etf_profile
from data_transformation.frames.financials import (
    ENDPOINTS as _FIN_ENDPOINTS,
    SUFFIXES as _FIN_SUFFIXES,
    _build_earnings_calendar_index,
    build_financials,
    build_financials_incremental,
)
from data_transformation.frames.insider import build_insider_df
from data_transformation.frames.price_daily import build_shareprice_daily
from data_transformation.frames.price_intraday import build_shareprice_intraday
from data_transformation.frames.sentiment import build_sentiment_df

logger = logging.getLogger(__name__)


_DATACLASS_BY_ASSET_TYPE = {
    "stocks": StockData,
    "etfs": ETFData,
}


def _load_cached_rt(sym_dir: Path, suffix: str) -> pl.DataFrame | None:
    """Load ``report_table_<suffix>.parquet`` from a per-symbol folder.

    Returns ``None`` when the file is absent or lacks the ``_source``
    column (an older-schema cache from before the incremental work
    landed). In both cases the caller falls back to the fresh-mode
    financials builder for that symbol so correctness is preserved.
    """
    path = sym_dir / f"report_table_{suffix}.parquet"
    if not path.exists():
        return None
    try:
        df = pl.read_parquet(path)
    except Exception as exc:
        logger.warning("failed to read cached report_table %s: %s", path, exc)
        return None
    if "_source" not in df.columns:
        return None
    return df


def transform_stocks_or_etfs(
    asset_type: str,
    historical_dir: Path,
    daily_dir: Path,
    dest_dir: Path,
    overview: pl.DataFrame,
    report: TransformationReport,
    symbols_filter: set[str] | None = None,
    skip_financials: bool = False,
    last_processed_daily_date: date | None = None,
    all_daily_dates: list[date] | None = None,
) -> int:
    """Build per-symbol StockData / ETFData with every implemented frame
    populated before the dataclass is saved.

    For ``asset_type == "stocks"``: shareprice_daily, shareprice_intraday,
    insider_df, and sentiment_df are built in one pass. financials_quarterly
    and financials_annually remain empty schema-correct frames until their
    builders land.

    For ``asset_type == "etfs"``: shareprice_daily, shareprice_intraday,
    and etf_profile are built in one pass.

    Iterates the union of symbols present under any of the relevant
    source endpoints (prices_daily, prices, and per asset type:
    etf_profile / insider / sentiment). A symbol is skipped when its
    dest directory already has ``metadata.json`` (resume behaviour) or
    it does not appear in ``assets_overview.parquet``.

    Returns the count of symbols processed (newly written + already
    transformed and skipped).
    """
    if asset_type not in _DATACLASS_BY_ASSET_TYPE:
        raise ValueError(
            f"transform_stocks_or_etfs does not handle asset_type={asset_type!r}"
        )
    cls = _DATACLASS_BY_ASSET_TYPE[asset_type]

    overview_filt = overview.filter(pl.col("assetType") == asset_type)
    about_lookup = dict(overview_filt.select("symbol", "about").iter_rows())
    sector_lookup: dict[str, str] = (
        dict(overview_filt.select("symbol", "sector").iter_rows())
        if asset_type == "stocks"
        else {}
    )
    overview_row_lookup: dict[str, dict] = (
        {
            row["symbol"]: {
                "reportedDate": row.get("reportedDate"),
                "timeOfTheDay": row.get("timeOfTheDay"),
            }
            for row in overview_filt.iter_rows(named=True)
        }
        if asset_type == "stocks"
        else {}
    )

    daily_idx = build_source_index(
        historical_dir, daily_dir, asset_type, "prices_daily",
    )
    intraday_idx = build_source_index(
        historical_dir, daily_dir, asset_type, "prices",
    )
    profile_idx = (
        build_source_index(historical_dir, daily_dir, asset_type, "etf_profile")
        if asset_type == "etfs"
        else {}
    )
    insider_idx = (
        build_source_index(historical_dir, daily_dir, asset_type, "insider")
        if asset_type == "stocks"
        else {}
    )
    sentiment_idx = (
        build_source_index(historical_dir, daily_dir, asset_type, "sentiment")
        if asset_type == "stocks"
        else {}
    )
    # Build 10 financial-source indexes (5 endpoints x 2 suffixes) once.
    fin_idx: dict[tuple[str, str], dict[str, list[Path]]] = {}
    # Scan daily/*/earnings_calendar.parquet once for the per-symbol qm0 /
    # am0 PIT gate. Empty maps when the daily tree has none of these files.
    ec_index: dict = {}
    ec_snap_dates: list = []
    if asset_type == "stocks" and not skip_financials:
        for ep in _FIN_ENDPOINTS:
            for suf in _FIN_SUFFIXES:
                fin_idx[(ep, suf)] = build_source_index(
                    historical_dir, daily_dir, asset_type, ep, suffix=suf,
                )
        ec_index, ec_snap_dates = _build_earnings_calendar_index(daily_dir)

    all_symbols = sorted(
        set(daily_idx) | set(intraday_idx) | set(profile_idx)
        | set(insider_idx) | set(sentiment_idx)
        | {s for idx in fin_idx.values() for s in idx}
    )

    daily_dates_for_dispatch = (
        all_daily_dates if all_daily_dates is not None else []
    )

    n_processed = 0
    for symbol in all_symbols:
        if symbols_filter is not None and symbol not in symbols_filter:
            continue
        if symbol not in about_lookup:
            logger.warning(
                "%s/%s: source files exist but no overview entry, skipping",
                asset_type, symbol,
            )
            continue

        sym_dest = symbol_dest_dir(dest_dir, asset_type, symbol)

        # Per-symbol dispatch: skip if cached last_processed_daily_date
        # already covers the newest daily folder; rebuild from scratch
        # if no metadata or the field is null; else incremental append.
        mode, since_date = resolve_mode(sym_dest, daily_dates_for_dispatch)

        if mode == "skip":
            n_processed += 1
            continue

        # Load existing for incremental. If load fails for any reason
        # (corrupt parquet, schema drift), fall back to fresh so we
        # always make forward progress.
        existing_inst = None
        cached_rt_q_df: pl.DataFrame | None = None
        cached_rt_a_df: pl.DataFrame | None = None
        if mode == "incremental":
            try:
                existing_inst = cls.load_from(sym_dest)
            except Exception as exc:
                logger.warning(
                    "%s/%s: failed to load existing -> fresh build: %s",
                    asset_type, symbol, exc,
                )
                existing_inst = None
                mode = "fresh"
                since_date = None
            if mode == "incremental" and asset_type == "stocks" and not skip_financials:
                cached_rt_q_df = _load_cached_rt(sym_dest, "quarterly")
                cached_rt_a_df = _load_cached_rt(sym_dest, "annual")

        try:
            inst = cls.default_instance()
            inst.ticker = symbol
            inst.about = about_lookup[symbol]
            if asset_type == "stocks":
                inst.sector = sector_to_index(sector_lookup.get(symbol, ""))

            sp_daily_paths = paths_for_mode(
                daily_idx.get(symbol, []), mode, since_date,
            )
            sp_daily = build_shareprice_daily(
                asset_type, symbol, sp_daily_paths, report,
                existing=(
                    existing_inst.shareprice_daily if existing_inst else None
                ),
            )
            inst.shareprice_daily = sp_daily

            intraday_paths = paths_for_mode(
                intraday_idx.get(symbol, []), mode, since_date,
            )
            sp_intraday = build_shareprice_intraday(
                asset_type, symbol, intraday_paths,
                sp_daily["Date"], report,
                existing=(
                    existing_inst.shareprice_intraday if existing_inst else None
                ),
            )
            inst.shareprice_intraday = sp_intraday

            rt_q: pl.DataFrame | None = None
            rt_a: pl.DataFrame | None = None

            if asset_type == "etfs":
                profile_paths = paths_for_mode(
                    profile_idx.get(symbol, []), mode, since_date,
                )
                inst.etf_profile = build_etf_profile(
                    symbol, profile_paths, report,
                    existing=(
                        existing_inst.etf_profile if existing_inst else None
                    ),
                )

            if asset_type == "stocks":
                insider_paths = paths_for_mode(
                    insider_idx.get(symbol, []), mode, since_date,
                )
                inst.insider_df = build_insider_df(
                    symbol, insider_paths, report,
                    existing=(
                        existing_inst.insider_df if existing_inst else None
                    ),
                )
                sentiment_paths = paths_for_mode(
                    sentiment_idx.get(symbol, []), mode, since_date,
                )
                inst.sentiment_df = build_sentiment_df(
                    symbol, sentiment_paths, report,
                    existing=(
                        existing_inst.sentiment_df if existing_inst else None
                    ),
                )
                if not skip_financials:
                    ec_for_symbol = {
                        snap: by_sym[symbol]
                        for snap, by_sym in ec_index.items()
                        if symbol in by_sym
                    }
                    # Financials needs historical paths in incremental
                    # mode so per-row PIT lookups can fall back when
                    # the resolved snapshot is missing an endpoint.
                    if (
                        mode == "incremental"
                        and existing_inst is not None
                        and cached_rt_q_df is not None
                        and cached_rt_a_df is not None
                    ):
                        new_source_paths = {
                            key: paths_for_mode(
                                idx.get(symbol, []), mode, since_date,
                                keep_historical=True,
                            )
                            for key, idx in fin_idx.items()
                        }
                        fin_q, fin_a, rt_q, rt_a = build_financials_incremental(
                            symbol, inst.shareprice_daily,
                            overview_row_lookup.get(symbol),
                            new_source_paths,
                            existing_inst.financials_quarterly,
                            existing_inst.financials_annually,
                            cached_rt_q_df, cached_rt_a_df,
                            report,
                            ec_index_for_symbol=ec_for_symbol,
                            ec_snap_dates_sorted=ec_snap_dates,
                        )
                    else:
                        source_paths_sym = {
                            key: idx.get(symbol, []) for key, idx in fin_idx.items()
                        }
                        fin_q, fin_a, rt_q, rt_a = build_financials(
                            symbol, inst.shareprice_daily,
                            overview_row_lookup.get(symbol),
                            source_paths_sym, report,
                            ec_index_for_symbol=ec_for_symbol,
                            ec_snap_dates_sorted=ec_snap_dates,
                        )
                    inst.financials_quarterly = fin_q
                    inst.financials_annually = fin_a

            inst.save_to(
                sym_dest, last_processed_daily_date=last_processed_daily_date,
            )
            if rt_q is not None:
                rt_q.write_parquet(sym_dest / "report_table_quarterly.parquet")
                rt_a.write_parquet(sym_dest / "report_table_annual.parquet")
            del sp_daily, sp_intraday, inst, existing_inst
            n_processed += 1
        except Exception as exc:
            logger.exception(
                "%s/%s: per-symbol transform failed", asset_type, symbol,
            )
            report.record(
                symbol, asset_type, "shareprice_daily",
                "schema_cast_failure",
                count=1, detail=str(exc)[:200],
            )

    return n_processed
