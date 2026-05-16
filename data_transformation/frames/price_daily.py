"""Build the daily-price frame for every asset type.

Phase 2: the simple ``price_daily`` frame for the five flat asset types
(forex, indices, cryptocurrencies, commodities, economic).

Phase 3: the richer ``shareprice_daily`` frame for stocks and etfs
(adds the single-day ``AdjFactor`` multiplier alongside raw OHLCV).
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import numpy as np
import polars as pl

from data_transformation._common import (
    TransformationReport,
    build_source_index,
    cast_to_schema,
    paths_for_mode,
    resolve_mode,
    symbol_dest_dir,
)
from data_transformation.AssetData import (
    CommoditiesData,
    CryptocurrenciesData,
    EconomicData,
    ETFData,
    ForexData,
    IndexData,
    StockData,
)
from data_transformation.AssetDataService import SCHEMAS
from data_transformation.frames._dedup import (
    attach_source_order,
    dedup_with_discrepancy_log,
)

logger = logging.getLogger(__name__)


# Asset-type -> dataclass for the simple price_daily group.
_SIMPLE_DATACLASS = {
    "forex": ForexData,
    "indices": IndexData,
    "cryptocurrencies": CryptocurrenciesData,
    "commodities": CommoditiesData,
    "economic": EconomicData,
}

# Asset-type -> dataclass for the stocks/etfs (shareprice_daily) group.
_STOCK_ETF_DATACLASS = {
    "stocks": StockData,
    "etfs": ETFData,
}

_OHLC_COLS: tuple[str, ...] = ("Open", "High", "Low", "Close")
_PRICE_FLOAT_COLS: tuple[str, ...] = ("Open", "High", "Low", "Close", "Volume")

# Float columns considered for dedup discrepancy logging on shareprice_daily.
# Source columns only (the derived AdjFactor is computed AFTER dedup).
_SP_DAILY_DEDUP_COLS: tuple[str, ...] = (
    "Open", "High", "Low", "Close", "Volume",
    "DividendAmount", "SplitCoefficient",
)

# Schema-Float32 columns of shareprice_daily that must all be non-null in
# the saved frame. Rows with any null among these are dropped.
_SP_DAILY_REQUIRED_FLOAT_COLS: tuple[str, ...] = (
    "Open", "High", "Low", "Close", "Volume",
    "DividendAmount", "SplitCoefficient", "AdjFactor",
)


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------

def transform_simple_price_daily(
    asset_type: str,
    historical_dir: Path,
    daily_dir: Path,
    dest_dir: Path,
    overview: pl.DataFrame,
    report: TransformationReport,
    symbols_filter: set[str] | None = None,
    last_processed_daily_date: date | None = None,
    all_daily_dates: list[date] | None = None,
) -> int:
    """Transform ``price_daily`` for one of the flat asset types.

    Iterates every symbol with at least one source file under
    ``historical/<asset_type>/`` or ``daily/*/<asset_type>/``, builds the
    corresponding dataclass instance, and writes it to
    ``<dest>/<asset_type>/data_<SYMBOL>/`` via ``save_to``.

    *overview* must be the assets_overview frame (used for the symbol's
    ``about`` field). *symbols_filter* (if given) restricts processing.

    Returns the number of symbols processed (newly written + already
    transformed and skipped).
    """
    if asset_type not in _SIMPLE_DATACLASS:
        raise ValueError(
            f"transform_simple_price_daily does not handle asset_type={asset_type!r}"
        )
    cls = _SIMPLE_DATACLASS[asset_type]

    about_lookup = dict(
        overview.filter(pl.col("assetType") == asset_type)
        .select("symbol", "about")
        .iter_rows()
    )

    src_index = build_source_index(
        historical_dir, daily_dir, asset_type, endpoint=None
    )
    daily_dates_for_dispatch = (
        all_daily_dates if all_daily_dates is not None else []
    )

    n_processed = 0
    for symbol in sorted(src_index.keys()):
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

        existing_frame: pl.DataFrame | None = None
        if mode == "incremental":
            existing_path = sym_dest / "price_daily.parquet"
            if existing_path.exists():
                try:
                    existing_frame = pl.read_parquet(existing_path)
                except Exception as exc:
                    logger.warning(
                        "%s/%s: failed to load existing price_daily -> "
                        "fresh build: %s", asset_type, symbol, exc,
                    )
                    existing_frame = None
                    mode = "fresh"
                    since_date = None
            else:
                # No existing parquet to merge against; treat as fresh.
                mode = "fresh"
                since_date = None

        try:
            filtered_paths = paths_for_mode(
                src_index[symbol], mode, since_date,
            )
            _build_one_symbol(
                asset_type, symbol, about_lookup[symbol], cls,
                filtered_paths, dest_dir, report,
                last_processed_daily_date=last_processed_daily_date,
                existing=existing_frame,
            )
            n_processed += 1
        except Exception as exc:
            logger.exception("%s/%s: build failed", asset_type, symbol)
            report.record(
                symbol, asset_type, "price_daily", "schema_cast_failure",
                count=1, detail=str(exc)[:200],
            )

    return n_processed


# ---------------------------------------------------------------------------
# Per-symbol pipeline
# ---------------------------------------------------------------------------

def _build_one_symbol(
    asset_type: str,
    symbol: str,
    about: str,
    cls,
    paths: list[Path],
    dest_dir: Path,
    report: TransformationReport,
    *,
    last_processed_daily_date: date | None = None,
    existing: pl.DataFrame | None = None,
) -> None:
    """Build and save one flat-asset-type symbol.

    Incremental mode: when *existing* is the previous run's
    ``price_daily`` frame, *paths* must contain only the *new* daily
    files (filtered upstream via
    :func:`data_transformation._common.paths_for_mode`). Existing is
    attached as the earliest source and ``keep="last"`` lets the new
    daily values restate overlapping dates;
    ``suppress_historic_boundary`` is disabled in this mode.
    """
    incremental = existing is not None

    frames: list[pl.DataFrame] = []
    if incremental and not existing.is_empty():
        frames.append(existing)

    for p in paths:
        try:
            raw = pl.read_parquet(p)
        except Exception as exc:
            logger.warning("%s/%s: failed to read %s: %s", asset_type, symbol, p, exc)
            continue
        try:
            frames.append(_normalize_simple_source(asset_type, raw))
        except Exception as exc:
            logger.warning(
                "%s/%s: failed to normalize %s: %s",
                asset_type, symbol, p, exc,
            )

    if not frames:
        df = pl.DataFrame(schema=SCHEMAS["price_daily"])
    else:
        merged = attach_source_order(frames)
        merged = dedup_with_discrepancy_log(
            merged, "Date", _PRICE_FLOAT_COLS, report,
            symbol, asset_type, "price_daily",
            keep="last",
            suppress_historic_boundary=not incremental,
        )
        df = _drop_null_ohlc(merged, symbol, asset_type, report)
        df = cast_to_schema(df, SCHEMAS["price_daily"], "price_daily")

    inst = cls.default_instance()
    inst.ticker = symbol
    inst.about = about
    inst.price_daily = df
    inst.save_to(
        symbol_dest_dir(dest_dir, asset_type, symbol),
        last_processed_daily_date=last_processed_daily_date,
    )


def _drop_null_ohlc(
    df: pl.DataFrame,
    symbol: str,
    asset_type: str,
    report: TransformationReport,
) -> pl.DataFrame:
    """Drop rows where any OHLC column is null. Volume nulls are preserved
    (forex/indices/commodities have no volume by design)."""
    if df.height == 0:
        return df
    null_mask = pl.any_horizontal([pl.col(c).is_null() for c in _OHLC_COLS])
    before = df.height
    out = df.filter(~null_mask)
    dropped = before - out.height
    if dropped:
        report.record(
            symbol, asset_type, "price_daily", "dedup_dropped_null_row",
            count=dropped,
            relative=dropped / before,
            detail=f"{dropped} of {before} rows had null OHLC",
        )
    return out


# ---------------------------------------------------------------------------
# Source schema normalization
# ---------------------------------------------------------------------------

def _normalize_simple_source(asset_type: str, df: pl.DataFrame) -> pl.DataFrame:
    """Project a raw per-source frame onto the canonical
    ``(Date, Open, High, Low, Close, Volume)`` shape used downstream.

    forex / indices: no Volume in source -> filled with null.
    cryptocurrencies: full OHLCV in source.
    commodities / economic: ``value`` in source -> broadcast to OHLC,
    ``unit`` (commodities) discarded, Volume null.
    """
    if asset_type in {"forex", "indices"}:
        return df.select(
            pl.col("Date").cast(pl.Date),
            pl.col("Open").cast(pl.Float32),
            pl.col("High").cast(pl.Float32),
            pl.col("Low").cast(pl.Float32),
            pl.col("Close").cast(pl.Float32),
            pl.lit(None, dtype=pl.Float32).alias("Volume"),
        )
    if asset_type == "cryptocurrencies":
        return df.select(
            pl.col("Date").cast(pl.Date),
            pl.col("Open").cast(pl.Float32),
            pl.col("High").cast(pl.Float32),
            pl.col("Low").cast(pl.Float32),
            pl.col("Close").cast(pl.Float32),
            pl.col("Volume").cast(pl.Float32),
        )
    if asset_type in {"commodities", "economic"}:
        close = pl.col("value").cast(pl.Float32)
        return df.select(
            pl.col("Date").cast(pl.Date),
            close.alias("Open"),
            close.alias("High"),
            close.alias("Low"),
            close.alias("Close"),
            pl.lit(None, dtype=pl.Float32).alias("Volume"),
        )
    raise ValueError(f"_normalize_simple_source: unsupported asset_type={asset_type!r}")


# ===========================================================================
# Phase 3: shareprice_daily for stocks and etfs
#
# The per-symbol orchestrator that drives Phases 3, 4, and (for etfs) 5
# in a single pass lives in ``frames/stocks_etfs.py`` so the factor frame
# stays in memory between phases. This file only exposes
# ``build_shareprice_daily``.
# ===========================================================================

def build_shareprice_daily(
    asset_type: str,
    symbol: str,
    paths: list[Path],
    report: TransformationReport,
    *,
    existing: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Build the ``shareprice_daily`` frame for one stocks/etfs symbol.

    OHLCV columns are kept raw and unadjusted. A single-day
    ``AdjFactor`` column is added; consumers compute any cumulative
    adjusted series themselves. See ``AssetData_design_choices.md``
    section 6 for the formula.

    Incremental mode: when *existing* is the previous run's saved frame
    (loaded from ``shareprice_daily.parquet``), *paths* must contain
    only the *new* daily files (use
    :func:`data_transformation._common.paths_for_mode` to filter). The
    existing frame is treated as the earliest source order so ``keep="last"``
    lets the new daily values restate overlapping dates, and ``AdjFactor``
    is stripped from existing and recomputed across the full merged frame.
    ``suppress_historic_boundary`` is disabled in this mode -- the
    existing frame's tail is never a partial bar.
    """
    empty_sp = pl.DataFrame(schema=SCHEMAS["shareprice_daily"])
    incremental = existing is not None

    if incremental and not paths:
        # Nothing new to merge -> return existing as-is.
        return existing if not existing.is_empty() else empty_sp
    if not paths and not incremental:
        return empty_sp

    frames: list[pl.DataFrame] = []
    if incremental and not existing.is_empty():
        # Strip AdjFactor (recomputed below) so the schema lines up with
        # the normalized daily-source frames.
        ex = (
            existing.drop("AdjFactor")
            if "AdjFactor" in existing.columns
            else existing
        )
        frames.append(ex)

    for p in paths:
        try:
            raw = pl.read_parquet(p)
        except Exception as exc:
            logger.warning(
                "%s/%s: failed to read %s: %s", asset_type, symbol, p, exc,
            )
            continue
        try:
            frames.append(_normalize_stock_etf_source(raw))
        except Exception as exc:
            logger.warning(
                "%s/%s: failed to normalize %s: %s",
                asset_type, symbol, p, exc,
            )

    if not frames:
        return existing if incremental and not existing.is_empty() else empty_sp

    merged = attach_source_order(frames)
    merged = dedup_with_discrepancy_log(
        merged, "Date", _SP_DAILY_DEDUP_COLS, report,
        symbol, asset_type, "shareprice_daily",
        keep="last",
        # Historic-boundary suppression is only meaningful for the
        # fresh path (the historic-source partial bar). In incremental
        # mode the existing frame's tail was already produced by a
        # completed daily run, never partial.
        suppress_historic_boundary=not incremental,
    )

    adj_factor = _compute_adj_factor(merged)
    merged = merged.with_columns(
        pl.Series("AdjFactor", adj_factor, dtype=pl.Float32),
    )

    null_mask = pl.any_horizontal(
        [pl.col(c).is_null() for c in _SP_DAILY_REQUIRED_FLOAT_COLS]
    )
    before = merged.height
    merged = merged.filter(~null_mask)
    dropped = before - merged.height
    if dropped:
        report.record(
            symbol, asset_type, "shareprice_daily",
            "dedup_dropped_null_row",
            count=dropped,
            relative=dropped / before,
            detail=(
                f"{dropped} of {before} rows had null in a schema "
                "Float32 column"
            ),
        )

    return cast_to_schema(merged, SCHEMAS["shareprice_daily"], "shareprice_daily")


def _normalize_stock_etf_source(df: pl.DataFrame) -> pl.DataFrame:
    """Project the raw prices_daily frame onto the canonical 8-column shape
    ``(Date, Open, High, Low, Close, Volume, DividendAmount, SplitCoefficient)``
    used by Phase 3."""
    return df.select(
        pl.col("Date").cast(pl.Date),
        pl.col("Open").cast(pl.Float32),
        pl.col("High").cast(pl.Float32),
        pl.col("Low").cast(pl.Float32),
        pl.col("Close").cast(pl.Float32),
        pl.col("Volume").cast(pl.Float32),
        pl.col("DividendAmount").cast(pl.Float32),
        pl.col("SplitCoefficient").cast(pl.Float32),
    )


def _compute_adj_factor(df: pl.DataFrame) -> np.ndarray:
    """Compute the single-day ``AdjFactor`` series as a 1-D ``np.float64``
    array of length ``df.height``. *df* must be sorted ascending by
    ``Date`` and contain ``Close``, ``DividendAmount``,
    ``SplitCoefficient`` columns.

    Formula (CRSP-style, anchored on the prior close):
      AdjFactor[i] = SplitCoefficient[i] * Close[i-1] / (Close[i-1] - DividendAmount[i])
                                                                     for i >= 1
      AdjFactor[0] = 1.0

    Null SplitCoefficient is treated as 1.0 (no split); null
    DividendAmount as 0.0 (no dividend). A null or non-positive prior
    Close, or a denominator that collapses to <= 0, falls back to the
    split coefficient alone (no dividend correction for that step).
    The schema-level null-row drop in ``build_shareprice_daily`` runs
    *after* this function.
    """
    n = df.height
    if n == 0:
        return np.array([], dtype=np.float64)

    closes = df["Close"].to_numpy().astype(np.float64, copy=False)
    divs = df["DividendAmount"].fill_null(0.0).to_numpy().astype(np.float64, copy=False)
    scs = df["SplitCoefficient"].fill_null(1.0).to_numpy().astype(np.float64, copy=False)

    out = np.ones(n, dtype=np.float64)
    if n >= 2:
        prev_close = closes[:-1]
        cur_div = divs[1:]
        cur_sc = scs[1:]
        denom = prev_close - cur_div
        with np.errstate(divide="ignore", invalid="ignore"):
            ratio = prev_close / denom
        valid = np.isfinite(ratio) & np.isfinite(prev_close) & (prev_close > 0) & (denom > 0)
        out[1:] = cur_sc * np.where(valid, ratio, 1.0)
    return out
