"""Build the daily-price frame for every asset type.

Phase 2: the simple ``price_daily`` frame for the five flat asset types
(forex, indices, cryptocurrencies, commodities, economic).

Phase 3: the richer ``shareprice_daily`` frame for stocks and etfs
(adds AdjClose / AdjVolume) plus the in-memory per-date factor frame
that Phase 4 consumes for intraday adjustment.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import polars as pl

from data_transformation._common import (
    TransformationReport,
    build_source_index,
    cast_to_schema,
    is_already_transformed,
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
# Source columns only (the derived AdjClose/AdjVolume are computed AFTER dedup).
_SP_DAILY_DEDUP_COLS: tuple[str, ...] = (
    "Open", "High", "Low", "Close", "Volume",
    "DividendAmount", "SplitCoefficient",
)

# Schema-Float32 columns of shareprice_daily that must all be non-null in
# the saved frame. Rows with any null among these are dropped.
_SP_DAILY_REQUIRED_FLOAT_COLS: tuple[str, ...] = (
    "Open", "High", "Low", "Close", "AdjClose",
    "Volume", "AdjVolume",
    "DividendAmount", "SplitCoefficient",
)

FACTOR_FRAME_SCHEMA: dict = {
    "Date": pl.Date,
    "adj_factor": pl.Float32,
    "cum_split": pl.Float32,
}


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
        if is_already_transformed(dest_dir, asset_type, symbol):
            n_processed += 1
            continue

        try:
            _build_one_symbol(
                asset_type, symbol, about_lookup[symbol], cls,
                src_index[symbol], dest_dir, report,
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
) -> None:
    frames: list[pl.DataFrame] = []
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
        )
        df = _drop_null_ohlc(merged, symbol, asset_type, report)
        df = cast_to_schema(df, SCHEMAS["price_daily"], "price_daily")

    inst = cls.default_instance()
    inst.ticker = symbol
    inst.about = about
    inst.price_daily = df
    inst.save_to(symbol_dest_dir(dest_dir, asset_type, symbol))


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
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build the ``shareprice_daily`` frame and the per-date factor frame
    for one stocks/etfs symbol.

    Returns ``(shareprice_daily, factor_frame)``.

    ``factor_frame`` schema: ``{Date, adj_factor, cum_split}``. ``adj_factor``
    is ``AdjClose / Close`` (i.e. the dividend-adjustment multiplier applied
    to OHLC); ``cum_split`` is the volume-side multiplier. Both are aligned
    to the dates that survived to ``shareprice_daily`` (rows dropped due to
    null Float32s do not appear).
    """
    empty_sp = pl.DataFrame(schema=SCHEMAS["shareprice_daily"])
    empty_factor = pl.DataFrame(schema=FACTOR_FRAME_SCHEMA)
    if not paths:
        return empty_sp, empty_factor

    frames: list[pl.DataFrame] = []
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
        return empty_sp, empty_factor

    merged = attach_source_order(frames)
    merged = dedup_with_discrepancy_log(
        merged, "Date", _SP_DAILY_DEDUP_COLS, report,
        symbol, asset_type, "shareprice_daily",
    )

    cum_split, div_factor = _compute_adjustment_factors(merged)
    merged = merged.with_columns(
        pl.Series("_cum_split", cum_split, dtype=pl.Float32),
        pl.Series("_div_factor", div_factor, dtype=pl.Float32),
    )
    merged = merged.with_columns(
        (pl.col("Close") * pl.col("_div_factor"))
            .cast(pl.Float32).alias("AdjClose"),
        (pl.col("Volume") * pl.col("_cum_split"))
            .cast(pl.Float32).alias("AdjVolume"),
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

    factor_frame = merged.select(
        pl.col("Date"),
        pl.col("_div_factor").cast(pl.Float32).alias("adj_factor"),
        pl.col("_cum_split").cast(pl.Float32).alias("cum_split"),
    )

    sp_daily = merged.drop("_cum_split", "_div_factor")
    sp_daily = cast_to_schema(sp_daily, SCHEMAS["shareprice_daily"], "shareprice_daily")
    return sp_daily, factor_frame


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


def _compute_adjustment_factors(
    df: pl.DataFrame,
) -> tuple[np.ndarray, np.ndarray]:
    """Compute ``(cum_split, div_factor)`` as 1-D ``np.float64`` arrays of
    length ``df.height``. *df* must be sorted ascending by ``Date`` and
    contain ``Close``, ``DividendAmount``, ``SplitCoefficient`` columns.

    Conventions (matching the README's Phase 3 spec):
      cum_split[t]  = product of SplitCoefficient[i] for i > t   (FUTURE splits)
      div_step[t]   = (Close[t] - DividendAmount[t+1]) / Close[t]   for t < n-1
                      1.0                                            for t = n-1
      div_factor[t] = product of div_step[i] for i in [t, n-1]   (FUTURE divs)

    Null SplitCoefficient is treated as 1.0 (no split); null DividendAmount
    as 0.0 (no dividend); a null or non-positive Close skips the dividend
    adjustment for that step (factor 1.0). The schema-level null-row drop
    runs *after* this function, so any null-Close row is removed before
    output regardless.
    """
    n = df.height
    if n == 0:
        return np.array([], dtype=np.float64), np.array([], dtype=np.float64)

    closes = df["Close"].to_numpy().astype(np.float64, copy=False)
    divs = df["DividendAmount"].fill_null(0.0).to_numpy().astype(np.float64, copy=False)
    scs = df["SplitCoefficient"].fill_null(1.0).to_numpy().astype(np.float64, copy=False)

    sc_step = np.empty(n, dtype=np.float64)
    sc_step[:-1] = scs[1:]
    sc_step[-1] = 1.0
    cum_split = np.flip(np.cumprod(np.flip(sc_step)))

    div_step = np.ones(n, dtype=np.float64)
    if n >= 2:
        denom = closes[:-1]
        nxt_div = divs[1:]
        with np.errstate(divide="ignore", invalid="ignore"):
            step = (denom - nxt_div) / denom
        step = np.where(np.isfinite(step) & (denom > 0), step, 1.0)
        div_step[:-1] = step
    div_factor = np.flip(np.cumprod(np.flip(div_step)))

    return cum_split, div_factor
