"""Phase 4: shareprice_intraday for stocks and etfs.

Driven from ``frames/stocks_etfs.py``'s combined orchestrator so the
in-memory factor frame produced by Phase 3 (``build_shareprice_daily``)
flows directly into ``build_shareprice_intraday`` without round-tripping
through disk.
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from data_transformation._common import (
    TransformationReport,
    cast_to_schema,
)
from data_transformation.AssetDataService import SCHEMAS
from data_transformation.frames._dedup import (
    attach_source_order,
    dedup_with_discrepancy_log,
)

logger = logging.getLogger(__name__)


_INTRADAY_DEDUP_COLS: tuple[str, ...] = (
    "Open", "High", "Low", "Close", "Volume",
)

_INTRADAY_ADJ_COLS: tuple[str, ...] = (
    "AdjOpen", "AdjHigh", "AdjLow", "AdjClose", "AdjVolume",
)


def build_shareprice_intraday(
    asset_type: str,
    symbol: str,
    paths: list[Path],
    factor_frame: pl.DataFrame,
    report: TransformationReport,
) -> pl.DataFrame:
    """Build the ``shareprice_intraday`` frame for one stocks/etfs symbol.

    *factor_frame* must be the per-date factor frame returned by
    :func:`data_transformation.frames.price_daily.build_shareprice_daily`
    for the same symbol; its rows enumerate every Date that survived the
    Phase 3 null-row drop. Intraday rows whose calendar date is not in
    that frame are dropped as orphans.

    *paths* is the symbol's prices/ source list (historical + every daily
    folder). Returns an empty schema-correct frame when no usable data is
    available.

    Adjustment math:
      AdjOpen/High/Low/Close = OHLC * adj_factor (joined on calendar date)
      AdjVolume              = Volume * cum_split (joined on calendar date)

    Null Adj* fields are *not* dropped (per spec); the count is recorded
    in ``transformation_report`` as ``intraday_null_field``.
    """
    empty = pl.DataFrame(schema=SCHEMAS["shareprice_intraday"])
    if not paths:
        return empty

    frames: list[pl.DataFrame] = []
    for p in paths:
        try:
            raw = pl.read_parquet(p)
        except Exception as exc:
            logger.warning(
                "%s/%s: failed to read intraday %s: %s",
                asset_type, symbol, p, exc,
            )
            continue
        try:
            frames.append(_normalize_intraday_source(raw))
        except Exception as exc:
            logger.warning(
                "%s/%s: failed to normalize intraday %s: %s",
                asset_type, symbol, p, exc,
            )

    if not frames:
        return empty

    merged = attach_source_order(frames)
    merged = dedup_with_discrepancy_log(
        merged, "Datetime", _INTRADAY_DEDUP_COLS, report,
        symbol, asset_type, "shareprice_intraday",
    )

    merged = _drop_orphan_dates(
        merged, factor_frame, symbol, asset_type, report,
    )
    if merged.height == 0:
        return empty

    joined = merged.join(
        factor_frame.rename({"Date": "_date"}),
        on="_date", how="left",
    )
    joined = joined.with_columns(
        (pl.col("Open") * pl.col("adj_factor")).cast(pl.Float32).alias("AdjOpen"),
        (pl.col("High") * pl.col("adj_factor")).cast(pl.Float32).alias("AdjHigh"),
        (pl.col("Low") * pl.col("adj_factor")).cast(pl.Float32).alias("AdjLow"),
        (pl.col("Close") * pl.col("adj_factor")).cast(pl.Float32).alias("AdjClose"),
        (pl.col("Volume") * pl.col("cum_split")).cast(pl.Float32).alias("AdjVolume"),
    )

    _log_null_adj_fields(joined, symbol, asset_type, report)

    return cast_to_schema(joined, SCHEMAS["shareprice_intraday"], "shareprice_intraday")


def _normalize_intraday_source(df: pl.DataFrame) -> pl.DataFrame:
    """Project the raw prices/ frame onto the canonical 6-column shape
    ``(Datetime, Open, High, Low, Close, Volume)``.

    Source column ``Date`` (typed ``pl.Datetime``) is renamed to
    ``Datetime``. A timezone-aware source column (e.g. US/Eastern from
    historical AV pulls) has its tz stripped, keeping the wall-clock
    timestamp - the target schema is timezone-naive ``pl.Datetime`` and
    intraday bars are interpreted as US/Eastern wall-clock by convention.
    """
    dtype = df.schema.get("Date")
    src_expr = pl.col("Date")
    if isinstance(dtype, pl.Datetime) and dtype.time_zone is not None:
        src_expr = src_expr.dt.replace_time_zone(None)
    return df.select(
        src_expr.cast(pl.Datetime("us")).alias("Datetime"),
        pl.col("Open").cast(pl.Float32),
        pl.col("High").cast(pl.Float32),
        pl.col("Low").cast(pl.Float32),
        pl.col("Close").cast(pl.Float32),
        pl.col("Volume").cast(pl.Float32),
    )


def _drop_orphan_dates(
    df: pl.DataFrame,
    factor_frame: pl.DataFrame,
    symbol: str,
    asset_type: str,
    report: TransformationReport,
) -> pl.DataFrame:
    """Drop intraday rows whose calendar date is not in the daily factor
    frame. Records the orphan count and ratio.

    Adds a ``_date`` (pl.Date) column on the surviving rows for the
    downstream factor join.
    """
    if df.height == 0:
        return df.with_columns(pl.lit(None, dtype=pl.Date).alias("_date"))

    df = df.with_columns(pl.col("Datetime").dt.date().alias("_date"))
    total = df.height
    if factor_frame.height == 0:
        # No valid dates at all -> every intraday row is an orphan.
        report.record(
            symbol, asset_type, "shareprice_intraday",
            "intraday_orphan_date_dropped",
            count=total, relative=1.0,
            detail="shareprice_daily had no rows; every intraday bar is orphan",
        )
        return df.head(0)

    kept = df.join(
        factor_frame.select(pl.col("Date").alias("_date")),
        on="_date", how="semi",
    )
    dropped = total - kept.height
    if dropped:
        report.record(
            symbol, asset_type, "shareprice_intraday",
            "intraday_orphan_date_dropped",
            count=dropped,
            relative=dropped / total,
            detail=(
                f"{dropped} of {total} intraday rows had a Datetime "
                "whose date was not present in shareprice_daily"
            ),
        )
    return kept


def _log_null_adj_fields(
    df: pl.DataFrame,
    symbol: str,
    asset_type: str,
    report: TransformationReport,
) -> None:
    n_rows = df.height
    if n_rows == 0:
        return
    null_count = sum(int(df[c].null_count()) for c in _INTRADAY_ADJ_COLS)
    if null_count == 0:
        return
    total_fields = n_rows * len(_INTRADAY_ADJ_COLS)
    report.record(
        symbol, asset_type, "shareprice_intraday",
        "intraday_null_field",
        count=null_count,
        relative=null_count / total_fields,
        detail=(
            f"{null_count} of {total_fields} Adj* fields were null "
            "(rows preserved per spec)"
        ),
    )
