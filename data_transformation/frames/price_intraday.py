"""Phase 4: shareprice_intraday for stocks and etfs.

Driven from ``frames/stocks_etfs.py``'s combined orchestrator so the
``shareprice_daily.Date`` axis produced by Phase 3 flows directly into
the orphan-date check without round-tripping through disk.

OHLCV is kept raw and unadjusted; consumers that need adjusted intraday
returns join ``shareprice_daily.AdjFactor`` on the calendar date of
``Datetime`` themselves.
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


def build_shareprice_intraday(
    asset_type: str,
    symbol: str,
    paths: list[Path],
    daily_dates: pl.Series,
    report: TransformationReport,
) -> pl.DataFrame:
    """Build the ``shareprice_intraday`` frame for one stocks/etfs symbol.

    *daily_dates* is the ``shareprice_daily.Date`` column for the same
    symbol (post null-row drop). Intraday rows whose calendar date is
    not in this set are dropped as orphans.

    *paths* is the symbol's prices/ source list (historical + every daily
    folder). Returns an empty schema-correct frame when no usable data is
    available.

    Output is raw OHLCV; no adjustment is applied here. Null OHLCV
    fields are *not* dropped (per spec); the count is recorded in
    ``transformation_report`` as ``intraday_null_field``.
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
        keep="last",
        suppress_historic_boundary=True,
    )

    merged = _drop_orphan_dates(
        merged, daily_dates, symbol, asset_type, report,
    )
    if merged.height == 0:
        return empty

    _log_null_ohlcv_fields(merged, symbol, asset_type, report)

    return cast_to_schema(merged, SCHEMAS["shareprice_intraday"], "shareprice_intraday")


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
    daily_dates: pl.Series,
    symbol: str,
    asset_type: str,
    report: TransformationReport,
) -> pl.DataFrame:
    """Drop intraday rows whose calendar date is not in *daily_dates*
    (the ``shareprice_daily.Date`` column for the same symbol). Records
    the orphan count and ratio.
    """
    if df.height == 0:
        return df

    df = df.with_columns(pl.col("Datetime").dt.date().alias("_date"))
    total = df.height
    if daily_dates.len() == 0:
        # No valid dates at all -> every intraday row is an orphan.
        report.record(
            symbol, asset_type, "shareprice_intraday",
            "intraday_orphan_date_dropped",
            count=total, relative=1.0,
            detail="shareprice_daily had no rows; every intraday bar is orphan",
        )
        return df.head(0).drop("_date")

    kept = df.join(
        daily_dates.to_frame().rename({daily_dates.name: "_date"}),
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
    return kept.drop("_date")


_OHLCV_COLS: tuple[str, ...] = ("Open", "High", "Low", "Close", "Volume")


def _log_null_ohlcv_fields(
    df: pl.DataFrame,
    symbol: str,
    asset_type: str,
    report: TransformationReport,
) -> None:
    n_rows = df.height
    if n_rows == 0:
        return
    null_count = sum(int(df[c].null_count()) for c in _OHLCV_COLS)
    if null_count == 0:
        return
    total_fields = n_rows * len(_OHLCV_COLS)
    report.record(
        symbol, asset_type, "shareprice_intraday",
        "intraday_null_field",
        count=null_count,
        relative=null_count / total_fields,
        detail=(
            f"{null_count} of {total_fields} OHLCV fields were null "
            "(rows preserved per spec)"
        ),
    )
