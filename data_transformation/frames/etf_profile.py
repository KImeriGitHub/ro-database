"""Phase 5: etf_profile for ETFs.

Driven from ``frames/stocks_etfs.py``'s combined orchestrator after
Phases 3 and 4 for the same symbol. The historical etf_profile parquet
contributes a single row dated to the historical run's data-complete
date; each daily folder contributes one row per day. The resulting
frame is sparse in time - consumers must treat absent dates as "no
profile snapshot taken".
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


# Float32 columns the dedup helper compares for value discrepancies.
_PROFILE_FLOAT_COLS: tuple[str, ...] = (
    "information_technology", "communication_services",
    "consumer_discretionary", "consumer_staples",
    "healthcare", "industrials", "utilities", "materials",
    "energy", "financials", "real_estate", "other",
    "net_assets", "net_expense_ratio", "portfolio_turnover",
    "dividend_yield",
)


def build_etf_profile(
    symbol: str,
    paths: list[Path],
    report: TransformationReport,
) -> pl.DataFrame:
    """Build the ``etf_profile`` frame for one ETF symbol.

    Returns an empty schema-correct frame when no usable data is
    available (no source files, all reads failed, or every source had
    its `date` / `holdings` column missing).

    Source schema -> target schema mapping:
      date (lowercase)   -> Date
      inception_date     -> dropped (absent from target schema)
      leveraged (Utf8)   -> Categorical (via SCHEMAS cast)
      holdings           -> kept as List(Struct) verbatim
      sector weights & scalar Float32s -> kept verbatim
    """
    empty = pl.DataFrame(schema=SCHEMAS["etf_profile"])
    if not paths:
        return empty

    frames: list[pl.DataFrame] = []
    for p in paths:
        try:
            raw = pl.read_parquet(p)
        except Exception as exc:
            logger.warning(
                "etfs/%s: failed to read etf_profile %s: %s",
                symbol, p, exc,
            )
            continue
        try:
            frames.append(_normalize_profile_source(raw))
        except Exception as exc:
            logger.warning(
                "etfs/%s: failed to normalize etf_profile %s: %s",
                symbol, p, exc,
            )

    if not frames:
        return empty

    merged = attach_source_order(frames)
    merged = dedup_with_discrepancy_log(
        merged, "Date", _PROFILE_FLOAT_COLS, report,
        symbol, "etfs", "etf_profile",
    )
    return cast_to_schema(merged, SCHEMAS["etf_profile"], "etf_profile")


def _normalize_profile_source(df: pl.DataFrame) -> pl.DataFrame:
    """Project the raw etf_profile frame onto the canonical target shape.

    The ``leveraged`` column stays Utf8 here; the final
    ``cast_to_schema`` step in :func:`build_etf_profile` performs the
    Utf8 -> Categorical conversion. Keeping it Utf8 through the dedup
    avoids Categorical category-merge complications during
    ``pl.concat`` of source frames whose categories may differ.
    """
    columns: list[pl.Expr] = []

    if "date" in df.columns:
        columns.append(pl.col("date").cast(pl.Date).alias("Date"))
    elif "Date" in df.columns:
        columns.append(pl.col("Date").cast(pl.Date))
    else:
        raise KeyError("etf_profile source missing 'date'/'Date' column")

    for c in _PROFILE_FLOAT_COLS:
        if c in df.columns:
            columns.append(pl.col(c).cast(pl.Float32))
        else:
            columns.append(pl.lit(None, dtype=pl.Float32).alias(c))

    if "holdings" not in df.columns:
        raise KeyError("etf_profile source missing 'holdings' column")
    columns.append(pl.col("holdings"))

    if "leveraged" in df.columns:
        columns.append(pl.col("leveraged").cast(pl.Utf8))
    else:
        columns.append(pl.lit(None, dtype=pl.Utf8).alias("leveraged"))

    return df.select(columns)
