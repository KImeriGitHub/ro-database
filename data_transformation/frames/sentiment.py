"""Phase 6b: sentiment_df for stocks.

Builds the news-sentiment frame from
``historical/stocks/sentiment/`` and ``daily/*/stocks/sentiment/``.
The transformed schema retains only the numeric scores plus
``Datetime``; titles, urls, authors, banners, source labels, and
sentiment-label strings are dropped at the cast step.

Driven from ``frames/stocks_etfs.py``'s combined per-symbol
orchestrator so the saved instance carries every implemented frame.
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


# Float32 score columns the dedup helper compares for value
# discrepancies, and that the final frame retains.
_SENTIMENT_FLOAT_COLS: tuple[str, ...] = (
    "ticker_relevance_score",
    "ticker_sentiment_score",
    "overall_sentiment_score",
    "blockchain", "earnings", "ipo", "mergers_and_acquisitions",
    "financial_markets", "economy_fiscal", "economy_monetary",
    "economy_macro", "energy_transportation", "finance",
    "life_sciences", "manufacturing", "real_estate",
    "retail_wholesale", "technology",
)

_SENTIMENT_DEDUP_KEYS: list[str] = ["Datetime", "url"]


def build_sentiment_df(
    symbol: str,
    paths: list[Path],
    report: TransformationReport,
) -> pl.DataFrame:
    """Build the ``sentiment_df`` frame for one stock symbol.

    Returns an empty schema-correct frame when no usable data is
    available.
    """
    empty = pl.DataFrame(schema=SCHEMAS["sentiment_df"])
    if not paths:
        return empty

    frames: list[pl.DataFrame] = []
    for p in paths:
        try:
            raw = pl.read_parquet(p)
        except Exception as exc:
            logger.warning(
                "stocks/%s: failed to read sentiment %s: %s", symbol, p, exc,
            )
            continue
        try:
            frames.append(_normalize_sentiment_source(symbol, raw))
        except Exception as exc:
            logger.warning(
                "stocks/%s: failed to normalize sentiment %s: %s",
                symbol, p, exc,
            )

    if not frames:
        return empty

    merged = attach_source_order(frames)
    merged = dedup_with_discrepancy_log(
        merged, _SENTIMENT_DEDUP_KEYS, _SENTIMENT_FLOAT_COLS, report,
        symbol, "stocks", "sentiment_df",
        flag_under_1pct=False,
    )
    merged = merged.sort("Datetime")
    return cast_to_schema(merged, SCHEMAS["sentiment_df"], "sentiment_df")


def _normalize_sentiment_source(symbol: str, df: pl.DataFrame) -> pl.DataFrame:
    """Project the raw sentiment parquet onto the canonical shape used by
    dedup: ``(Datetime, url, <19 Float32 score cols>)``.

    A defensive ``ticker == symbol`` filter runs first if the source
    carries a ``ticker`` column (the per-symbol files already filter
    upstream, but the column may or may not be present per the spec).
    Missing Float32 score columns are filled with null. The ``url``
    column is kept here purely as a dedup tie-breaker; the final cast
    drops it.
    """
    if "time_published" not in df.columns:
        raise KeyError("sentiment source missing 'time_published' column")

    if "ticker" in df.columns:
        df = df.filter(pl.col("ticker") == symbol)

    columns: list[pl.Expr] = [
        pl.col("time_published").cast(pl.Datetime("us")).alias("Datetime"),
    ]
    if "url" in df.columns:
        columns.append(pl.col("url").cast(pl.Utf8))
    else:
        columns.append(pl.lit(None, dtype=pl.Utf8).alias("url"))
    for c in _SENTIMENT_FLOAT_COLS:
        if c in df.columns:
            columns.append(pl.col(c).cast(pl.Float32))
        else:
            columns.append(pl.lit(None, dtype=pl.Float32).alias(c))
    return df.select(columns)
