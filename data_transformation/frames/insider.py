"""Phase 6a: insider_df for stocks.

Builds the insider transactions frame from
``historical/stocks/insider/`` and ``daily/*/stocks/insider/``. The output
is a chronological list of insider transactions, one row per
``(transactionDate, executive, security_type)`` after dedup. ``Date``
equals ``transactionDate``; the schema does not retain a separate
snapshot date. Avoiding lookahead leakage is the responsibility of the
feature-generation step that consumes ``StockData``.

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
from data_transformation.AssetData import CANONICAL_INSIDER_ROLES
from data_transformation.AssetDataService import SCHEMAS
from data_transformation.frames._dedup import (
    attach_source_order,
    dedup_with_discrepancy_log,
)

logger = logging.getLogger(__name__)


# Ordered regex rule list, applied to lowercased executive_title.
# The first matching rule wins. The rule order IS the spec - see
# AssetData_specifications.md. Bare acronyms (cfo/coo/cto/cio/vp/ceo)
# use `\b...\b` so they do not match inside unrelated words
# (e.g. "Director" contains the substring "cto" but `\bcto\b` does not
# match). VP precedes CEO so "Vice President" routes to VP, not CEO's
# `president` pattern. CEO precedes the catch-all `chief ` so "Chief
# Executive Officer" routes to CEO, not Other C-Suite.
_INSIDER_ROLE_RULES: list[tuple[str, str]] = [
    ("CAO",             r"chief accounting|controller|principal accounting"),
    ("General Counsel", r"general counsel|chief legal|secretary"),
    ("CFO",             r"\bcfo\b|chief financial|treasurer|principal financial"),
    ("COO",             r"\bcoo\b|chief operating"),
    ("CTO_CIO",         r"\bcto\b|\bcio\b|chief technology|chief information|chief digital"),
    ("VP",              r"\bvp\b|vice president|executive vice|senior vice"),
    ("CEO",             r"\bceo\b|chief executive|president"),
    ("Other C-Suite",   r"chief "),
    ("Chairman",        r"chairman|chair of"),
    ("Director",        r"director"),
    ("10% Owner",       r"10%|beneficial owner"),
    ("Officer",         r"officer"),
]

# Sanity-check: every label produced by the rule list must be a known
# canonical role. The "Other" fallback is the implicit final case.
assert all(label in CANONICAL_INSIDER_ROLES for label, _ in _INSIDER_ROLE_RULES)
assert "Other" in CANONICAL_INSIDER_ROLES


_INSIDER_DEDUP_KEYS: list[str] = [
    "transactionDate", "executive", "security_type",
]
_INSIDER_FLOAT_COLS: tuple[str, ...] = ("shares", "share_price")


def build_insider_df(
    symbol: str,
    paths: list[Path],
    report: TransformationReport,
) -> pl.DataFrame:
    """Build the ``insider_df`` frame for one stock symbol.

    Returns an empty schema-correct frame when no usable data is
    available (no source files, all reads failed, etc.).
    """
    empty = pl.DataFrame(schema=SCHEMAS["insider_df"])
    if not paths:
        return empty

    frames: list[pl.DataFrame] = []
    for p in paths:
        try:
            raw = pl.read_parquet(p)
        except Exception as exc:
            logger.warning(
                "stocks/%s: failed to read insider %s: %s", symbol, p, exc,
            )
            continue
        try:
            frames.append(_normalize_insider_source(raw))
        except Exception as exc:
            logger.warning(
                "stocks/%s: failed to normalize insider %s: %s",
                symbol, p, exc,
            )

    if not frames:
        return empty

    merged = attach_source_order(frames)
    merged = dedup_with_discrepancy_log(
        merged, _INSIDER_DEDUP_KEYS, _INSIDER_FLOAT_COLS, report,
        symbol, "stocks", "insider_df",
        flag_under_1pct=False,
    )

    merged = merged.with_columns(
        pl.col("transactionDate").alias("Date"),
        _role_expr().alias("Executive_role"),
        pl.col("acquisition_or_disposal").alias("AcqDis"),
        pl.col("shares").alias("Shares"),
    )

    before = merged.height
    merged = merged.filter(
        pl.col("Shares").is_not_null()
        & pl.col("AcqDis").is_in(["A", "D"])
    )
    dropped = before - merged.height
    if dropped:
        report.record(
            symbol, "stocks", "insider_df", "dedup_dropped_null_row",
            count=dropped,
            relative=dropped / before,
            detail=(
                f"{dropped} of {before} rows had null Shares or "
                "non-A/D acquisition_or_disposal"
            ),
        )

    merged = merged.sort("Date")
    return cast_to_schema(merged, SCHEMAS["insider_df"], "insider_df")


def _normalize_insider_source(df: pl.DataFrame) -> pl.DataFrame:
    """Project the raw insider parquet onto the canonical 7-column shape
    used by dedup and downstream mapping:
    ``(transactionDate, executive, executive_title, security_type,
    acquisition_or_disposal, shares, share_price)``.

    Missing string columns are filled with null Utf8; missing Float32
    columns with null Float32. ``transactionDate`` is required.
    """
    if "transactionDate" not in df.columns:
        raise KeyError("insider source missing 'transactionDate' column")

    columns: list[pl.Expr] = [pl.col("transactionDate").cast(pl.Date)]
    for c in (
        "executive", "executive_title", "security_type",
        "acquisition_or_disposal",
    ):
        if c in df.columns:
            columns.append(pl.col(c).cast(pl.Utf8))
        else:
            columns.append(pl.lit(None, dtype=pl.Utf8).alias(c))
    for c in ("shares", "share_price"):
        if c in df.columns:
            columns.append(pl.col(c).cast(pl.Float32))
        else:
            columns.append(pl.lit(None, dtype=pl.Float32).alias(c))
    return df.select(columns)


def _role_expr() -> pl.Expr:
    """Build a polars expression that maps ``executive_title`` (Utf8) to
    one of ``CANONICAL_INSIDER_ROLES`` via the ordered regex rule list.
    Empty / null / unmatched titles fall through to ``"Other"``.
    """
    title_lc = pl.col("executive_title").fill_null("").str.to_lowercase()
    expr: pl.Expr | None = None
    for label, pattern in _INSIDER_ROLE_RULES:
        cond = title_lc.str.contains(pattern)
        if expr is None:
            expr = pl.when(cond).then(pl.lit(label))
        else:
            expr = expr.when(cond).then(pl.lit(label))
    assert expr is not None
    return expr.otherwise(pl.lit("Other"))
