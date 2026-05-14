"""Shared deduplication helper used by every per-frame builder.

A frame is concatenated from multiple sources (historical + each daily
folder) and tagged with ``_source_order`` so that ``unique(keep="last")``
keeps the most recent daily snapshot. Before dedup, value discrepancies
across duplicate keys are detected and logged into the
``TransformationReport``.
"""

from __future__ import annotations

import logging

import polars as pl

from data_transformation._common import TransformationReport

logger = logging.getLogger(__name__)


SOURCE_ORDER_COL = "_source_order"


def attach_source_order(frames: list[pl.DataFrame]) -> pl.DataFrame:
    """Concat *frames* and tag each row with its source index (0..N-1).

    Source 0 is the first frame passed (usually historical), the remaining
    frames carry strictly higher orders. ``vertical_relaxed`` is used so
    nullable Float32 columns originating from different sources merge
    cleanly.
    """
    parts = [
        f.with_columns(pl.lit(i, dtype=pl.UInt32).alias(SOURCE_ORDER_COL))
        for i, f in enumerate(frames)
    ]
    return pl.concat(parts, how="vertical_relaxed")


def dedup_with_discrepancy_log(
    df: pl.DataFrame,
    key: str | list[str],
    float_cols: tuple[str, ...],
    report: TransformationReport,
    symbol: str,
    asset_type: str,
    frame_name: str,
    *,
    keep: str = "first",
    flag_under_1pct: bool = True,
    suppress_historic_boundary: bool = False,
) -> pl.DataFrame:
    """Sort *df* by ``(key..., _source_order)``, log per-key value
    discrepancies in *float_cols*, then dedup keeping one row per key.

    *keep* selects which source wins on collisions:
    ``"first"`` = earliest source order (PIT-correct: the snapshot that
    first captured the row wins, restatements are dropped). ``"last"`` =
    most recent source order (latest restated value wins). Price frames
    use ``"last"``; everything else uses the default ``"first"``.

    *flag_under_1pct* set to False suppresses
    ``dedup_value_discrepancy_under_1pct`` records. Insider/sentiment
    use this: small (<1%) drift between snapshots is normal noise on
    those frames and is not worth a report row. Over-1pct entries
    still fire and represent the actual signal worth reviewing.

    *suppress_historic_boundary* set to True suppresses the
    discrepancy classification (both under and over) on the single
    duplicate key that sits at the historic-snapshot boundary: the
    maximum value of *key* observed among ``_source_order == 0`` rows.
    The historic snapshot's last bar is routinely a partial bar
    (24/7 markets like crypto, or any historic pull captured mid-
    session), so its OHLCV disagrees benignly with the same date
    re-pulled in a later daily snapshot. The row itself is unaffected
    -- ``keep="last"`` already discards the partial historic value in
    favor of the daily value. Only single-column keys are supported;
    multi-key callers (insider/sentiment) ignore the flag. The
    suppression is silent (no report row) and applies only to
    discrepancies that include a source-0 row, so daily-vs-daily
    restatements on the boundary date still surface.

    *key* may be a single column name or a list of column names for
    composite keys (e.g. insider rows keyed on
    ``(transactionDate, executive, security_type)``).

    *df* must already carry the ``_source_order`` column attached by
    :func:`attach_source_order`. Output drops that column.
    """
    if keep not in ("first", "last"):
        raise ValueError(f"keep must be 'first' or 'last', got {keep!r}")

    keys: list[str] = [key] if isinstance(key, str) else list(key)
    if df.height == 0:
        return df.drop(SOURCE_ORDER_COL) if SOURCE_ORDER_COL in df.columns else df

    df = df.sort([*keys, SOURCE_ORDER_COL])

    counts = df.group_by(keys).agg(pl.len().alias("_n"))
    dup_keys = counts.filter(pl.col("_n") > 1).select(keys)

    if dup_keys.height > 0:
        dup_rows = df.join(dup_keys, on=keys, how="inner")
        if suppress_historic_boundary and len(keys) == 1:
            hist_rows = df.filter(pl.col(SOURCE_ORDER_COL) == 0)
            if hist_rows.height > 0:
                boundary = hist_rows.select(pl.col(keys[0]).max()).item()
                dup_rows = dup_rows.filter(
                    ~(
                        (pl.col(keys[0]) == boundary)
                        & (pl.col(SOURCE_ORDER_COL) == 0)
                    )
                )
        n_under, n_over, samples_under, samples_over = _classify_discrepancies(
            dup_rows, keys, float_cols
        )
        if n_under and flag_under_1pct:
            report.record(
                symbol, asset_type, frame_name,
                "dedup_value_discrepancy_under_1pct",
                count=n_under,
                relative=n_under / df.height,
                detail=("; ".join(samples_under))[:500],
            )
        if n_over:
            report.record(
                symbol, asset_type, frame_name,
                "dedup_value_discrepancy_over_1pct",
                count=n_over,
                relative=n_over / df.height,
                detail=("; ".join(samples_over))[:500],
            )

    deduped = df.unique(subset=keys, keep=keep, maintain_order=False)
    return deduped.drop(SOURCE_ORDER_COL).sort(keys)


def _classify_discrepancies(
    dup_rows: pl.DataFrame,
    keys: list[str],
    float_cols: tuple[str, ...],
) -> tuple[int, int, list[str], list[str]]:
    """Per duplicate key, classify by max relative discrepancy across
    *float_cols* (<1% vs >=1%). Returns
    ``(n_under, n_over, samples_under, samples_over)`` with up to three
    sample detail strings each.
    """
    if not float_cols:
        return 0, 0, [], []

    aggs: list[pl.Expr] = []
    for c in float_cols:
        aggs.append(pl.col(c).min().alias(f"{c}_min"))
        aggs.append(pl.col(c).max().alias(f"{c}_max"))
    grouped = dup_rows.group_by(keys).agg(aggs)

    rel_cols: list[pl.Expr] = []
    for c in float_cols:
        mn = pl.col(f"{c}_min")
        mx = pl.col(f"{c}_max")
        rel = (
            pl.when(
                mn.is_not_null() & mx.is_not_null() & (mx.abs() > 0) & (mn != mx)
            )
            .then((mx - mn).abs() / mx.abs())
            .otherwise(0.0)
        )
        rel_cols.append(rel.cast(pl.Float64).alias(f"{c}_rel"))
    grouped = grouped.with_columns(rel_cols)
    grouped = grouped.with_columns(
        pl.max_horizontal([pl.col(f"{c}_rel") for c in float_cols]).alias("_max_rel")
    )
    diverged = grouped.filter(pl.col("_max_rel") > 0)
    if diverged.height == 0:
        return 0, 0, [], []

    n_under = diverged.filter(pl.col("_max_rel") < 0.01).height
    n_over = diverged.filter(pl.col("_max_rel") >= 0.01).height

    samples_under = _sample_details(
        diverged.filter(pl.col("_max_rel") < 0.01).head(3), keys, float_cols
    )
    samples_over = _sample_details(
        diverged.filter(pl.col("_max_rel") >= 0.01).head(3), keys, float_cols
    )
    return n_under, n_over, samples_under, samples_over


def _sample_details(
    sample: pl.DataFrame,
    keys: list[str],
    float_cols: tuple[str, ...],
) -> list[str]:
    out: list[str] = []
    for row in sample.iter_rows(named=True):
        pieces: list[str] = []
        for c in float_cols:
            rel = row[f"{c}_rel"]
            if rel and rel > 0:
                mn = row[f"{c}_min"]
                mx = row[f"{c}_max"]
                pieces.append(f"{c}={mn:.4g}/{mx:.4g}")
        key_label = "|".join(str(row[k]) for k in keys)
        out.append(f"{key_label} " + ",".join(pieces))
    return out
