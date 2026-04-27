"""Roll up ``ingestion_report.parquet`` issue counts.

Headline counts are flat ints (``timezone_mismatch``, ``av_throttle``).
Issue types that vary by endpoint (``structure_error``, ``empty_content``,
``cast_failure``) are returned as a total plus a per-(asset_type, endpoint)
breakdown, so a single endpoint regression is easy to spot.
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

_BREAKDOWN_ISSUES = ("structure_error", "empty_content", "cast_failure")
_FLAT_ISSUES = ("timezone_mismatch", "av_throttle")


def _empty() -> dict:
    out: dict = {issue: 0 for issue in _FLAT_ISSUES}
    for issue in _BREAKDOWN_ISSUES:
        out[issue] = {"total": 0, "by_asset_endpoint": []}
    out["total_issues"] = 0
    return out


def analyze_ingestion(report_path: Path) -> dict:
    if not report_path.exists():
        logger.info(
            f"No ingestion report at {report_path}; "
            "issue counts will be reported as zero"
        )
        out = _empty()
        out["missing"] = True
        return out

    df = pl.read_parquet(report_path)
    out: dict = {"missing": False, "total_issues": df.height}

    for issue in _FLAT_ISSUES:
        out[issue] = df.filter(pl.col("issue_type") == issue).height

    for issue in _BREAKDOWN_ISSUES:
        sub = df.filter(pl.col("issue_type") == issue)
        breakdown = (
            sub.group_by(["asset_type", "endpoint"])
            .agg(pl.len().alias("count"))
            .sort(["asset_type", "endpoint"])
        )
        rows = [
            {
                "asset_type": r["asset_type"],
                "endpoint": r["endpoint"],
                "count": int(r["count"]),
            }
            for r in breakdown.iter_rows(named=True)
        ]
        out[issue] = {"total": sub.height, "by_asset_endpoint": rows}

    return out
