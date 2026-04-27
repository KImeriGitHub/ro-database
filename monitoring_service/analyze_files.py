"""Cheap structural checks against the folder being reported on.

- ``file_counts`` per (asset_type, endpoint): how many parquets were written
  vs how many the catalog expected (filtered by yield_status where
  applicable). A silently broken endpoint task drops the ratio sharply.
- ``storage``: total bytes and file count under the folder. Useful for
  cost/baseline tracking and as a smoke signal that something was written.

Both functions are best-effort: missing folders or unreadable files are
recorded with a hint rather than raising.
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

# Same mapping used by the orchestrators. Repeated here to avoid pulling
# the daily/historical packages just to read a constant.
_ASSET_ENDPOINTS: dict[str, tuple[str, ...]] = {
    "stocks": (
        "prices", "prices_daily", "income_statement", "balance_sheet",
        "cash_flow", "earnings", "earnings_estimates", "insider", "sentiment",
    ),
    "etfs": ("prices", "prices_daily", "etf_profile"),
    "forex": ("forex",),
    "indices": ("indices",),
    "cryptocurrencies": ("cryptocurrencies",),
    "commodities": ("commodities",),
    "economic": ("economic",),
}

# Endpoints with a per-symbol yield column. Other endpoints are full-catalog.
_YIELD_COLUMN_ENDPOINTS = {
    "prices", "prices_daily", "income_statement", "balance_sheet",
    "cash_flow", "earnings", "earnings_estimates", "insider", "sentiment",
    "etf_profile",
}

# Endpoints that emit two files per symbol (annual + quarterly).
_FUNDAMENTAL_ENDPOINTS = {
    "income_statement", "balance_sheet", "cash_flow",
    "earnings", "earnings_estimates",
}

# Endpoints that read a single asset_type catalog and write one file per
# symbol whose yield_status.direct cell is True.
_DIRECT_ENDPOINTS = {"forex", "indices", "cryptocurrencies", "commodities", "economic"}


def _expected_count(
    asset_type: str,
    endpoint: str,
    catalog_dir: Path,
    yield_status: pl.DataFrame | None,
) -> int | None:
    catalog_path = catalog_dir / f"{asset_type}.parquet"
    if not catalog_path.exists():
        return None
    catalog = pl.read_parquet(catalog_path, columns=["symbol"])
    catalog_symbols = set(catalog["symbol"].to_list())

    if yield_status is None or "symbol" not in yield_status.columns:
        return len(catalog_symbols)

    if endpoint in _YIELD_COLUMN_ENDPOINTS and endpoint in yield_status.columns:
        col = endpoint
    elif endpoint in _DIRECT_ENDPOINTS and "direct" in yield_status.columns:
        col = "direct"
    else:
        return len(catalog_symbols)

    yielding = (
        yield_status.filter(
            pl.col(col) == True   # noqa: E712
        )
        .select("symbol")
        ["symbol"].to_list()
    )
    return len(set(yielding) & catalog_symbols)


def _file_count(folder_dir: Path, asset_type: str, endpoint: str) -> int:
    ep_dir = folder_dir / asset_type / endpoint
    if not ep_dir.exists():
        return 0

    if endpoint in _FUNDAMENTAL_ENDPOINTS:
        # Each symbol contributes up to two files; count distinct symbols
        # that have at least one of the two files written.
        seen: set[str] = set()
        for path in ep_dir.glob("*.parquet"):
            stem = path.stem
            for suffix in ("_annual", "_quarterly"):
                if stem.endswith(suffix):
                    seen.add(stem[: -len(suffix)])
                    break
            else:
                seen.add(stem)
        return len(seen)

    if endpoint == "sentiment":
        # ALL_MESSAGES.parquet plus per-symbol files. Count per-symbol files.
        return sum(
            1 for p in ep_dir.glob("*.parquet")
            if p.stem != "ALL_MESSAGES"
        )

    return sum(1 for _ in ep_dir.glob("*.parquet"))


def analyze_files(folder_dir: Path, catalog_dir: Path) -> dict:
    yield_path = catalog_dir / "yield_status.parquet"
    yield_status = pl.read_parquet(yield_path) if yield_path.exists() else None

    file_counts: dict = {}
    for asset_type, endpoints in _ASSET_ENDPOINTS.items():
        per_asset: dict = {}
        for endpoint in endpoints:
            written = _file_count(folder_dir, asset_type, endpoint)
            expected = _expected_count(
                asset_type, endpoint, catalog_dir, yield_status
            )
            entry: dict = {"files_written": written, "expected": expected}
            if expected:
                entry["ratio"] = round(written / expected, 4)
            per_asset[endpoint] = entry
        file_counts[asset_type] = per_asset

    return file_counts


def analyze_storage(folder_dir: Path) -> dict:
    if not folder_dir.exists():
        return {"missing": True, "bytes": 0, "file_count": 0}
    total_bytes = 0
    file_count = 0
    for path in folder_dir.rglob("*"):
        if path.is_file():
            file_count += 1
            try:
                total_bytes += path.stat().st_size
            except OSError:
                pass
    return {"bytes": total_bytes, "file_count": file_count}
