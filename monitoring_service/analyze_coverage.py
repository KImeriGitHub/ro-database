"""Per-symbol parquet coverage probes.

For a small set of named ETFs (and, when available, every constituent of
QQQ's ETF profile), check that intraday and daily price parquets exist with
the expected shape:

- Intraday: at least :data:`INTRADAY_MIN_ROWS` 1-min bars, and per-OHLCV
  column null ratio strictly below :data:`MAX_NULL_RATIO`.
- Daily: exactly one row.

Also reports ``freshness`` (max ``Date`` per file) so a frozen feed shows up.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path

import polars as pl

from historical_data_setup._common import symbol_parquet_name

logger = logging.getLogger(__name__)

REQUIRED_ETFS = ("SPY", "MDY", "EWJ", "EWU", "DIA", "QQQ")
QQQ_PROFILE_FILE = symbol_parquet_name("etfs", "QQQ")
INTRADAY_MIN_ROWS = 390
DAILY_EXPECTED_ROWS = 1
MAX_NULL_RATIO = 0.01
_PRICE_COLUMNS = ("Open", "High", "Low", "Close", "Volume")


def _resolve_files(folder_dir: Path, asset_type: str, symbol: str) -> tuple[Path, Path]:
    fname = symbol_parquet_name(asset_type, symbol)
    return (
        folder_dir / asset_type / "prices" / fname,
        folder_dir / asset_type / "prices_daily" / fname,
    )


def _max_date(df: pl.DataFrame) -> str | None:
    if "Date" not in df.columns or df.height == 0:
        return None
    val = df["Date"].max()
    if isinstance(val, (date, datetime)):
        return val.isoformat()
    return str(val)


def _check_intraday(path: Path) -> dict:
    if not path.exists():
        return {"ok": False, "reason": "missing", "rows": 0, "max_date": None}

    df = pl.read_parquet(path)
    rows = df.height
    max_date = _max_date(df)

    failures: list[str] = []
    if rows < INTRADAY_MIN_ROWS:
        failures.append(f"rows={rows} < {INTRADAY_MIN_ROWS}")

    null_ratios: dict[str, float] = {}
    for col in _PRICE_COLUMNS:
        if col not in df.columns:
            failures.append(f"missing column {col}")
            continue
        ratio = (df[col].null_count() / rows) if rows else 1.0
        null_ratios[col] = round(ratio, 6)
        if ratio >= MAX_NULL_RATIO:
            failures.append(f"{col} null_ratio={ratio:.4f} >= {MAX_NULL_RATIO}")

    return {
        "ok": not failures,
        "reason": "; ".join(failures) if failures else None,
        "rows": rows,
        "max_date": max_date,
        "null_ratios": null_ratios,
    }


def _check_daily(path: Path) -> dict:
    if not path.exists():
        return {"ok": False, "reason": "missing", "rows": 0, "max_date": None}

    df = pl.read_parquet(path)
    rows = df.height
    max_date = _max_date(df)
    ok = rows == DAILY_EXPECTED_ROWS
    return {
        "ok": ok,
        "reason": (
            None if ok else f"rows={rows} != {DAILY_EXPECTED_ROWS}"
        ),
        "rows": rows,
        "max_date": max_date,
    }


def _check_one(folder_dir: Path, asset_type: str, symbol: str) -> dict:
    intraday_path, daily_path = _resolve_files(folder_dir, asset_type, symbol)
    return {
        "symbol": symbol,
        "asset_type": asset_type,
        "intraday": _check_intraday(intraday_path),
        "daily": _check_daily(daily_path),
    }


def _read_qqq_holdings(folder_dir: Path) -> tuple[str, list[str]]:
    """Return ``(status, holdings)`` for the QQQ ETF profile in this folder.

    *status* is one of ``"present"``, ``"missing"``, ``"unreadable"``.
    """
    profile_path = folder_dir / "etfs" / "etf_profile" / QQQ_PROFILE_FILE
    if not profile_path.exists():
        logger.info(f"QQQ ETF profile not found at {profile_path}; skipping holdings probe")
        return "missing", []

    try:
        df = pl.read_parquet(profile_path)
    except Exception as e:
        logger.warning(f"Could not read QQQ profile {profile_path}: {e}")
        return "unreadable", []

    if "holdings" not in df.columns or df.height == 0:
        return "present", []

    raw = df["holdings"][0]
    if raw is None:
        return "present", []

    holdings: list[str] = []
    for entry in raw:
        sym = entry.get("symbol") if isinstance(entry, dict) else None
        if sym:
            holdings.append(sym)
    return "present", holdings


def analyze_coverage(folder_dir: Path) -> dict:
    """Run the coverage probes for *folder_dir*.

    *folder_dir* is the daily-folder root (``daily/<YYYY-MM-DD>/``) for daily/
    weekend modes, or the historical root (``historical/``) for the historical
    mode.  In every case the layout is ``<asset_type>/<endpoint>/SYMBOL.parquet``.
    """
    qqq_status, holdings = _read_qqq_holdings(folder_dir)

    etf_results = [
        _check_one(folder_dir, "etfs", sym) for sym in REQUIRED_ETFS
    ]

    holdings_results: list[dict] = []
    for sym in holdings:
        # Holdings sit in the stocks tree; if a holding is itself an ETF the
        # file simply will not be there and the probe records "missing".
        holdings_results.append(_check_one(folder_dir, "stocks", sym))

    failures: list[str] = []
    intraday_ok = 0
    daily_ok = 0
    for res in etf_results + holdings_results:
        if res["intraday"]["ok"]:
            intraday_ok += 1
        else:
            failures.append(
                f"{res['asset_type']}/{res['symbol']}/intraday: {res['intraday']['reason']}"
            )
        if res["daily"]["ok"]:
            daily_ok += 1
        else:
            failures.append(
                f"{res['asset_type']}/{res['symbol']}/daily: {res['daily']['reason']}"
            )

    total = len(etf_results) + len(holdings_results)
    return {
        "qqq_profile_status": qqq_status,
        "qqq_holdings_count": len(holdings),
        "required_etfs": list(REQUIRED_ETFS),
        "etf_results": etf_results,
        "holdings_results": holdings_results,
        "summary": {
            "total_checked": total,
            "intraday_ok": intraday_ok,
            "daily_ok": daily_ok,
            "failures": failures,
        },
    }
