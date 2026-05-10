"""Shared helpers for the integration test scripts.

These tests target a real, persistent ``database/`` folder and run the actual
pipelines against Alpha Vantage, so re-runs need to keep the catalog narrowed
to a small, reproducible subset of symbols. ``reduce_catalogs`` performs that
narrowing and propagates the trim to ``yield_status.parquet`` and to every
``earnings_calendar.parquet`` it can find under ``historical/`` and
``daily/<YYYY-MM-DD>/`` so downstream daily / weekly / transform runs stay
consistent.

The "random" 10 extra stocks are picked with a stable hash-based scheme. The
candidate population (active stocks not in the mandatory list) shifts as AV
LISTING_STATUS changes day to day, but a hash ranking only re-shuffles when a
previously-picked symbol disappears from the catalog, which keeps the kept
set as stable as possible across re-inits.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from pathlib import Path

import polars as pl

from maintainance_scripts.logging_setup import (
    DEFAULT_DATEFMT,
    DEFAULT_FORMAT,
    configure_logging,
)

logger = logging.getLogger(__name__)


# Stocks that must always be in the reduced catalog (FRD coverage in frd_dir).
MANDATORY_STOCKS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA",
    "JPM", "GS", "BRK-B", "IBM", "T", "NEE", "SPG", "O", "TSM", "F",
]

# ETFs that must always be in the reduced catalog: FRD coverage (QQQ, SPY, GLD)
# plus the four monitoring_service.analyze_coverage probes (MDY, EWJ, EWU, DIA).
MANDATORY_ETFS = ["QQQ", "SPY", "GLD", "MDY", "EWJ", "EWU", "DIA"]

EXTRA_RANDOM_STOCKS = 25
RANDOM_SEED = "ro-database-int-tests-41"

# Catalogs whose symbols come from non-stock/non-etf asset types. Their rows
# in yield_status.parquet must be kept untouched when we reduce.
OTHER_CATALOG_FILES = (
    "forex.parquet",
    "indices.parquet",
    "cryptocurrencies.parquet",
    "commodities.parquet",
    "economic.parquet",
)


def _hash_rank(symbol: str) -> str:
    """Stable per-symbol ranking key. Smaller hex -> earlier in the picking order."""
    h = hashlib.sha256(f"{RANDOM_SEED}:{symbol}".encode("utf-8"))
    return h.hexdigest()


def pick_extra_stocks(
    candidates: list[str], k: int = EXTRA_RANDOM_STOCKS
) -> list[str]:
    """Pick *k* extra stocks from *candidates* by deterministic hash ranking.

    The ranking only changes for symbols whose hash position is affected by
    the candidate set, so adding/removing unrelated symbols does not shuffle
    the result.
    """
    ranked = sorted(candidates, key=_hash_rank)
    return sorted(ranked[:k])


def _kept_stocks(stocks_path: Path) -> list[str]:
    df = pl.read_parquet(stocks_path)
    all_syms = df["symbol"].to_list()
    mandatory_present = [s for s in MANDATORY_STOCKS if s in all_syms]
    missing_mandatory = [s for s in MANDATORY_STOCKS if s not in all_syms]
    if missing_mandatory:
        logger.warning(
            f"reduce_catalogs: mandatory stocks not found in stocks.parquet: "
            f"{missing_mandatory}"
        )

    # OMITTING Pool: active, non-mandatory, with an ipoDate set (more stable than
    # newly-listed empty-history symbols).
    #pool_df = df
    #if "status" in pool_df.columns:
    #    pool_df = pool_df.filter(
    #        pl.col("status").str.to_lowercase() == "active"
    #    )
    #if "ipoDate" in pool_df.columns:
    #    pool_df = pool_df.filter(pl.col("ipoDate").is_not_null())
    pool = [
        s for s in df["symbol"].to_list() if s not in MANDATORY_STOCKS
    ]
    extras = pick_extra_stocks(pool, EXTRA_RANDOM_STOCKS)

    return sorted(set(mandatory_present + extras))


def _kept_etfs(etfs_path: Path) -> list[str]:
    df = pl.read_parquet(etfs_path)
    all_syms = df["symbol"].to_list()
    kept = [s for s in MANDATORY_ETFS if s in all_syms]
    missing = [s for s in MANDATORY_ETFS if s not in all_syms]
    if missing:
        logger.warning(
            f"reduce_catalogs: mandatory ETFs not found in etfs.parquet: "
            f"{missing}"
        )
    return sorted(set(kept))


def _trim_earnings_calendar(path: Path, kept_stocks: list[str]) -> None:
    """Filter a single ``earnings_calendar.parquet`` to *kept_stocks*."""
    if not path.exists():
        return
    ec = pl.read_parquet(path)
    before = ec.height
    ec = ec.filter(pl.col("symbol").is_in(kept_stocks))
    ec.write_parquet(path, compression="zstd")
    logger.info(
        f"reduce_catalogs: {path} {before} -> {ec.height} rows"
    )


def reduce_catalogs(
    catalog_dir: Path,
    historical_dir: Path | None = None,
    daily_dir: Path | None = None,
) -> tuple[list[str], list[str]]:
    """Trim ``stocks.parquet`` / ``etfs.parquet`` and propagate the trim.

    Returns ``(kept_stocks, kept_etfs)``. Files updated:

    - ``stocks.parquet``        -> rows filtered to kept stocks.
    - ``etfs.parquet``          -> rows filtered to kept ETFs.
    - ``yield_status.parquet``  -> rows for stock symbols not in *kept_stocks*
      and ETF symbols not in *kept_etfs* are dropped. Symbols belonging to
      other asset types (forex / indices / crypto / commodities / economic)
      are left untouched.
    - ``historical/earnings_calendar.parquet`` -> filtered to *kept_stocks*
      when *historical_dir* is provided and the file exists.
    - ``daily/<YYYY-MM-DD>/earnings_calendar.parquet`` -> filtered to
      *kept_stocks* for every dated subdir under *daily_dir* when provided.

    The earnings_calendar trims are no-ops when the file is absent, so
    callers can pass *historical_dir* / *daily_dir* unconditionally even
    when those trees haven't been populated yet (e.g. fresh init runs).
    """
    catalog_dir = Path(catalog_dir)
    stocks_path = catalog_dir / "stocks.parquet"
    etfs_path = catalog_dir / "etfs.parquet"

    if not stocks_path.exists():
        raise FileNotFoundError(f"stocks.parquet not found at {stocks_path}")
    if not etfs_path.exists():
        raise FileNotFoundError(f"etfs.parquet not found at {etfs_path}")

    kept_stocks = _kept_stocks(stocks_path)
    kept_etfs = _kept_etfs(etfs_path)

    # stocks.parquet
    stocks_df = pl.read_parquet(stocks_path)
    before = stocks_df.height
    stocks_df = stocks_df.filter(pl.col("symbol").is_in(kept_stocks))
    stocks_df.write_parquet(stocks_path, compression="zstd")
    logger.info(
        f"reduce_catalogs: stocks.parquet {before} -> {stocks_df.height} rows "
        f"({len(kept_stocks)} kept symbols)"
    )

    # etfs.parquet
    etfs_df = pl.read_parquet(etfs_path)
    before = etfs_df.height
    etfs_df = etfs_df.filter(pl.col("symbol").is_in(kept_etfs))
    etfs_df.write_parquet(etfs_path, compression="zstd")
    logger.info(
        f"reduce_catalogs: etfs.parquet {before} -> {etfs_df.height} rows "
        f"({len(kept_etfs)} kept symbols)"
    )

    # yield_status.parquet: keep all rows whose symbol is in kept_stocks,
    # kept_etfs, or any other-asset-type catalog. Without that union we'd
    # drop legitimate forex/indices/etc rows that happen to share names.
    yield_path = catalog_dir / "yield_status.parquet"
    if yield_path.exists():
        other_syms: set[str] = set()
        for fname in OTHER_CATALOG_FILES:
            fpath = catalog_dir / fname
            if not fpath.exists():
                continue
            other_syms.update(
                pl.read_parquet(fpath, columns=["symbol"])["symbol"].to_list()
            )
        keep_syms = set(kept_stocks) | set(kept_etfs) | other_syms
        ys = pl.read_parquet(yield_path)
        before = ys.height
        ys = ys.filter(pl.col("symbol").is_in(list(keep_syms)))
        ys.write_parquet(yield_path, compression="zstd")
        logger.info(
            f"reduce_catalogs: yield_status.parquet {before} -> {ys.height} rows"
        )

    # earnings_calendar.parquet now lives in historical/ and daily/<date>/
    # (it moved out of catalog/). Trim every copy we can find.
    if historical_dir is not None:
        _trim_earnings_calendar(
            Path(historical_dir) / "earnings_calendar.parquet", kept_stocks,
        )
    if daily_dir is not None:
        daily_dir = Path(daily_dir)
        if daily_dir.exists():
            for child in sorted(daily_dir.iterdir()):
                if not child.is_dir():
                    continue
                _trim_earnings_calendar(
                    child / "earnings_calendar.parquet", kept_stocks,
                )

    return kept_stocks, kept_etfs


def kept_symbols(catalog_dir: Path) -> tuple[list[str], list[str]]:
    """Return ``(kept_stocks, kept_etfs)`` from an already-reduced catalog."""
    stocks = pl.read_parquet(catalog_dir / "stocks.parquet", columns=["symbol"])
    etfs = pl.read_parquet(catalog_dir / "etfs.parquet", columns=["symbol"])
    return sorted(stocks["symbol"].to_list()), sorted(etfs["symbol"].to_list())


# ── Path constants used by the int_test_*.py scripts ──────────────────

INT_TESTS_DIR = Path(__file__).resolve().parent
DATABASE_DIR = INT_TESTS_DIR / "database"
FRD_DIR = INT_TESTS_DIR / "frd_dir"
TRANSFORMATION_DIR = INT_TESTS_DIR / "transformation"
LOGS_DIR = INT_TESTS_DIR / "logs"
CATALOG_DIR = DATABASE_DIR / "catalog"
HISTORICAL_DIR = DATABASE_DIR / "historical"
DAILY_DIR = DATABASE_DIR / "daily"


def configure_int_test_logging(script_path: str | Path) -> Path:
    """Set up logging for an int_*.py script.

    Calls :func:`configure_logging` for the usual stdout handler, then attaches
    a ``FileHandler`` writing to
    ``tests/integration_tests/logs/<YYYYMMDD-HHMMSS>_<script_stem>.log``.
    The timestamp is captured at call time so each run gets its own file.
    Returns the path of the log file.
    """
    configure_logging()
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    stem = Path(script_path).stem
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_path = LOGS_DIR / f"{timestamp}_{stem}.log"

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(
        logging.Formatter(fmt=DEFAULT_FORMAT, datefmt=DEFAULT_DATEFMT)
    )
    logging.getLogger().addHandler(file_handler)
    logger.info(f"Integration test log file: {log_path}")
    return log_path
