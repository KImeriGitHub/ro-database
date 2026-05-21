"""Populate tests/integration_tests/frd_dir/ with FirstRateData-shaped CSVs sourced from Alpha Vantage.

Used by the integration test suite to provide an FRD-like local mirror without
requiring a real FirstRateData purchase. Files already present in the
destination are presumed fine and are skipped unless --wipe is passed.

Layout produced (flat directory, no subfolders):
    tests/integration_tests/frd_dir/
        catalog_stocks.csv
        catalog_etfs.csv
        {SYMBOL}_1min.csv
        {SYMBOL}_1day_unadjusted.csv
        {SYMBOL}_1day_splitadjusted.csv
        {SYMBOL}_1day_splitdivadjusted.csv

Run from the project root:
    python tests/integration_tests/_helper_build_frd_test_dir.py [--cutoff-year 2020] [--wipe]

Default cutoff year is 2000; raising it (e.g. 2020) cuts the AV call volume
drastically. Pass --wipe to clear the destination directory before fetching
(otherwise existing files are kept and only missing ones are fetched).
"""

from __future__ import annotations

import argparse
import asyncio
import io
import logging
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import aiohttp
import polars as pl

from asset_catalog_service.updates._common import (
    AV_BASE,
    fetch_json,
    fetch_text,
    normalize_sector,
)
from historical_data_setup._common import (
    RateLimiter,
    fetch_av_json,
    generate_months,
)
from maintainance_scripts.get_api_key import get_alpha_vantage_key

logger = logging.getLogger(__name__)

OUT_DIR = PROJECT_ROOT / "tests" / "integration_tests" / "frd_dir"

STOCKS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA",
    "JPM", "GS", "BRK-B", "IBM", "T", "NEE", "SPG", "O", "TSM", "F",
]
ETFS = ["QQQ", "SPY", "GLD"]

RATE_BY_TIER = {"standard": 2, "premium": 74}

_TS_DAILY_KEY = "Time Series (Daily)"
_TS_INTRADAY_KEY = "Time Series (1min)"


# ── Catalog ──────────────────────────────────────────────────────────


def fetch_listings(api_key: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Fetch LISTING_STATUS active+delisted, return (stocks, etfs)."""
    logger.info("Fetching LISTING_STATUS active+delisted...")
    active_csv = fetch_text(
        f"{AV_BASE}/query?function=LISTING_STATUS&state=active&apikey={api_key}"
    )
    delisted_csv = fetch_text(
        f"{AV_BASE}/query?function=LISTING_STATUS&state=delisted&apikey={api_key}"
    )

    active = pl.read_csv(io.StringIO(active_csv), null_values=["null"], infer_schema_length=0)
    delisted = pl.read_csv(io.StringIO(delisted_csv), null_values=["null"], infer_schema_length=0)
    combined = pl.concat([active, delisted], how="vertical_relaxed")

    stocks = (
        combined.filter(pl.col("assetType") == "Stock")
        .with_columns(
            pl.col("ipoDate").cast(pl.Date, strict=False),
            pl.col("delistingDate").cast(pl.Date, strict=False),
        )
        .select("symbol", "name", "ipoDate", "delistingDate", "status")
    )
    etfs = (
        combined.filter(pl.col("assetType") == "ETF")
        .with_columns(
            pl.col("ipoDate").cast(pl.Date, strict=False),
            pl.col("delistingDate").cast(pl.Date, strict=False),
        )
        .select("symbol", "name", "ipoDate", "delistingDate", "status")
    )
    return stocks, etfs


def fetch_sector(api_key: str, symbol: str) -> str:
    url = f"{AV_BASE}/query?function=OVERVIEW&symbol={symbol}&apikey={api_key}"
    data = fetch_json(url)
    return normalize_sector(data.get("Sector"))


def write_stock_catalog(
    stocks_df: pl.DataFrame, sectors: dict[str, str], out_path: Path
) -> None:
    out = (
        stocks_df.with_columns(
            pl.col("symbol")
            .map_elements(lambda s: sectors.get(s, "Other"), return_dtype=pl.Utf8)
            .alias("Sector")
        )
        .rename({
            "symbol": "Ticker",
            "name": "Company Name",
            "ipoDate": "IPO Date",
            "delistingDate": "Delisting Date",
            "status": "Status",
        })
        .select("Ticker", "Company Name", "Sector", "IPO Date", "Status", "Delisting Date")
    )
    out.write_csv(out_path)
    logger.info(f"Wrote {out_path.name} ({out.height} rows)")


def write_etf_catalog(etfs_df: pl.DataFrame, out_path: Path) -> None:
    out = (
        etfs_df.rename({
            "symbol": "Ticker",
            "name": "Name",
            "ipoDate": "IPO Date",
            "delistingDate": "Delisting Date",
            "status": "Status",
        })
        .select("Ticker", "Name", "IPO Date", "Status", "Delisting Date")
    )
    out.write_csv(out_path)
    logger.info(f"Wrote {out_path.name} ({out.height} rows)")


# ── Daily prices (3 CSVs per symbol, computed from AV adjusted feed) ─


def _compute_cumul_split(split_coefs: list[float]) -> list[float]:
    """cs[t] = product of split_coef[s] for s > t (strictly future days).

    Last row gets 1.0, walking backwards multiplies by the *next* day's
    split coefficient.  AV's split coefficient is on the day OF the split:
    prices on/after that day are already post-split, prices before need
    to be divided by the coefficient.
    """
    n = len(split_coefs)
    cs = [1.0] * n
    for i in range(n - 2, -1, -1):
        cs[i] = cs[i + 1] * split_coefs[i + 1]
    return cs


async def fetch_and_write_daily(
    symbol: str,
    api_key: str,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    out_dir: Path,
    cutoff_year: int,
) -> None:
    unadj_path = out_dir / f"{symbol}_1day_unadjusted.csv"
    sa_path = out_dir / f"{symbol}_1day_splitadjusted.csv"
    sda_path = out_dir / f"{symbol}_1day_splitdivadjusted.csv"
    if unadj_path.exists() and sa_path.exists() and sda_path.exists():
        logger.info(f"  daily {symbol}: all 3 files present, skipping")
        return

    url = (
        f"{AV_BASE}/query?function=TIME_SERIES_DAILY_ADJUSTED"
        f"&symbol={symbol}&outputsize=full&apikey={api_key}"
    )
    try:
        data = await fetch_av_json(url, session, rate_limiter)
    except Exception as e:
        logger.warning(f"  daily {symbol}: fetch failed: {e}")
        return

    ts = data.get(_TS_DAILY_KEY)
    if not ts:
        logger.warning(f"  daily {symbol}: missing '{_TS_DAILY_KEY}'")
        return

    rows: list[dict] = []
    for date_str, ohlcv in ts.items():
        try:
            rows.append({
                "Date": date_str,
                "open": float(ohlcv["1. open"]),
                "high": float(ohlcv["2. high"]),
                "low": float(ohlcv["3. low"]),
                "close": float(ohlcv["4. close"]),
                "adj_close": float(ohlcv["5. adjusted close"]),
                "volume": float(ohlcv["6. volume"]),
                "split_coef": float(ohlcv["8. split coefficient"]),
            })
        except (KeyError, ValueError, TypeError) as e:
            logger.warning(f"  daily {symbol} {date_str}: cast failure: {e}")

    if not rows:
        return

    df = (
        pl.DataFrame(rows)
        .with_columns(pl.col("Date").str.to_date("%Y-%m-%d"))
        .sort("Date")
        .filter(pl.col("Date") >= date(cutoff_year, 1, 1))
    )
    if df.height == 0:
        logger.info(f"  daily {symbol}: 0 rows after cutoff")
        return

    cs = _compute_cumul_split(df["split_coef"].to_list())
    df = df.with_columns(pl.Series("cs", cs))

    # Split-adjusted (price / cs, volume * cs)
    df = df.with_columns(
        (pl.col("open") / pl.col("cs")).alias("sa_open"),
        (pl.col("high") / pl.col("cs")).alias("sa_high"),
        (pl.col("low") / pl.col("cs")).alias("sa_low"),
        (pl.col("close") / pl.col("cs")).alias("sa_close"),
        (pl.col("volume") * pl.col("cs")).alias("sa_volume"),
    )

    # Split + dividend adjusted: sda_close == AV's adj_close, OHL scaled
    # by the dividend-only ratio, volume same as split-adjusted.
    df = df.with_columns(
        pl.when(pl.col("sa_close") > 0)
        .then(pl.col("adj_close") / pl.col("sa_close"))
        .otherwise(pl.lit(1.0))
        .alias("r")
    ).with_columns(
        (pl.col("sa_open") * pl.col("r")).alias("sda_open"),
        (pl.col("sa_high") * pl.col("r")).alias("sda_high"),
        (pl.col("sa_low") * pl.col("r")).alias("sda_low"),
        pl.col("adj_close").alias("sda_close"),
        pl.col("sa_volume").alias("sda_volume"),
    )

    wrote = 0
    if not unadj_path.exists():
        df.select(
            pl.col("Date").alias("timestamp"),
            "open", "high", "low", "close", "volume",
        ).write_csv(unadj_path)
        wrote += 1

    if not sa_path.exists():
        df.select(
            pl.col("Date").alias("timestamp"),
            pl.col("sa_open").alias("open"),
            pl.col("sa_high").alias("high"),
            pl.col("sa_low").alias("low"),
            pl.col("sa_close").alias("close"),
            pl.col("sa_volume").alias("volume"),
        ).write_csv(sa_path)
        wrote += 1

    if not sda_path.exists():
        df.select(
            pl.col("Date").alias("timestamp"),
            pl.col("sda_open").alias("open"),
            pl.col("sda_high").alias("high"),
            pl.col("sda_low").alias("low"),
            pl.col("sda_close").alias("close"),
            pl.col("sda_volume").alias("volume"),
        ).write_csv(sda_path)
        wrote += 1

    logger.info(f"  daily {symbol}: wrote {wrote} file(s) ({df.height} rows each)")

# ── Intraday (one CSV per symbol, concatenated months) ───────────────


async def _fetch_intraday_month(
    symbol: str,
    month: str,
    api_key: str,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
) -> list[dict]:
    """Fetch one month of 1-min bars and return them as a list of row dicts."""
    url = (
        f"{AV_BASE}/query?function=TIME_SERIES_INTRADAY"
        f"&symbol={symbol}&interval=1min&month={month}"
        f"&adjusted=false&outputsize=full&apikey={api_key}"
    )
    try:
        data = await fetch_av_json(url, session, rate_limiter)
    except Exception as e:
        logger.warning(f"  intraday {symbol} {month}: fetch failed: {e}")
        return []

    ts = data.get(_TS_INTRADAY_KEY)
    if not ts:
        return []

    rows: list[dict] = []
    for dt_str, ohlcv in ts.items():
        try:
            rows.append({
                "timestamp": dt_str,
                "open": float(ohlcv["1. open"]),
                "high": float(ohlcv["2. high"]),
                "low": float(ohlcv["3. low"]),
                "close": float(ohlcv["4. close"]),
                "volume": float(ohlcv["5. volume"]),
            })
        except (KeyError, ValueError, TypeError):
            continue
    return rows


async def fetch_and_write_intraday(
    symbol: str,
    ipo_date: date | None,
    delisting_date: date | None,
    api_key: str,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    out_dir: Path,
    cutoff_year: int,
) -> None:
    out_path = out_dir / f"{symbol}_1min.csv"
    if out_path.exists():
        logger.info(f"  intraday {symbol}: file present, skipping")
        return

    ipo_str = ipo_date.isoformat() if ipo_date else None
    delist_str = delisting_date.isoformat() if delisting_date else None
    months = generate_months(ipo_str, delist_str)
    months = [m for m in months if int(m.split("-", 1)[0]) >= cutoff_year]
    if not months:
        logger.info(f"  intraday {symbol}: no months to fetch")
        return

    logger.info(f"  intraday {symbol}: fetching {len(months)} months concurrently")

    # Months fan out concurrently; rate_limiter serialises actual HTTP slot
    # acquisition so the AV per-minute budget is still honoured.
    month_results = await asyncio.gather(
        *[
            _fetch_intraday_month(symbol, m, api_key, session, rate_limiter)
            for m in months
        ]
    )
    all_rows: list[dict] = [row for batch in month_results for row in batch]

    if not all_rows:
        logger.info(f"  intraday {symbol}: 0 rows")
        return

    df = (
        pl.DataFrame(all_rows)
        .unique(subset=["timestamp"])
        .sort("timestamp")
    )
    df.write_csv(out_path)
    logger.info(f"  intraday {symbol}: wrote {df.height} rows")


# ── Orchestration ────────────────────────────────────────────────────


def _wipe_out_dir() -> None:
    """Remove every file directly under OUT_DIR (non-recursive)."""
    if not OUT_DIR.exists():
        return
    removed = 0
    for entry in OUT_DIR.iterdir():
        if entry.is_file():
            entry.unlink()
            removed += 1
    logger.info(f"Wiped {removed} file(s) from {OUT_DIR}")


async def run(api_key: str, tier: str, cutoff_year: int, wipe: bool) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if wipe:
        _wipe_out_dir()

    # 1. LISTING_STATUS (always: needed for ipo/delist dates that drive
    # intraday month generation, even when catalogs already exist).
    av_stocks, av_etfs = fetch_listings(api_key)

    target_stocks = av_stocks.filter(pl.col("symbol").is_in(STOCKS))
    target_etfs = av_etfs.filter(pl.col("symbol").is_in(ETFS))

    missing_stocks = set(STOCKS) - set(target_stocks["symbol"].to_list())
    if missing_stocks:
        logger.warning(f"Stocks missing from AV LISTING_STATUS: {sorted(missing_stocks)}")
    missing_etfs = set(ETFS) - set(target_etfs["symbol"].to_list())
    if missing_etfs:
        logger.warning(f"ETFs missing from AV LISTING_STATUS: {sorted(missing_etfs)}")

    # 2. Catalogs (skip if files already present)
    stock_catalog_path = OUT_DIR / "catalog_stocks.csv"
    etf_catalog_path = OUT_DIR / "catalog_etfs.csv"

    if not stock_catalog_path.exists():
        logger.info(f"Fetching OVERVIEW sectors for {target_stocks.height} stocks...")
        sectors: dict[str, str] = {}
        for sym in target_stocks["symbol"].to_list():
            try:
                sectors[sym] = fetch_sector(api_key, sym)
                logger.info(f"  sector {sym}: {sectors[sym]}")
            except Exception as e:
                logger.warning(f"  sector {sym}: failed ({e}), defaulting to Other")
                sectors[sym] = "Other"
        write_stock_catalog(target_stocks, sectors, stock_catalog_path)
    else:
        logger.info("catalog_stocks.csv present, skipping")

    if not etf_catalog_path.exists():
        write_etf_catalog(target_etfs, etf_catalog_path)
    else:
        logger.info("catalog_etfs.csv present, skipping")

    # 3. Per-symbol time series
    rate_limiter = RateLimiter(calls_per_minute=RATE_BY_TIER[tier])

    rows_to_process = list(target_stocks.iter_rows(named=True)) + list(
        target_etfs.iter_rows(named=True)
    )
    total = len(rows_to_process)

    async with aiohttp.ClientSession() as session:
        for idx, row in enumerate(rows_to_process, 1):
            sym = row["symbol"]
            logger.info(f"[{idx}/{total}] {sym}")
            await fetch_and_write_daily(
                sym, api_key, session, rate_limiter, OUT_DIR, cutoff_year
            )
            await fetch_and_write_intraday(
                sym, row["ipoDate"], row["delistingDate"],
                api_key, session, rate_limiter, OUT_DIR, cutoff_year,
            )

    logger.info(f"Done. Output at {OUT_DIR}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build tests/integration_tests/frd_dir/ mimicking FirstRateData via Alpha Vantage."
    )
    parser.add_argument(
        "--cutoff-year", type=int, default=2000,
        help="Earliest year of data to pull (default: 2000). Raise to shorten run.",
    )
    parser.add_argument(
        "--wipe", action="store_true",
        help="Remove all files in the destination folder before fetching. "
             "Without this flag, existing files are kept and only missing ones are fetched.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    api_key = get_alpha_vantage_key("premium")
    asyncio.run(run(api_key, "premium", args.cutoff_year, args.wipe))


if __name__ == "__main__":
    main()
