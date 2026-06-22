"""Create or update stocks.parquet and etfs.parquet.

Init (init_stocks_etfs): builds catalogs from AV LISTING_STATUS, optionally
merged with FirstRate Data CSVs.  Sectors come from FirstRate CSV or AV
OVERVIEW queries.

Update (update_stocks_etfs): daily incremental update.  Queries OVERVIEW for
any newly-appeared stock symbols to populate their sector.
"""

import io
import logging
import time
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from asset_catalog_service.updates._common import (
    AV_BASE,
    CatalogFetchError,
    fetch_json,
    fetch_text,
    normalize_sector,
    with_network_retry,
)

logger = logging.getLogger(__name__)

_STOCKS_REQUIRED_HEADERS = {"Ticker", "Company Name", "Sector", "IPO Date", "Status"}
_ETFS_REQUIRED_HEADERS = {"Ticker", "Name", "IPO Date", "Status"}

_RATE_BATCH = 70  # queries per minute (2 under limit for margin)

_SECTOR_FETCH_MAX_ATTEMPTS = 3
_SECTOR_FETCH_RETRY_BACKOFF = 5.0  # seconds, multiplied by attempt number

# ── Validation ───────────────────────────────────────────────────────


def validate_firstrate_csvs(
    stocks_dir: Path | None, etfs_dir: Path | None
) -> None:
    """Validate that FirstRate CSVs exist and have required headers.

    Both directories (if provided) are checked before raising, so the
    caller sees all problems at once.  Raises ``ValueError`` on failure.
    """
    errors: list[str] = []

    if stocks_dir is not None:
        csv_path = stocks_dir / "catalog_stocks.csv"
        if not csv_path.exists():
            errors.append(f"catalog_stocks.csv not found in {stocks_dir}")
        else:
            with open(csv_path, "r", encoding="utf-8") as f:
                header_line = f.readline()
            headers = {h.strip() for h in header_line.split(",")}
            missing = _STOCKS_REQUIRED_HEADERS - headers
            if missing:
                errors.append(
                    f"catalog_stocks.csv missing required headers: "
                    f"{sorted(missing)}"
                )

    if etfs_dir is not None:
        csv_path = etfs_dir / "catalog_etfs.csv"
        if not csv_path.exists():
            errors.append(f"catalog_etfs.csv not found in {etfs_dir}")
        else:
            with open(csv_path, "r", encoding="utf-8") as f:
                header_line = f.readline()
            headers = {h.strip() for h in header_line.split(",")}
            missing = _ETFS_REQUIRED_HEADERS - headers
            if missing:
                errors.append(
                    f"catalog_etfs.csv missing required headers: "
                    f"{sorted(missing)}"
                )

    if errors:
        raise ValueError(
            "FirstRate CSV validation failed:\n  " + "\n  ".join(errors)
        )


# ── AV helpers ───────────────────────────────────────────────────────


def _fetch_av_listings(api_key: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Fetch LISTING_STATUS (active + delisted), return (stocks, etfs).

    Returned DataFrames have columns: symbol, name, ipoDate, delistingDate,
    status.  ``exchange`` and ``assetType`` are dropped.

    A ticker can appear in both active and delisted lists when the symbol
    was re-issued (old company delisted, new company now trades the same
    ticker string).  The active row wins for ``name``, ``status``, and
    ``delistingDate`` (so the catalog reflects the currently-trading entity),
    but ``ipoDate`` is the minimum across the active and delisted rows so
    the earliest date for which any data may exist under the ticker is kept.
    """
    logger.info("Fetching LISTING_STATUS (active + delisted)...")
    active_csv = with_network_retry(
        fetch_text,
        f"{AV_BASE}/query?function=LISTING_STATUS&state=active&apikey={api_key}",
        label="LISTING_STATUS active",
    )
    delisted_csv = with_network_retry(
        fetch_text,
        f"{AV_BASE}/query?function=LISTING_STATUS&state=delisted&apikey={api_key}",
        label="LISTING_STATUS delisted",
    )

    active_df = pl.read_csv(
        io.StringIO(active_csv), null_values=["null"], infer_schema_length=0
    )
    delisted_df = pl.read_csv(
        io.StringIO(delisted_csv), null_values=["null"], infer_schema_length=0
    )
    # Active rows go first so unique(keep="first") prefers them on collision.
    combined = pl.concat([active_df, delisted_df], how="vertical_relaxed")

    return (
        _collapse_listings(combined, "Stock", "stocks"),
        _collapse_listings(combined, "ETF", "etfs"),
    )


def _collapse_listings(
    combined: pl.DataFrame, asset_type: str, label: str
) -> pl.DataFrame:
    """Filter to one assetType and dedup by symbol, keeping earliest ipoDate.

    For re-issued tickers (same symbol present in both active and delisted),
    the active row wins for every column except ipoDate, which is replaced
    with the minimum ipoDate seen across all rows for that symbol.
    """
    pre = combined.filter(pl.col("assetType") == asset_type).with_columns(
        pl.col("ipoDate").cast(pl.Date, strict=False),
        pl.col("delistingDate").cast(pl.Date, strict=False),
    )
    min_ipo = pre.group_by("symbol").agg(
        pl.col("ipoDate").min().alias("_min_ipo")
    )
    collapsed = (
        pre
        .unique(subset=["symbol"], keep="first", maintain_order=True)
        .join(min_ipo, on="symbol", how="left")
        .with_columns(pl.col("_min_ipo").alias("ipoDate"))
        .drop("_min_ipo")
        .select("symbol", "name", "ipoDate", "delistingDate", "status")
    )
    if collapsed.height < pre.height:
        logger.info(
            f"{label}: collapsed {pre.height - collapsed.height} duplicate "
            f"symbols from LISTING_STATUS (re-issued tickers; kept active row, "
            f"earliest ipoDate)"
        )
    return collapsed


def _fetch_sector(api_key: str, symbol: str) -> str:
    """Query AV OVERVIEW for a single symbol's sector."""
    url = f"{AV_BASE}/query?function=OVERVIEW&symbol={symbol}&apikey={api_key}"
    data = fetch_json(url)
    raw = data.get("Sector")
    return normalize_sector(raw)


def _fetch_sectors_batch(
    api_key: str, symbols: list[str]
) -> dict[str, str]:
    """Query OVERVIEW for each symbol, rate-limited to 74 calls/min."""
    results: dict[str, str] = {}
    total = len(symbols)
    batch_start = time.monotonic()

    for i, symbol in enumerate(symbols):
        if i > 0 and i % _RATE_BATCH == 0:
            elapsed = time.monotonic() - batch_start
            if elapsed < 60:
                sleep_time = 60 - elapsed + 0.1 # tiny buffer
                logger.info(
                    f"Rate limit pause: sleeping {sleep_time:.1f}s "
                    f"({i}/{total} complete)"
                )
                time.sleep(sleep_time)
            batch_start = time.monotonic()

        results[symbol] = _fetch_sector_with_retry(api_key, symbol)

        if i > 0 and i % 500 == 0:
            logger.info(f"Sector fetch progress: {i}/{total}")

    logger.info(f"Sector fetch complete: {total} symbols.")
    return results


def _fetch_sector_with_retry(api_key: str, symbol: str) -> str:
    """Wrap ``_fetch_sector`` with retries; fall back to ``"Other"`` on failure."""
    try:
        return with_network_retry(
            _fetch_sector,
            api_key,
            symbol,
            max_attempts=_SECTOR_FETCH_MAX_ATTEMPTS,
            backoff=_SECTOR_FETCH_RETRY_BACKOFF,
            label=symbol,
        )
    except CatalogFetchError:
        logger.warning(
            f"Giving up on {symbol} after {_SECTOR_FETCH_MAX_ATTEMPTS} "
            f"attempts. Defaulting to Other."
        )
        return "Other"

# ── FirstRate loaders ────────────────────────────────────────────────


def _load_firstrate_stocks(stocks_dir: Path) -> pl.DataFrame:
    """Load and normalise catalog_stocks.csv from a FirstRate directory."""
    csv_path = stocks_dir / "catalog_stocks.csv"
    df = pl.read_csv(csv_path, infer_schema_length=0, null_values=[""])

    # Trim header whitespace that FirstRate CSVs sometimes include
    df = df.rename({c: c.strip() for c in df.columns})

    rename_map = {
        "Ticker": "symbol",
        "Company Name": "name",
        "Sector": "sector",
        "IPO Date": "ipoDate",
        "Status": "status",
    }
    select_cols = list(rename_map.values())
    df = df.rename(rename_map)

    if "Delisting Date" in df.columns:
        df = df.rename({"Delisting Date": "delistingDate"})
        select_cols.append("delistingDate")

    df = df.select(select_cols)

    # Normalise sector values
    df = df.with_columns(
        pl.col("sector").map_elements(normalize_sector, return_dtype=pl.Utf8)
    )

    # Cast dates
    df = df.with_columns(
        pl.col("ipoDate").cast(pl.Date, strict=False),
    )
    if "delistingDate" in df.columns:
        df = df.with_columns(
            pl.col("delistingDate").cast(pl.Date, strict=False),
        )
    else:
        df = df.with_columns(
            pl.lit(None).cast(pl.Date).alias("delistingDate"),
        )

    return df.select("symbol", "name", "sector", "ipoDate", "delistingDate", "status")


def _load_firstrate_etfs(etfs_dir: Path) -> pl.DataFrame:
    """Load and normalise catalog_etfs.csv from a FirstRate directory."""
    csv_path = etfs_dir / "catalog_etfs.csv"
    df = pl.read_csv(csv_path, infer_schema_length=0, null_values=[""])

    df = df.rename({c: c.strip() for c in df.columns})

    rename_map = {
        "Ticker": "symbol",
        "Name": "name",
        "IPO Date": "ipoDate",
        "Status": "status",
    }
    select_cols = list(rename_map.values())
    df = df.rename(rename_map)

    if "Delisting Date" in df.columns:
        df = df.rename({"Delisting Date": "delistingDate"})
        select_cols.append("delistingDate")

    df = df.select(select_cols)

    df = df.with_columns(
        pl.col("ipoDate").cast(pl.Date, strict=False),
    )
    if "delistingDate" in df.columns:
        df = df.with_columns(
            pl.col("delistingDate").cast(pl.Date, strict=False),
        )
    else:
        df = df.with_columns(
            pl.lit(None).cast(pl.Date).alias("delistingDate"),
        )

    return df.select("symbol", "name", "ipoDate", "delistingDate", "status")


# ── Merge logic ──────────────────────────────────────────────────────


def _merge_stocks(
    av: pl.DataFrame, fr: pl.DataFrame
) -> pl.DataFrame:
    """Merge AV and FirstRate stock listings.  FirstRate takes precedence."""
    av_syms = set(av["symbol"].to_list())
    fr_syms = set(fr["symbol"].to_list())

    only_fr = sorted(fr_syms - av_syms)
    only_av = sorted(av_syms - fr_syms)
    common = sorted(av_syms & fr_syms)

    if only_fr:
        logger.info(
            f"stocks: {len(only_fr)} symbols in FirstRate but not AV "
            f"(first 10: {only_fr[:10]})"
        )
    if only_av:
        logger.info(
            f"stocks: {len(only_av)} symbols in AV but not FirstRate "
            f"(first 10: {only_av[:10]})"
        )

    # Log status disagreements for common symbols
    if common:
        av_common = av.filter(pl.col("symbol").is_in(common)).select(
            "symbol", pl.col("status").alias("_av_status")
        )
        fr_common = fr.filter(pl.col("symbol").is_in(common)).select(
            "symbol", pl.col("status").alias("_fr_status")
        )
        merged = av_common.join(fr_common, on="symbol", how="inner")
        disagree = merged.filter(pl.col("_av_status") != pl.col("_fr_status"))
        if disagree.height > 0:
            logger.info(
                f"stocks: {disagree.height} status disagreements "
                f"(FirstRate takes precedence):"
            )
            for row in disagree.iter_rows(named=True):
                logger.info(
                    f"  {row['symbol']}: "
                    f"AV={row['_av_status']} vs FR={row['_fr_status']}"
                )

    # Build result: FirstRate rows for common + FR-only, AV rows for AV-only
    fr_rows = fr  # all FirstRate symbols (covers common + FR-only)
    av_only_rows = av.filter(pl.col("symbol").is_in(only_av))
    # AV-only rows need a sector column
    av_only_rows = av_only_rows.with_columns(
        pl.lit(None).cast(pl.Utf8).alias("sector"),
    ).select("symbol", "name", "sector", "ipoDate", "delistingDate", "status")

    return pl.concat([fr_rows, av_only_rows], how="vertical_relaxed")


def _merge_etfs(
    av: pl.DataFrame, fr: pl.DataFrame
) -> pl.DataFrame:
    """Merge AV and FirstRate ETF listings.  FirstRate takes precedence."""
    av_syms = set(av["symbol"].to_list())
    fr_syms = set(fr["symbol"].to_list())

    only_fr = sorted(fr_syms - av_syms)
    only_av = sorted(av_syms - fr_syms)
    common = sorted(av_syms & fr_syms)

    if only_fr:
        logger.info(
            f"etfs: {len(only_fr)} symbols in FirstRate but not AV "
            f"(first 10: {only_fr[:10]})"
        )
    if only_av:
        logger.info(
            f"etfs: {len(only_av)} symbols in AV but not FirstRate "
            f"(first 10: {only_av[:10]})"
        )

    if common:
        av_common = av.filter(pl.col("symbol").is_in(common)).select(
            "symbol", pl.col("status").alias("_av_status")
        )
        fr_common = fr.filter(pl.col("symbol").is_in(common)).select(
            "symbol", pl.col("status").alias("_fr_status")
        )
        merged = av_common.join(fr_common, on="symbol", how="inner")
        disagree = merged.filter(pl.col("_av_status") != pl.col("_fr_status"))
        if disagree.height > 0:
            logger.info(
                f"etfs: {disagree.height} status disagreements "
                f"(FirstRate takes precedence):"
            )
            for row in disagree.iter_rows(named=True):
                logger.info(
                    f"  {row['symbol']}: "
                    f"AV={row['_av_status']} vs FR={row['_fr_status']}"
                )

    fr_rows = fr
    av_only_rows = av.filter(pl.col("symbol").is_in(only_av))

    return pl.concat([fr_rows, av_only_rows], how="vertical_relaxed")


# ── Init ─────────────────────────────────────────────────────────────


def init_stocks_etfs(
    api_key: str,
    catalog_dir: Path,
    stocks_dir: Path | None = None,
    etfs_dir: Path | None = None,
) -> None:
    """Initial setup for stocks.parquet and etfs.parquet."""
    stocks_path = catalog_dir / "stocks.parquet"
    etfs_path = catalog_dir / "etfs.parquet"

    av_stocks, av_etfs = _fetch_av_listings(api_key)

    # ── stocks ──
    if stocks_dir is not None:
        fr_stocks = _load_firstrate_stocks(stocks_dir)
        stocks = _merge_stocks(av_stocks, fr_stocks)
    else:
        stocks = av_stocks.with_columns(
            pl.lit(None).cast(pl.Utf8).alias("sector"),
        ).select("symbol", "name", "sector", "ipoDate", "delistingDate", "status")

    # Query OVERVIEW for stocks still missing a sector
    missing_sector = stocks.filter(pl.col("sector").is_null())["symbol"].to_list()
    if missing_sector:
        logger.info(
            f"stocks: {len(missing_sector)} symbols need OVERVIEW for sector"
        )
        sectors = _fetch_sectors_batch(api_key, missing_sector)
        sector_df = pl.DataFrame(
            {"symbol": list(sectors.keys()), "_sector": list(sectors.values())}
        )
        stocks = (
            stocks.join(sector_df, on="symbol", how="left")
            .with_columns(
                pl.coalesce("sector", "_sector").alias("sector"),
            )
            .drop("_sector")
        )

    stocks.write_parquet(stocks_path, compression="zstd")
    logger.info(f"Established stocks.parquet ({stocks.height} rows)")

    # ── etfs ──
    if etfs_dir is not None:
        fr_etfs = _load_firstrate_etfs(etfs_dir)
        etfs = _merge_etfs(av_etfs, fr_etfs)
    else:
        etfs = av_etfs

    etfs.write_parquet(etfs_path, compression="zstd")
    logger.info(f"Established etfs.parquet ({etfs.height} rows)")


# ── Update ───────────────────────────────────────────────────────────


def update_stocks_etfs(api_key: str, catalog_dir: Path) -> None:
    """Daily update for stocks.parquet and etfs.parquet."""
    stocks_path = catalog_dir / "stocks.parquet"
    etfs_path = catalog_dir / "etfs.parquet"

    if not stocks_path.exists() or not etfs_path.exists():
        raise FileNotFoundError(
            "stocks.parquet and/or etfs.parquet not found. "
            "Run init_catalog.py first."
        )

    av_stocks, av_etfs = _fetch_av_listings(api_key)

    # AV sometimes classifies the same symbol as both a stock and an ETF in one
    # run. We do not drop either side; just surface the collision for review.
    stock_syms = set(av_stocks["symbol"].to_list())
    etf_syms = set(av_etfs["symbol"].to_list())
    collisions = sorted(stock_syms & etf_syms)
    if collisions:
        logger.warning(
            f"stocks/etfs: {len(collisions)} symbols classified as both stock "
            f"and ETF this run (kept in both catalogs): {collisions}"
        )

    # Active symbols on the opposite asset type, used by _update_listing to
    # fast-path a vanished symbol to Delisted when it has been reissued under
    # the other type (e.g. an old ETF whose ticker is now an active stock).
    active_stock_syms = set(
        av_stocks.filter(pl.col("status") == "Active")["symbol"].to_list()
    )
    active_etf_syms = set(
        av_etfs.filter(pl.col("status") == "Active")["symbol"].to_list()
    )

    _update_listing("stocks", stocks_path, av_stocks, api_key, active_etf_syms)
    _update_listing("etfs", etfs_path, av_etfs, None, active_stock_syms)


def _update_listing(
    label: str,
    path: Path,
    fresh: pl.DataFrame,
    api_key: str | None,
    other_active_syms: set[str],
) -> None:
    """Compare existing listing catalog with fresh data and apply changes."""
    existing = pl.read_parquet(path)
    today = date.today()
    one_month_ago = today - timedelta(days=30)

    has_sector = "sector" in existing.columns

    existing_syms = set(existing["symbol"].to_list())
    fresh_syms = set(fresh["symbol"].to_list())

    added = sorted(fresh_syms - existing_syms)
    vanished = sorted(existing_syms - fresh_syms)
    common = sorted(existing_syms & fresh_syms)

    result = existing.clone()

    # 1. New symbols
    if added:
        new_rows = fresh.filter(pl.col("symbol").is_in(added))

        if has_sector and api_key is not None:
            # Same path as init: 70/min pacing + per-symbol retry + "Other" fallback.
            sectors = _fetch_sectors_batch(api_key, added)
            sector_df = pl.DataFrame(
                {"symbol": list(sectors.keys()), "sector": list(sectors.values())}
            )
            new_rows = new_rows.join(sector_df, on="symbol", how="left")
        elif has_sector:
            new_rows = new_rows.with_columns(
                pl.lit("Other").alias("sector"),
            )

        # Ensure column order matches existing
        new_rows = new_rows.select(existing.columns)
        result = pl.concat([result, new_rows], how="vertical_relaxed")

        logger.info(f"{label}: {len(added)} new entries:")
        for row in new_rows.iter_rows(named=True):
            sector_part = f" | sector={row['sector']}" if has_sector else ""
            logger.info(
                f"  + {row['symbol']} | {row['name']}{sector_part} "
                f"| ipo={row['ipoDate']} | status={row['status']}"
            )

    # 2. Vanished symbols.
    #
    # The "other-set" (other_active_syms) is the set of symbols currently
    # Active on the opposite asset type this run. Membership means the ticker
    # has been reissued under the other type (e.g. an old ETF whose ticker now
    # trades as an active stock), so the vanished side should delist
    # immediately rather than sit in Corrupted probation.
    if vanished:
        vanished_rows = result.filter(pl.col("symbol").is_in(vanished))

        # 2a. No delistingDate yet -> set today.
        #     In other-set -> Delisted (reissue); else Corrupted (probation).
        no_delist_rows = vanished_rows.filter(pl.col("delistingDate").is_null())
        no_delist = no_delist_rows["symbol"].to_list()

        new_delisted = sorted(
            s for s in no_delist if s in other_active_syms
        )
        new_corrupted = sorted(
            s for s in no_delist if s not in other_active_syms
        )

        if no_delist:
            logger.info(
                f"{label}: {len(no_delist)} vanished without delistingDate, "
                f"setting delistingDate={today}:"
            )
            for s in new_corrupted:
                logger.info(f"  ! {s} -> Corrupted")
            for s in new_delisted:
                logger.info(f"  x {s} -> Delisted (reissued under other type)")
            result = result.with_columns(
                pl.when(pl.col("symbol").is_in(no_delist))
                .then(pl.lit(today))
                .otherwise(pl.col("delistingDate"))
                .alias("delistingDate"),
                pl.when(pl.col("symbol").is_in(new_delisted))
                .then(pl.lit("Delisted"))
                .when(pl.col("symbol").is_in(new_corrupted))
                .then(pl.lit("Corrupted"))
                .otherwise(pl.col("status"))
                .alias("status"),
            )

        # 2b. Has delistingDate and still Corrupted -> promote to Delisted when
        #     either 30+ days have passed, or the symbol is in the other-set
        #     (reissue fast-path). Already-Delisted rows are terminal; skip.
        corrupted_dated = vanished_rows.filter(
            pl.col("delistingDate").is_not_null()
            & (pl.col("status") == "Corrupted")
        )
        if corrupted_dated.height > 0:
            promote = corrupted_dated.filter(
                (pl.col("delistingDate") < one_month_ago)
                | pl.col("symbol").is_in(other_active_syms)
            )
            if promote.height > 0:
                promote_syms = promote["symbol"].to_list()
                logger.info(
                    f"{label}: {len(promote_syms)} Corrupted vanished promoted "
                    f"to Delisted:"
                )
                for row in promote.iter_rows(named=True):
                    reason = (
                        "reissued under other type"
                        if row["symbol"] in other_active_syms
                        else f"delisted since {row['delistingDate']}"
                    )
                    logger.info(f"  x {row['symbol']} ({reason})")
                result = result.with_columns(
                    pl.when(pl.col("symbol").is_in(promote_syms))
                    .then(pl.lit("Delisted"))
                    .otherwise(pl.col("status"))
                    .alias("status")
                )

    # 3. Check common symbols for ipoDate / delistingDate changes
    if common:
        fresh_common = fresh.filter(pl.col("symbol").is_in(common)).select(
            "symbol",
            pl.col("ipoDate").alias("_ipo_new"),
            pl.col("delistingDate").alias("_delist_new"),
            pl.col("status").alias("_status_new"),
        )
        merged = (
            result.filter(pl.col("symbol").is_in(common))
            .join(fresh_common, on="symbol", how="left")
        )

        # 3a. ipoDate moved earlier (both non-null) -> update the date only.
        # Status is left untouched: the catalog already tolerates re-issued
        # tickers (see _collapse_listings), so an earlier ipoDate is treated
        # as new information, not corruption.  Fresh later than existing is
        # ignored: we keep the earliest known date so we never lose a prior
        # issuer's data window.  Without the update, the same change would
        # be re-detected on every run.
        ipo_changed = merged.filter(
            pl.col("ipoDate").is_not_null()
            & pl.col("_ipo_new").is_not_null()
            & (pl.col("_ipo_new") < pl.col("ipoDate"))
        )
        if ipo_changed.height > 0:
            ipo_syms = ipo_changed["symbol"].to_list()
            logger.info(
                f"{label}: {len(ipo_syms)} ipoDate moved earlier, updating:"
            )
            for row in ipo_changed.iter_rows(named=True):
                logger.info(
                    f"  ! {row['symbol']}: ipo {row['ipoDate']} -> {row['_ipo_new']}"
                )
            ipo_updates = ipo_changed.select(
                "symbol", pl.col("_ipo_new").alias("_ipo_upd")
            )
            result = (
                result.join(ipo_updates, on="symbol", how="left")
                .with_columns(
                    pl.when(pl.col("_ipo_upd").is_not_null())
                    .then(pl.col("_ipo_upd"))
                    .otherwise(pl.col("ipoDate"))
                    .alias("ipoDate"),
                )
                .drop("_ipo_upd")
            )

        # 3a2. ipoDate was null, now has a value -> update it
        ipo_filled = merged.filter(
            pl.col("ipoDate").is_null()
            & pl.col("_ipo_new").is_not_null()
        )
        if ipo_filled.height > 0:
            filled_syms = ipo_filled["symbol"].to_list()
            logger.info(
                f"{label}: {len(filled_syms)} ipoDate filled from null:"
            )
            for row in ipo_filled.iter_rows(named=True):
                logger.info(
                    f"  ~ {row['symbol']}: null -> {row['_ipo_new']}"
                )
            ipo_updates = ipo_filled.select(
                "symbol", pl.col("_ipo_new").alias("_ipo_upd")
            )
            result = (
                result.join(ipo_updates, on="symbol", how="left")
                .with_columns(
                    pl.when(pl.col("_ipo_upd").is_not_null())
                    .then(pl.col("_ipo_upd"))
                    .otherwise(pl.col("ipoDate"))
                    .alias("ipoDate")
                )
                .drop("_ipo_upd")
            )

        # 3b. delistingDate changed -> update value
        delist_changed = merged.filter(
            (pl.col("delistingDate") != pl.col("_delist_new"))
            | (
                pl.col("delistingDate").is_null()
                & pl.col("_delist_new").is_not_null()
            )
            | (
                pl.col("delistingDate").is_not_null()
                & pl.col("_delist_new").is_null()
            )
        )
        if delist_changed.height > 0:
            delist_syms = delist_changed["symbol"].to_list()
            logger.info(f"{label}: {len(delist_syms)} delistingDate changes:")
            for row in delist_changed.iter_rows(named=True):
                logger.info(
                    f"  ~ {row['symbol']}: "
                    f"{row['delistingDate']} -> {row['_delist_new']}"
                )
            updates = delist_changed.select(
                "symbol", pl.col("_delist_new").alias("_upd")
            )
            result = (
                result.join(updates, on="symbol", how="left")
                .with_columns(
                    pl.when(pl.col("symbol").is_in(delist_syms))
                    .then(pl.col("_upd"))
                    .otherwise(pl.col("delistingDate"))
                    .alias("delistingDate")
                )
                .drop("_upd")
            )

        # 3c. Status sync from fresh.
        #
        # Re-listed: a previously Corrupted/Delisted symbol that AV now
        # reports back in the active LISTING_STATUS (_status_new == "Active")
        # is reset to Active. Block 3b already cleared delistingDate.
        #
        # Newly delisted: an Active symbol that AV now reports in the
        # delisted LISTING_STATUS (_status_new == "Delisted") is promoted
        # to Delisted. This complements block 2b (Corrupted -> Delisted after
        # 30 days of being missing); block 2b only fires when the symbol has
        # vanished from both lists, while this fires when AV explicitly
        # confirms the delisting.
        relisted = merged.filter(
            pl.col("status").is_in(["Corrupted", "Delisted"])
            & (pl.col("_status_new") == "Active")
        )
        delisted_now = merged.filter(
            ~pl.col("status").is_in(["Corrupted", "Delisted"])
            & (pl.col("_status_new") == "Delisted")
        )
        relisted_syms = relisted["symbol"].to_list()
        delisted_now_syms = delisted_now["symbol"].to_list()
        if relisted_syms:
            logger.info(
                f"{label}: {len(relisted_syms)} re-listed, status -> Active:"
            )
            for row in relisted.iter_rows(named=True):
                logger.info(f"  ^ {row['symbol']} (was {row['status']})")
        if delisted_now_syms:
            logger.info(
                f"{label}: {len(delisted_now_syms)} newly delisted, "
                f"status -> Delisted:"
            )
            for row in delisted_now.iter_rows(named=True):
                logger.info(
                    f"  v {row['symbol']} (delistingDate={row['_delist_new']})"
                )
        if relisted_syms or delisted_now_syms:
            result = result.with_columns(
                pl.when(pl.col("symbol").is_in(relisted_syms))
                .then(pl.lit("Active"))
                .when(pl.col("symbol").is_in(delisted_now_syms))
                .then(pl.lit("Delisted"))
                .otherwise(pl.col("status"))
                .alias("status")
            )

    result.write_parquet(path, compression="zstd")
    logger.info(f"{label}: saved ({result.height} total rows)")
