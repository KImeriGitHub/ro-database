"""Download historical insider transaction data (INSIDER_TRANSACTIONS) for stocks.

Only fetched for active symbols. The response contains a flat ``"data"`` list;
each record becomes a row in a single DataFrame per symbol.
"""

import logging
from pathlib import Path

import aiohttp
import polars as pl

from historical_data_setup._common import (
    AV_BASE,
    AVResponseError,
    IssueTracker,
    RateLimiter,
    fetch_av_json,
    read_catalog_symbols,
)

logger = logging.getLogger(__name__)

_NULL_SENTINELS = {None, "None", "", "."}
_STRING_COLUMNS = {"executive", "executive_title", "security_type", "acquisition_or_disposal"}


async def fetch_insider(
    catalog_dir: Path,
    historical_dir: Path,
    api_key: str,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    issue_tracker: IssueTracker,
    asset_type: str = "stocks",
) -> None:
    """Download insider transaction data for all active symbols of the given asset type."""
    catalog = read_catalog_symbols(catalog_dir, asset_type)
    catalog = catalog.filter(pl.col("status") == "Active")
    output_dir = historical_dir / asset_type / "insider"
    output_dir.mkdir(parents=True, exist_ok=True)

    total = catalog.height
    logger.info(f"insider ({asset_type}): {total} active symbols to process")

    for idx, row in enumerate(catalog.iter_rows(named=True), 1):
        symbol = row["symbol"]
        out_path = output_dir / f"{symbol}.parquet"

        if out_path.exists():
            continue

        logger.info(f"[{idx}/{total}] {symbol}")

        url = (
            f"{AV_BASE}/query?function=INSIDER_TRANSACTIONS"
            f"&symbol={symbol}&apikey={api_key}"
        )

        try:
            data = await fetch_av_json(url, session, rate_limiter)
        except AVResponseError as e:
            issue_tracker.record(
                symbol, asset_type, "insider", "av_throttle", str(e),
            )
            continue
        except Exception as e:
            issue_tracker.record(
                symbol, asset_type, "insider",
                "structure_error", f"fetch failed: {e}",
            )
            continue

        # Validate top-level key
        if "data" not in data or not isinstance(data["data"], list):
            issue_tracker.record(
                symbol, asset_type, "insider",
                "structure_error", "missing or invalid 'data' key",
            )
            del data
            continue

        records = data["data"]
        del data

        if not records:
            issue_tracker.record(
                symbol, asset_type, "insider",
                "empty_content", "empty data list",
            )
            continue

        # Rename transaction_date -> transactionDate, drop ticker,
        # replace "None" strings with actual None
        cleaned: list[dict] = []
        for rec in records:
            mapped: dict = {}
            for k, v in rec.items():
                if k == "ticker":
                    continue
                val = None if v in _NULL_SENTINELS else v
                if k == "transaction_date":
                    mapped["transactionDate"] = val
                else:
                    mapped[k] = val
            cleaned.append(mapped)

        del records

        # Build all-String DataFrame
        df = pl.DataFrame(cleaned, infer_schema_length=0)
        del cleaned

        # Cast transactionDate (required)
        if "transactionDate" not in df.columns:
            issue_tracker.record(
                symbol, asset_type, "insider",
                "structure_error", "missing transactionDate column",
            )
            del df
            continue

        try:
            df = df.with_columns(
                pl.col("transactionDate").str.to_date("%Y-%m-%d")
            )
        except Exception as e:
            issue_tracker.record(
                symbol, asset_type, "insider",
                "cast_failure", f"transactionDate to Date failed: {e}",
            )
            del df
            continue

        # Cast remaining columns: known strings stay, all others must be Float32
        for col_name in df.columns:
            if col_name == "transactionDate":
                continue
            if col_name in _STRING_COLUMNS:
                continue
            try:
                df = df.with_columns(pl.col(col_name).cast(pl.Float32))
            except Exception as e:
                # Force cast: non-castable values become null
                df = df.with_columns(
                    pl.col(col_name).cast(pl.Float32, strict=False)
                )
                issue_tracker.record(
                    symbol, asset_type, "insider",
                    "cast_failure",
                    f"{col_name} to Float32 had non-castable values "
                    f"(forced to null): {e}",
                )

        df = df.sort("transactionDate")
        df.write_parquet(out_path, compression="zstd")
        logger.info(f"  {symbol}: saved {df.height} rows")
        del df
