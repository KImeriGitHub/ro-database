"""Daily pull of ETF profile data (ETF_PROFILE).

Identical to the historical endpoint except that the ``date`` column is set to
``folder_date`` rather than today's real date -- keeps resume idempotent with
respect to a single day's folder.
"""

import logging
from datetime import date
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
from historical_data_setup.endpoints.etf_profile import (
    REQUIRED_KEYS,
    SCALAR_KEYS,
    SECTOR_COLUMNS,
    _STRING_KEYS,
    _build_holdings,
    _build_schema,
    _build_sector_values,
    _clean,
)

logger = logging.getLogger(__name__)


async def fetch_etf_profile(
    catalog_dir: Path,
    daily_dir: Path,
    api_key: str,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    issue_tracker: IssueTracker,
    asset_type: str,
    folder_date: date,
    previous_date: date,
    symbols_filter: set[str] | None = None,
) -> None:
    if asset_type != "etfs":
        logger.info(f"etf_profile: skipping asset_type={asset_type!r} (ETFs only)")
        return

    catalog = read_catalog_symbols(catalog_dir, asset_type)
    if symbols_filter is not None:
        catalog = catalog.filter(pl.col("symbol").is_in(list(symbols_filter)))
    output_dir = daily_dir / asset_type / "etf_profile"
    output_dir.mkdir(parents=True, exist_ok=True)

    total = catalog.height
    logger.info(f"etf_profile: {total} symbols to process")

    for idx, row in enumerate(catalog.iter_rows(named=True), 1):
        symbol = row["symbol"]
        out_path = output_dir / f"{symbol}.parquet"

        if out_path.exists():
            continue

        url = (
            f"{AV_BASE}/query?function=ETF_PROFILE"
            f"&symbol={symbol}&apikey={api_key}"
        )

        try:
            data = await fetch_av_json(url, session, rate_limiter)
        except AVResponseError as e:
            issue_tracker.record(
                symbol, asset_type, "etf_profile", "av_throttle", str(e),
            )
            continue
        except Exception as e:
            issue_tracker.record(
                symbol, asset_type, "etf_profile",
                "structure_error", f"fetch failed: {e}",
            )
            continue

        missing = REQUIRED_KEYS - data.keys()
        if missing:
            issue_tracker.record(
                symbol, asset_type, "etf_profile",
                "structure_error", f"missing keys: {missing}",
            )
            del data
            continue

        sectors_list = data.get("sectors", [])
        if not isinstance(sectors_list, list):
            issue_tracker.record(
                symbol, asset_type, "etf_profile",
                "structure_error", "sectors is not a list",
            )
            sectors_list = []

        if not sectors_list:
            issue_tracker.record(
                symbol, asset_type, "etf_profile",
                "empty_content", "empty sectors list",
            )

        sector_values = _build_sector_values(
            sectors_list, symbol, asset_type, issue_tracker,
        )

        holdings_list = data.get("holdings", [])
        if not isinstance(holdings_list, list):
            holdings_list = []

        holdings = _build_holdings(
            holdings_list, symbol, asset_type, issue_tracker,
        )

        row_data: dict = {"date": folder_date}

        for col in SECTOR_COLUMNS:
            row_data[col] = sector_values.get(col)

        row_data["holdings"] = holdings

        for key in SCALAR_KEYS:
            raw = data.get(key)
            val = _clean(raw)

            if val is None or key in _STRING_KEYS:
                row_data[key] = val
            else:
                try:
                    row_data[key] = float(val)
                except (ValueError, TypeError):
                    issue_tracker.record(
                        symbol, asset_type, "etf_profile",
                        "cast_failure",
                        f"scalar {key} to Float32 failed: value={val!r}",
                    )
                    row_data[key] = None

        del data

        schema = _build_schema()
        df = pl.DataFrame([row_data], schema=schema)
        df.write_parquet(out_path, compression="zstd")
        logger.info(f"  etf_profile: {symbol}: saved etf_profile")
        del df, row_data
