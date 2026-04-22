"""Download ETF profile data (ETF_PROFILE) for ETF symbols.

One row per symbol containing sector weights (pivoted to fixed snake_case
columns), holdings as a list-of-structs, and scalar metadata fields.
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

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SECTOR_MAP = {
    "INFORMATION TECHNOLOGY": "information_technology",
    "COMMUNICATION SERVICES": "communication_services",
    "CONSUMER DISCRETIONARY": "consumer_discretionary",
    "CONSUMER STAPLES": "consumer_staples",
    "HEALTHCARE": "healthcare",
    "INDUSTRIALS": "industrials",
    "UTILITIES": "utilities",
    "MATERIALS": "materials",
    "ENERGY": "energy",
    "FINANCIALS": "financials",
    "REAL ESTATE": "real_estate",
}

SECTOR_COLUMNS = list(_SECTOR_MAP.values()) + ["other"]

REQUIRED_KEYS = {
    "net_assets", "net_expense_ratio", "portfolio_turnover",
    "dividend_yield", "inception_date", "leveraged",
    "sectors", "holdings",
}

SCALAR_KEYS = [
    "net_assets", "net_expense_ratio", "portfolio_turnover",
    "dividend_yield", "inception_date", "leveraged",
]

_STRING_KEYS = {"inception_date", "leveraged"}

_NULL_SENTINELS = {None, "None", "n/a", "", "."}

_HOLDINGS_DTYPE = pl.List(pl.Struct({"symbol": pl.Utf8, "weight": pl.Float32}))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean(val):
    """Return None for any null sentinel, else return the value."""
    return None if val in _NULL_SENTINELS else val


def _build_sector_values(
    sectors_raw: list[dict],
    symbol: str,
    asset_type: str,
    issue_tracker: IssueTracker,
) -> dict[str, float | None]:
    """Pivot [{sector, weight}] into a dict of snake_case column -> float."""
    result: dict[str, float | None] = {col: None for col in SECTOR_COLUMNS}
    other_total = 0.0
    has_other = False

    for entry in sectors_raw:
        sector_name = entry.get("sector")
        raw_weight = _clean(entry.get("weight"))

        if raw_weight is None:
            weight = None
        else:
            try:
                weight = float(raw_weight)
            except (ValueError, TypeError):
                issue_tracker.record(
                    symbol, asset_type, "etf_profile",
                    "cast_failure",
                    f"sector weight to Float32 failed: sector={sector_name!r}, value={raw_weight!r}",
                )
                weight = None

        col = _SECTOR_MAP.get(sector_name)
        if col is not None:
            result[col] = weight
        else:
            if weight is not None:
                other_total += weight
                has_other = True

    if has_other:
        result["other"] = other_total

    return result


def _build_holdings(
    holdings_raw: list[dict],
    symbol: str,
    asset_type: str,
    issue_tracker: IssueTracker,
) -> list[dict] | None:
    """Filter and convert holdings into [{symbol, weight}].

    Entries where symbol is a null sentinel are discarded.
    Returns None if the resulting list is empty.
    """
    result: list[dict] = []

    for h in holdings_raw:
        sym = h.get("symbol")
        if sym in _NULL_SENTINELS:
            continue

        raw_weight = _clean(h.get("weight"))
        if raw_weight is None:
            weight = None
        else:
            try:
                weight = float(raw_weight)
            except (ValueError, TypeError):
                issue_tracker.record(
                    symbol, asset_type, "etf_profile",
                    "cast_failure",
                    f"holding weight to Float32 failed: holding={sym!r}, value={raw_weight!r}",
                )
                weight = None

        result.append({"symbol": sym, "weight": weight})

    return result if result else None


def _build_schema() -> dict:
    """Build the explicit Polars schema for the single-row DataFrame."""
    schema: dict = {"date": pl.Date}

    for col in SECTOR_COLUMNS:
        schema[col] = pl.Float32

    schema["holdings"] = _HOLDINGS_DTYPE

    for key in SCALAR_KEYS:
        if key in _STRING_KEYS:
            schema[key] = pl.Utf8
        else:
            schema[key] = pl.Float32

    return schema


# ---------------------------------------------------------------------------
# Main fetch function
# ---------------------------------------------------------------------------

async def fetch_etf_profile(
    catalog_dir: Path,
    historical_dir: Path,
    api_key: str,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    issue_tracker: IssueTracker,
    asset_type: str = "etfs",
) -> None:
    """Download ETF profile data for all symbols of the given asset type."""
    if asset_type != "etfs":
        logger.info(f"etf_profile: skipping asset_type={asset_type!r} (ETFs only)")
        return

    catalog = read_catalog_symbols(catalog_dir, asset_type)
    output_dir = historical_dir / asset_type / "etf_profile"
    output_dir.mkdir(parents=True, exist_ok=True)

    total = catalog.height
    logger.info(f"etf_profile (etfs): {total} symbols to process")

    today = date.today()

    for idx, row in enumerate(catalog.iter_rows(named=True), 1):
        symbol = row["symbol"]
        out_path = output_dir / f"{symbol}.parquet"

        if out_path.exists():
            continue

        url = (
            f"{AV_BASE}/query?function=ETF_PROFILE"
            f"&symbol={symbol}&apikey={api_key}"
        )

        # -- Fetch --
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

        # -- Validate required keys --
        missing = REQUIRED_KEYS - data.keys()
        if missing:
            issue_tracker.record(
                symbol, asset_type, "etf_profile",
                "structure_error", f"missing keys: {missing}",
            )
            del data
            continue

        # -- Process sectors --
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

        # -- Process holdings --
        holdings_list = data.get("holdings", [])
        if not isinstance(holdings_list, list):
            holdings_list = []

        holdings = _build_holdings(
            holdings_list, symbol, asset_type, issue_tracker,
        )

        # -- Process scalar fields --
        row_data: dict = {"date": today}

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

        # -- Build DataFrame and save --
        schema = _build_schema()
        df = pl.DataFrame([row_data], schema=schema)
        df.write_parquet(out_path, compression="zstd")
        logger.info(f"  etf_profile: {symbol} saved etf_profile")
        del df, row_data
