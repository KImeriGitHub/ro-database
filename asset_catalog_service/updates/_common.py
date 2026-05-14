"""Shared constants and HTTP helpers for catalog updates."""

import logging
import time
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import requests

logger = logging.getLogger(__name__)

AV_BASE = "https://www.alphavantage.co"

# AV emits a single-key JSON body when it refuses to answer. "Information" and
# "Note" are transient (rate-limit / quota throttling) and worth retrying.
# "Error Message" is a permanent bad-request signal -- retrying won't help.
_AV_THROTTLE_KEYS = ("Information", "Note")
_AV_ERROR_KEY = "Error Message"

_FETCH_MAX_ATTEMPTS = 4
_FETCH_RETRY_BACKOFF = 15.0  # seconds, multiplied by attempt number

COMMODITY_ENTRIES = {
    "XAU": "Gold",
    "XAG": "Silver",
    "WTI": "West Texas Intermediate Crude Oil",
    "BRENT": "Brent Crude Oil",
    "NATURAL_GAS": "Natural Gas",
    "COPPER": "Copper",
    "ALUMINUM": "Aluminum",
    "WHEAT": "Wheat",
    "CORN": "Corn",
    "COTTON": "Cotton",
    "SUGAR": "Sugar",
    "COFFEE": "Coffee",
    "ALL_COMMODITIES": "All Commodities",
}

ECONOMIC_ENTRIES = {
    "REAL_GDP": "Real GDP",
    "REAL_GDP_PER_CAPITA": "Real GDP Per Capita",
    "TREASURY_YIELD_30Y": "Treasury Yield 30 Years",
    "TREASURY_YIELD_10Y": "Treasury Yield 10 Years",
    "TREASURY_YIELD_7Y": "Treasury Yield 7 Years",
    "TREASURY_YIELD_5Y": "Treasury Yield 5 Years",
    "TREASURY_YIELD_2Y": "Treasury Yield 2 Years",
    "TREASURY_YIELD_3M": "Treasury Yield 3 Months",
    "FEDERAL_FUNDS_RATE": "Federal Funds Rate",
    "CPI": "Consumer Price Index",
    "INFLATION": "Inflation",
    "RETAIL_SALES": "Retail Sales",
    "DURABLES": "Durables",
    "UNEMPLOYMENT": "Unemployment",
    "NONFARM_PAYROLL": "Nonfarm Payroll",
}

CANONICAL_SECTORS = [
    "Basic Materials",
    "Communication Services",
    "Consumer Cyclical",
    "Consumer Defensive",
    "Energy",
    "Financial Services",
    "Healthcare",
    "Industrials",
    "Real Estate",
    "Technology",
    "Utilities",
    "Other",
]

# Maps AV OVERVIEW values (uppercase) and FirstRate CSV values (title case)
# to canonical sector names.
_SECTOR_MAP = {
    "BASIC MATERIALS": "Basic Materials",
    "COMMUNICATION SERVICES": "Communication Services",
    "CONSUMER CYCLICAL": "Consumer Cyclical",
    "CONSUMER DEFENSIVE": "Consumer Defensive",
    "CONSUMER STAPLES": "Consumer Defensive",
    "ENERGY": "Energy",
    "FINANCIAL SERVICES": "Financial Services",
    "FINANCIALS": "Financial Services",
    "HEALTHCARE": "Healthcare",
    "INDUSTRIALS": "Industrials",
    "REAL ESTATE": "Real Estate",
    "TECHNOLOGY": "Technology",
    "UTILITIES": "Utilities",
    "NONE": "Other",
    "OTHER": "Other",
    "Basic Materials": "Basic Materials",
    "Communication Services": "Communication Services",
    "Consumer Cyclical": "Consumer Cyclical",
    "Consumer Defensive": "Consumer Defensive",
    "Energy": "Energy",
    "Financial Services": "Financial Services",
    "Healthcare": "Healthcare",
    "Industrials": "Industrials",
    "Real Estate": "Real Estate",
    "Technology": "Technology",
    "Utilities": "Utilities",
}


def normalize_sector(raw: str | None) -> str:
    """Map a raw sector string to a canonical sector name."""
    if raw is None or raw.strip() == "":
        return "Other"
    return _SECTOR_MAP.get(raw.strip(), "Other")


YIELD_ENDPOINTS = [
    "prices",
    "prices_daily",
    "income_statement",
    "balance_sheet",
    "cash_flow",
    "earnings",
    "earnings_estimates",
    "insider",
    "sentiment",
    "etf_profile",
    "direct",
]


class CatalogFetchError(Exception):
    """Raised when a catalog HTTP fetch fails.

    Catalog URLs embed the API key as a ``apikey=...`` query parameter, so
    ``requests``' native exception messages (which echo the request URL) must
    never reach the logs. ``fetch_text`` / ``fetch_json`` translate every
    failure into a ``CatalogFetchError`` whose message contains only the HTTP
    status code or the underlying exception's type name.
    """


def _sanitized_request_error(exc: Exception) -> CatalogFetchError:
    """Build a ``CatalogFetchError`` whose message holds no URL.

    ``requests`` exceptions stringify with the full request URL (and therefore
    the API key) inlined. We extract the only safe-to-log piece -- the status
    code for ``HTTPError``, otherwise the exception class name -- and discard
    the rest. ``raise ... from None`` at the call site suppresses the chained
    cause so ``logger.exception`` tracebacks don't reintroduce the URL either.
    """
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return CatalogFetchError(f"HTTP {exc.response.status_code}")
    return CatalogFetchError(type(exc).__name__)


def _av_throttle_message(data: object) -> str | None:
    """Return the throttle message if ``data`` is an AV throttle body, else None.

    AV throttle bodies are single-key dicts keyed by "Information" or "Note".
    Multi-key responses with the same key (real data) are not throttles.
    """
    if not isinstance(data, dict) or len(data) != 1:
        return None
    for key in _AV_THROTTLE_KEYS:
        if key in data:
            return str(data[key])
    return None


def fetch_text(url: str) -> str:
    """Fetch text from a URL, retrying on AV throttle responses.

    AV emits a small JSON body in place of the expected CSV when throttled.
    That condition is detected, retried with linear backoff, and surfaced as
    a ``CatalogFetchError`` if it persists past the retry budget.
    """
    last_body: str | None = None
    for attempt in range(1, _FETCH_MAX_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
        except requests.RequestException as e:
            raise _sanitized_request_error(e) from None
        text = resp.text.strip()
        if not text.startswith("{"):
            return text
        # AV emits an error JSON (e.g. throttle "Note") in place of the CSV.
        # The body itself doesn't echo the URL, so it's safe to log verbatim.
        last_body = text
        logger.warning(
            f"AV CSV endpoint returned JSON "
            f"(attempt {attempt}/{_FETCH_MAX_ATTEMPTS}): {text[:200]}"
        )
        if attempt < _FETCH_MAX_ATTEMPTS:
            time.sleep(_FETCH_RETRY_BACKOFF * attempt)

    raise CatalogFetchError(
        f"Expected CSV but got JSON after {_FETCH_MAX_ATTEMPTS} attempts: "
        f"{(last_body or '')[:200]}"
    )


def fetch_json(url: str) -> dict:
    """Fetch JSON from a URL, retrying on AV throttle responses.

    AV emits a single-key body (``Information`` or ``Note``) when it refuses
    to answer due to rate limiting; that condition is detected and retried
    with linear backoff. A permanent ``Error Message`` body is surfaced
    immediately without retry. HTTP errors are sanitized so the URL (and the
    API key it embeds) never reach the logs.
    """
    last_throttle: str | None = None
    for attempt in range(1, _FETCH_MAX_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            raise _sanitized_request_error(e) from None

        if isinstance(data, dict) and _AV_ERROR_KEY in data:
            raise CatalogFetchError(
                f"AV error: {str(data[_AV_ERROR_KEY])[:200]}"
            )

        throttle = _av_throttle_message(data)
        if throttle is None:
            return data

        last_throttle = throttle
        logger.warning(
            f"AV throttle (attempt {attempt}/{_FETCH_MAX_ATTEMPTS}): "
            f"{throttle[:200]}"
        )
        if attempt < _FETCH_MAX_ATTEMPTS:
            time.sleep(_FETCH_RETRY_BACKOFF * attempt)

    raise CatalogFetchError(
        f"AV throttle persisted after {_FETCH_MAX_ATTEMPTS} attempts: "
        f"{(last_throttle or '')[:200]}"
    )


def update_simple_catalog(
    label: str, path: Path, fresh: pl.DataFrame
) -> None:
    """Update logic shared by indices, forex, and cryptocurrency catalogs.

    ``fresh`` must have columns: symbol, name.
    The existing parquet has: symbol, name, ipoDate, delistingDate, status.
    """
    existing = pl.read_parquet(path)
    today = date.today()
    one_month_ago = today - timedelta(days=30)

    existing_syms = set(existing["symbol"].to_list())
    fresh_syms = set(fresh["symbol"].to_list())

    added = sorted(fresh_syms - existing_syms)
    missing = sorted(existing_syms - fresh_syms)

    result = existing.clone()

    # 1. New entries
    if added:
        new_rows = fresh.filter(pl.col("symbol").is_in(added)).with_columns(
            pl.lit(None).cast(pl.Date).alias("ipoDate"),
            pl.lit(None).cast(pl.Date).alias("delistingDate"),
            pl.lit(None).cast(pl.Utf8).alias("status"),
        )
        result = pl.concat([result, new_rows], how="vertical_relaxed")
        logger.info(f"{label}: {len(added)} new entries:")
        for row in new_rows.iter_rows(named=True):
            logger.info(f"  + {row['symbol']} ({row['name']})")

    # 2. Missing entries
    if missing:
        missing_rows = result.filter(pl.col("symbol").is_in(missing))

        # 2a. No delistingDate yet -> set today + Corrupted
        no_delist = missing_rows.filter(
            pl.col("delistingDate").is_null()
        )["symbol"].to_list()

        if no_delist:
            logger.info(
                f"{label}: {len(no_delist)} newly missing, "
                f"setting delistingDate={today}, status=Corrupted:"
            )
            for s in sorted(no_delist):
                logger.info(f"  ! {s}")
            result = result.with_columns(
                pl.when(pl.col("symbol").is_in(no_delist))
                .then(pl.lit(today))
                .otherwise(pl.col("delistingDate"))
                .alias("delistingDate"),
                pl.when(pl.col("symbol").is_in(no_delist))
                .then(pl.lit("Corrupted"))
                .otherwise(pl.col("status"))
                .alias("status"),
            )

        # 2b. Has delistingDate older than 1 month -> Delisted
        has_delist = missing_rows.filter(pl.col("delistingDate").is_not_null())
        if has_delist.height > 0:
            old = has_delist.filter(pl.col("delistingDate") < one_month_ago)
            if old.height > 0:
                old_syms = old["symbol"].to_list()
                logger.info(
                    f"{label}: {len(old_syms)} missing > 1 month, "
                    f"marking Delisted:"
                )
                for row in old.iter_rows(named=True):
                    logger.info(
                        f"  x {row['symbol']} "
                        f"(delisted since {row['delistingDate']})"
                    )
                result = result.with_columns(
                    pl.when(pl.col("symbol").is_in(old_syms))
                    .then(pl.lit("Delisted"))
                    .otherwise(pl.col("status"))
                    .alias("status")
                )

    result.write_parquet(path, compression="zstd")
    logger.info(f"{label}: saved ({result.height} total rows)")
