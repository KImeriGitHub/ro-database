"""Shared helpers for historical data setup: rate limiter, HTTP fetch, month
generation, issue tracking, and catalog reading."""

import asyncio
import logging
import re
import time
from collections import deque
from datetime import date, datetime
from pathlib import Path

import aiohttp
import polars as pl

logger = logging.getLogger(__name__)

AV_BASE = "https://www.alphavantage.co"


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------

class RateLimiter:
    """Sliding-window rate limiter for API calls.

    Tracks timestamps of the last *calls_per_minute* calls. A caller awaits
    ``wait()`` which returns immediately if there are fewer than
    *calls_per_minute* calls within the trailing *window* seconds, otherwise
    sleeps until the oldest call falls out of the window.

    Shared safely across all endpoint coroutines running in a single event
    loop: an ``asyncio.Lock`` serialises registration so the budget is
    enforced globally.
    """

    def __init__(
        self,
        calls_per_minute: float = 74.0,
        window: float = 60.0,
        min_gap: float = 0.6,
    ):
        self._max_calls = int(calls_per_minute)
        self._window = window
        self._min_gap = min_gap
        self._timestamps: deque[float] = deque()
        self._lock = asyncio.Lock()

        # Calculate the theoretical maximum gap allowed to still hit the target rate
        max_allowable_gap = self._window / float(self._max_calls)

        if self._min_gap >= max_allowable_gap:
            logger.warning(
                f"RateLimiter: min_gap ({self._min_gap}s) is too large to allow "
                f"{self._max_calls} calls within {self._window}s. "
                f"Reducing min_gap to {max_allowable_gap * 0.9:.2f}s."
            )
            # Cap the min_gap at 90% of the average to ensure the throughput is possible
            self._min_gap = max_allowable_gap * 0.9

    async def wait(self) -> None:
        """Block until a call slot is available in the sliding window."""
        async with self._lock:
            while True:
                now = time.monotonic()
                cutoff = now - self._window
                # evicting all timestamps that have slid out
                while self._timestamps and self._timestamps[0] <= cutoff:
                    self._timestamps.popleft()

                if len(self._timestamps) < self._max_calls:
                    # Enforce minimum inter-call spacing to avoid micro-bursts
                    if self._timestamps:
                        gap = now - self._timestamps[-1]
                        if gap < self._min_gap:
                            await asyncio.sleep(self._min_gap - gap)
                            continue
                    self._timestamps.append(now)
                    return

                # Too many timestamps in the window
                sleep_for = self._timestamps[0] + self._window - now
                if sleep_for > 0:
                    await asyncio.sleep(sleep_for)


# ---------------------------------------------------------------------------
# HTTP fetch with rate limiting + AV throttle detection
# ---------------------------------------------------------------------------

class AVResponseError(Exception):
    """Raised when Alpha Vantage returns an unrecoverable error."""


# Module-level counter incremented inside ``fetch_av_json`` once per HTTP
# request actually issued (including retries). Lets the monitoring service
# report API budget usage at the end of an in-process run. Resets are the
# caller's responsibility -- in normal use the counter starts at zero per
# Python process, which is the right granularity for daily/weekend jobs.
_av_call_count = 0


def get_av_call_count() -> int:
    return _av_call_count


def reset_av_call_count() -> None:
    global _av_call_count
    _av_call_count = 0


async def fetch_av_json(
    url: str,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    max_retries: int = 3,
) -> dict:
    """Fetch JSON from Alpha Vantage with rate limiting and retry.

    Detects AV throttle responses (keys "Note" or "Information") and retries
    after 60 s, up to *max_retries* times.
    """
    global _av_call_count
    for attempt in range(1, max_retries + 1):
        await rate_limiter.wait()
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=120)) as resp:
            _av_call_count += 1
            resp.raise_for_status()
            data = await resp.json(content_type=None)

        # AV signals rate-limit / error via top-level "Note" or "Information"
        throttle_msg = data.get("Note") or data.get("Information")
        if throttle_msg:
            if attempt < max_retries:
                logger.warning(
                    f"AV throttle (attempt {attempt}/{max_retries}): "
                    f"{throttle_msg[:120]} -- retrying in 60s"
                )
                await asyncio.sleep(60)
                continue
            raise AVResponseError(
                f"AV throttle after {max_retries} retries: {throttle_msg[:200]}"
            )

        return data

    raise AVResponseError("fetch_av_json: exhausted retries without a valid response")


# ---------------------------------------------------------------------------
# Month range generator
# ---------------------------------------------------------------------------

_EARLIEST = date(2000, 1, 1)


def generate_months(ipo_date: str | None, delisting_date: str | None) -> list[str]:
    """Generate YYYY-MM strings from max(ipo_date, 2000-01) to min(delisting_date, today).

    *ipo_date* and *delisting_date* are expected as ``"YYYY-MM-DD"`` strings or
    ``None``.  ``None`` defaults to 2000-01 and today respectively.
    """
    if ipo_date:
        try:
            start = datetime.strptime(ipo_date, "%Y-%m-%d").date()
        except ValueError:
            start = _EARLIEST
    else:
        start = _EARLIEST

    if delisting_date:
        try:
            end = datetime.strptime(delisting_date, "%Y-%m-%d").date()
        except ValueError:
            end = date.today()
    else:
        end = date.today()

    start = max(start.replace(day=1), _EARLIEST)
    end = end.replace(day=1)

    months: list[str] = []
    cursor = start
    while cursor <= end:
        months.append(cursor.strftime("%Y-%m"))
        # Advance to next month
        if cursor.month == 12:
            cursor = cursor.replace(year=cursor.year + 1, month=1)
        else:
            cursor = cursor.replace(month=cursor.month + 1)

    return months


# ---------------------------------------------------------------------------
# Catalog reader
# ---------------------------------------------------------------------------

def read_catalog_symbols(catalog_dir: Path, asset_type: str) -> pl.DataFrame:
    """Read symbols from catalog parquet. Does not exclude any status.

    Args:
        catalog_dir: Path to the catalog directory.
        asset_type: Asset type name (e.g. ``"stocks"``, ``"commodities"``).

    Returns:
        Full catalog DataFrame (columns vary by asset type).
    """
    path = catalog_dir / f"{asset_type}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Catalog file not found: {path}")

    df = pl.read_parquet(path)
    return df


# ---------------------------------------------------------------------------
# Issue tracker
# ---------------------------------------------------------------------------

class IssueTracker:
    """Accumulates per-symbol ingestion issues and saves them as parquet."""

    def __init__(self):
        self._rows: list[dict] = []

    def record(
        self,
        symbol: str,
        asset_type: str,
        endpoint: str,
        issue_type: str,
        detail: str,
    ) -> None:
        """Record a single issue.

        *issue_type* should be one of: ``structure_error``, ``empty_content``,
        ``cast_failure``, ``timezone_mismatch``, ``av_throttle``.
        """
        self._rows.append(
            {
                "symbol": symbol,
                "asset_type": asset_type,
                "endpoint": endpoint,
                "issue_type": issue_type,
                "detail": detail,
                "timestamp": datetime.now(),
            }
        )
        logger.info(f"Issue [{issue_type}] {symbol} ({endpoint}): {detail}")

    @property
    def count(self) -> int:
        return len(self._rows)

    def save(self, path: Path) -> None:
        """Save accumulated issues to parquet. Merges with existing file."""
        if not self._rows:
            logger.info("No ingestion issues to save")
            return

        schema = {
            "symbol": pl.Utf8,
            "asset_type": pl.Utf8,
            "endpoint": pl.Utf8,
            "issue_type": pl.Utf8,
            "detail": pl.Utf8,
            "timestamp": pl.Datetime,
        }
        new_df = pl.DataFrame(self._rows, schema=schema)

        if path.exists():
            existing = pl.read_parquet(path)
            new_df = pl.concat([existing, new_df], how="vertical_relaxed")

        new_df.write_parquet(path, compression="zstd")
        logger.info(f"Saved {len(self._rows)} new issues to {path} ({new_df.height} total)")


# ---------------------------------------------------------------------------
# Per-symbol parquet filename
# ---------------------------------------------------------------------------

# Asset-type filename prefix. Windows reserves names like CON, PRN, AUX, NUL,
# COM0-9, LPT0-9 regardless of extension, so a ticker that collides with one
# (e.g. PRN) would be unwritable as ``PRN.parquet``. Always prepending an
# asset-type prefix yields ``etfs_PRN.parquet`` / ``stocks_CON.parquet`` and
# sidesteps the issue uniformly across asset types.
ASSET_TYPE_FILE_PREFIX: dict[str, str] = {
    "stocks": "stocks_",
    "etfs": "etfs_",
    "forex": "forex_",
    "indices": "indices_",
    "cryptocurrencies": "cryptocurrencies_",
    "commodities": "commodities_",
    "economic": "economic_",
}


def symbol_parquet_name(asset_type: str, symbol: str, suffix: str = "") -> str:
    """Build a per-symbol parquet filename, prefixed by asset type.

    *suffix* is appended before the extension (e.g. ``"_annual"`` for
    fundamental endpoints).
    """
    prefix = ASSET_TYPE_FILE_PREFIX[asset_type]
    return f"{prefix}{symbol}{suffix}.parquet"


# ---------------------------------------------------------------------------
# FirstRate Data CSV helpers
# ---------------------------------------------------------------------------


def frd_csv_path(frd_dir: Path | None, symbol: str, suffix: str) -> Path | None:
    """Return ``frd_dir/{symbol}_{suffix}.csv`` if it exists, else ``None``.

    Lightweight per-symbol check -- no upfront directory scan.
    *suffix* is e.g. ``"1min"``, ``"1day_unadjusted"``.
    """
    if frd_dir is None:
        return None
    path = frd_dir / f"{symbol}_{suffix}.csv"
    if path.exists():
        return path
    return None


# ---------------------------------------------------------------------------
# Response validation helpers
# ---------------------------------------------------------------------------

_STRING_COLUMNS = {"reportedCurrency", "reportTime"}
_DATE_COLUMNS = {"fiscalDateEnding", "reportedDate"}

# Values in AV responses that represent missing data -- treated as null
_NULL_SENTINELS = {None, "None", "", "."}

_TZ_KEY_RE = re.compile(r"^\d+\.\s*Time Zone$")


def validate_meta_data(data: dict, symbol: str, asset_type: str, endpoint: str,
                       issue_tracker: IssueTracker,
                       expected_tz: str = "US/Eastern") -> bool:
    """Check that 'Meta Data' exists and timezone matches *expected_tz*.

    Returns True if the response structure is usable, False otherwise.
    """
    meta = data.get("Meta Data")
    if meta is None:
        issue_tracker.record(symbol, asset_type, endpoint,
                             "structure_error", "missing 'Meta Data' key")
        return False

    for key, value in meta.items():
        if _TZ_KEY_RE.match(key):
            if value != expected_tz:
                issue_tracker.record(symbol, asset_type, endpoint,
                                     "timezone_mismatch", f"tz={value}")
            break

    return True


# ---------------------------------------------------------------------------
# Fundamental endpoint helper (income_statement, balance_sheet, cash_flow, earnings)
# ---------------------------------------------------------------------------

def _build_fundamental_df(
    records: list[dict],
    symbol: str,
    asset_type: str,
    endpoint: str,
    report_label: str,
    issue_tracker: IssueTracker,
) -> pl.DataFrame | None:
    """Convert a list of report dicts into a typed polars DataFrame.

    Casting rules:
    - fiscalDateEnding -> pl.Date (required)
    - reportedDate     -> pl.Date (cast_failure if it fails)
    - reportedCurrency, reportTime -> pl.String
    - everything else  -> pl.Float32 (keep as pl.String on failure, record cast_failure)

    Returns None if *records* is empty.
    """
    if not records:
        return None

    # Replace null sentinels with actual None
    cleaned: list[dict] = []
    for rec in records:
        cleaned.append({k: (None if v in _NULL_SENTINELS else v) for k, v in rec.items()})

    # Build all-String DataFrame
    df = pl.DataFrame(cleaned, infer_schema_length=0)

    # Cast fiscalDateEnding (required)
    if "fiscalDateEnding" in df.columns:
        try:
            df = df.with_columns(
                pl.col("fiscalDateEnding").str.to_date("%Y-%m-%d")
            )
        except Exception as e:
            issue_tracker.record(
                symbol, asset_type, endpoint,
                "cast_failure",
                f"{report_label}: fiscalDateEnding to Date failed: {e}",
            )
            return None
    else:
        issue_tracker.record(
            symbol, asset_type, endpoint,
            "structure_error",
            f"{report_label}: missing fiscalDateEnding column",
        )
        return None

    # Cast remaining columns
    for col_name in df.columns:
        if col_name in _DATE_COLUMNS:
            if col_name == "fiscalDateEnding":
                continue  # already cast
            # reportedDate -> attempt pl.Date
            try:
                df = df.with_columns(
                    pl.col(col_name).str.to_date("%Y-%m-%d")
                )
            except Exception as e:
                issue_tracker.record(
                    symbol, asset_type, endpoint,
                    "cast_failure",
                    f"{report_label}: {col_name} to Date failed: {e}",
                )
        elif col_name in _STRING_COLUMNS:
            continue  # keep as String
        else:
            # Attempt Float32 -- must succeed; do not fall back to String
            try:
                df = df.with_columns(
                    pl.col(col_name).cast(pl.Float32)
                )
            except Exception as e:
                # Force cast: non-castable values become null
                df = df.with_columns(
                    pl.col(col_name).cast(pl.Float32, strict=False)
                )
                issue_tracker.record(
                    symbol, asset_type, endpoint,
                    "cast_failure",
                    f"{report_label}: {col_name} to Float32 had non-castable "
                    f"values (forced to null): {e}",
                )

    return df.sort("fiscalDateEnding")


async def fetch_fundamental_endpoint(
    catalog_dir: Path,
    historical_dir: Path,
    api_key: str,
    session: aiohttp.ClientSession,
    rate_limiter: RateLimiter,
    issue_tracker: IssueTracker,
    asset_type: str,
    av_function: str,
    endpoint: str,
    annual_key: str,
    quarterly_key: str,
) -> None:
    """Generic fetcher for fundamental endpoints that return annual + quarterly data."""
    catalog = read_catalog_symbols(catalog_dir, asset_type)
    output_dir = historical_dir / asset_type / endpoint
    output_dir.mkdir(parents=True, exist_ok=True)

    total = catalog.height
    logger.info(f"{endpoint} ({asset_type}): {total} symbols to process")

    for idx, row in enumerate(catalog.iter_rows(named=True), 1):
        symbol = row["symbol"]
        annual_path = output_dir / symbol_parquet_name(asset_type, symbol, "_annual")
        quarterly_path = output_dir / symbol_parquet_name(asset_type, symbol, "_quarterly")

        if annual_path.exists() and quarterly_path.exists():
            continue

        url = (
            f"{AV_BASE}/query?function={av_function}"
            f"&symbol={symbol}&apikey={api_key}"
        )

        try:
            data = await fetch_av_json(url, session, rate_limiter)
        except AVResponseError as e:
            issue_tracker.record(symbol, asset_type, endpoint, "av_throttle", str(e))
            continue
        except Exception as e:
            issue_tracker.record(
                symbol, asset_type, endpoint,
                "structure_error", f"fetch failed: {e}",
            )
            continue

        # Validate top-level keys
        expected_keys = {"symbol", annual_key, quarterly_key}
        missing = expected_keys - data.keys()
        if missing:
            issue_tracker.record(
                symbol, asset_type, endpoint,
                "structure_error", f"missing top-level keys: {missing}",
            )
            del data
            continue

        annual_records = data.get(annual_key, [])
        quarterly_records = data.get(quarterly_key, [])
        del data

        # Check empty content
        if not annual_records:
            issue_tracker.record(
                symbol, asset_type, endpoint,
                "empty_content", f"empty {annual_key}",
            )
        if not quarterly_records:
            issue_tracker.record(
                symbol, asset_type, endpoint,
                "empty_content", f"empty {quarterly_key}",
            )

        # Build and save annual
        annual_df = _build_fundamental_df(
            annual_records, symbol, asset_type, endpoint, "annual", issue_tracker,
        )
        if annual_df is not None:
            annual_df.write_parquet(annual_path, compression="zstd")
            logger.info(f"  {endpoint} ({asset_type}): {symbol} saved {annual_df.height} annual rows")
            del annual_df

        # Build and save quarterly
        quarterly_df = _build_fundamental_df(
            quarterly_records, symbol, asset_type, endpoint, "quarterly", issue_tracker,
        )
        if quarterly_df is not None:
            quarterly_df.write_parquet(quarterly_path, compression="zstd")
            logger.info(f"  {endpoint} ({asset_type}): {symbol} saved {quarterly_df.height} quarterly rows")
            del quarterly_df

        del annual_records, quarterly_records
