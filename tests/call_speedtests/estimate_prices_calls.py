"""Estimate API calls and time for TIME_SERIES_INTRADAY historical fetch.

Each symbol requires one API call per month from max(ipoDate, 2000-01) to
min(delistingDate, today). Queries LISTING_STATUS to get the full symbol list,
then samples real API calls to measure round-trip time.

Usage:
    python tests/call_speedtests/estimate_prices_calls.py
    python tests/call_speedtests/estimate_prices_calls.py --max-calls 20
"""

import argparse
import io
import sys
import time
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from maintainance_scripts.get_api_key import get_alpha_vantage_key

import polars as pl
import requests

AV_BASE = "https://www.alphavantage.co"
MIN_INTERVAL = 60.0 / 74.0
EARLIEST = date(2000, 1, 1)


def _fetch_listings(api_key: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Query LISTING_STATUS for active + delisted, return (stocks, etfs)."""
    print("Querying LISTING_STATUS (active + delisted)...")
    active_csv = requests.get(
        f"{AV_BASE}/query?function=LISTING_STATUS&state=active&apikey={api_key}",
        timeout=60,
    ).text
    delisted_csv = requests.get(
        f"{AV_BASE}/query?function=LISTING_STATUS&state=delisted&apikey={api_key}",
        timeout=60,
    ).text

    active = pl.read_csv(io.StringIO(active_csv), null_values=["null"], infer_schema_length=0)
    delisted = pl.read_csv(io.StringIO(delisted_csv), null_values=["null"], infer_schema_length=0)
    combined = pl.concat([active, delisted], how="vertical_relaxed")

    stocks = combined.filter(pl.col("assetType") == "Stock").select(
        "symbol", "ipoDate", "delistingDate",
    )
    etfs = combined.filter(pl.col("assetType") == "ETF").select(
        "symbol", "ipoDate", "delistingDate",
    )
    return stocks, etfs


def _count_months(ipo_date: str | None, delisting_date: str | None) -> int:
    """Count months from max(ipo_date, 2000-01) to min(delisting_date, today)."""
    if ipo_date:
        try:
            start = datetime.strptime(ipo_date, "%Y-%m-%d").date()
        except ValueError:
            start = EARLIEST
    else:
        start = EARLIEST

    if delisting_date:
        try:
            end = datetime.strptime(delisting_date, "%Y-%m-%d").date()
        except ValueError:
            end = date.today()
    else:
        end = date.today()

    start = max(start.replace(day=1), EARLIEST)
    end = end.replace(day=1)

    count = 0
    cursor = start
    while cursor <= end:
        count += 1
        if cursor.month == 12:
            cursor = cursor.replace(year=cursor.year + 1, month=1)
        else:
            cursor = cursor.replace(month=cursor.month + 1)
    return count


def main(
    max_calls: int | None = None,
    api_tier: str = "premium",
) -> None:
    api_key = get_alpha_vantage_key(api_tier)

    stocks, etfs = _fetch_listings(api_key)

    stocks_months = 0
    stocks_symbols = []
    for row in stocks.iter_rows(named=True):
        m = _count_months(row.get("ipoDate"), row.get("delistingDate"))
        stocks_months += m
        stocks_symbols.append((row["symbol"], row.get("ipoDate"), row.get("delistingDate"), m))

    etfs_months = 0
    for row in etfs.iter_rows(named=True):
        etfs_months += _count_months(row.get("ipoDate"), row.get("delistingDate"))

    total_months = stocks_months + etfs_months

    print(f"Stocks: {stocks.height} symbols, {stocks_months:,} total months")
    print(f"ETFs:   {etfs.height} symbols, {etfs_months:,} total months")
    print(f"Total API calls needed: {total_months:,}")
    print()

    if max_calls is not None and max_calls == 0:
        print("--max-calls 0: skipping API calls, showing catalog stats only.")
        return

    # Sample API calls from different symbols
    sample_size = max_calls if max_calls is not None else 20
    step = max(1, len(stocks_symbols) // sample_size)
    sample_entries = stocks_symbols[::step][:sample_size]

    call_count = 0
    total_rows = 0
    last_call = 0.0
    t_start = time.monotonic()

    print(
        f"{'call':>5} | {'symbol':>8} | {'month':>7} | "
        f"{'round_trip':>10} | {'rows':>6} | {'payload_kb':>10}"
    )
    print("-" * 65)

    for symbol, ipo, delist, months in sample_entries:
        if max_calls is not None and call_count >= max_calls:
            break

        # Pick a recent month for this symbol
        if delist:
            try:
                end = datetime.strptime(delist, "%Y-%m-%d").date()
            except ValueError:
                end = date.today()
        else:
            end = date.today()
        month_str = end.replace(day=1).strftime("%Y-%m")

        url = (
            f"{AV_BASE}/query?function=TIME_SERIES_INTRADAY"
            f"&symbol={symbol}&interval=1min&month={month_str}"
            f"&adjusted=false&outputsize=full&apikey={api_key}"
        )

        elapsed = time.monotonic() - last_call
        if elapsed < MIN_INTERVAL:
            time.sleep(MIN_INTERVAL - elapsed)

        t0 = time.monotonic()
        last_call = t0
        try:
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            raw_size = len(resp.content)
            data = resp.json()
        except Exception as e:
            print(f"  {symbol}: request error: {e}")
            time.sleep(10)
            continue

        round_trip = time.monotonic() - t0

        throttle_msg = data.get("Note") or data.get("Information")
        if throttle_msg:
            print(f"  THROTTLED: {throttle_msg[:120]} -- waiting 60s")
            time.sleep(60)
            continue

        call_count += 1

        ts = data.get("Time Series (1min)", {})
        rows = len(ts)
        total_rows += rows
        del data

        print(
            f"{call_count:>5} | {symbol:>8} | {month_str:>7} | "
            f"{round_trip:>9.2f}s | {rows:>6} | {raw_size / 1024:>9.1f}"
        )

    elapsed_s = time.monotonic() - t_start

    if call_count < 1:
        print("\nNo successful calls to extrapolate from.")
        return

    avg_sec = elapsed_s / call_count
    avg_rows = total_rows / call_count

    est_total_time = total_months * avg_sec

    print()
    print("=" * 65)
    print("Observed:")
    print(f"  API calls:          {call_count}")
    print(f"  Total rows:         {total_rows:,}")
    print(f"  Avg round-trip:     {avg_sec:.2f}s")
    print(f"  Avg rows/call:      {avg_rows:.0f}")
    print()
    print("Extrapolation (full catalog):")
    print(f"  Total API calls:    {total_months:,}")
    print(f"  Est. total time:    ~{est_total_time / 3600:.1f} hours ({est_total_time / 60:.0f} min)")
    print()
    print(
        "Note: intraday prices have large JSON payloads (~8k rows/month)."
        " Round-trip time (~3.5s) dominates the rate limiter (0.8s)."
        " For historical data, FirstRate Data is used instead of AV."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Estimate TIME_SERIES_INTRADAY API call count and time"
    )
    parser.add_argument(
        "--max-calls", type=int, default=None,
        help="Stop after this many successful calls (default: 20)",
    )
    parser.add_argument(
        "--api-tier", default="premium", choices=("standard", "premium"),
        help="API key tier (default: premium)",
    )
    args = parser.parse_args()
    main(args.max_calls, args.api_tier)
