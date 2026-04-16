"""Estimate API calls and time for TIME_SERIES_DAILY_ADJUSTED historical fetch.

One API call per symbol (full history returned). Queries LISTING_STATUS to
get the symbol list, then samples real calls to measure round-trip time.

Usage:
    python tests/call_speedtests/estimate_prices_daily_calls.py
    python tests/call_speedtests/estimate_prices_daily_calls.py --max-calls 10
"""

import argparse
import io
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from maintainance_scripts.get_api_key import get_alpha_vantage_key

import polars as pl
import requests

AV_BASE = "https://www.alphavantage.co"
MIN_INTERVAL = 60.0 / 74.9


def _fetch_listings(api_key: str) -> tuple[list[str], list[str]]:
    """Query LISTING_STATUS, return (stock_symbols, etf_symbols)."""
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

    stocks = combined.filter(pl.col("assetType") == "Stock")["symbol"].to_list()
    etfs = combined.filter(pl.col("assetType") == "ETF")["symbol"].to_list()
    return stocks, etfs


def main(
    max_calls: int | None = None,
    api_tier: str = "premium",
) -> None:
    api_key = get_alpha_vantage_key(api_tier)

    stock_symbols, etf_symbols = _fetch_listings(api_key)
    total_symbols = len(stock_symbols) + len(etf_symbols)

    print(f"Stocks: {len(stock_symbols)} symbols")
    print(f"ETFs:   {len(etf_symbols)} symbols")
    print(f"Total API calls needed: {total_symbols:,} (1 per symbol)")
    print()

    sample_size = max_calls if max_calls is not None else 10
    step = max(1, len(stock_symbols) // sample_size)
    sample = stock_symbols[::step][:sample_size]

    call_count = 0
    total_rows = 0
    last_call = 0.0
    t_start = time.monotonic()

    print(
        f"{'call':>5} | {'symbol':>8} | {'round_trip':>10} | "
        f"{'rows':>6} | {'payload_kb':>10}"
    )
    print("-" * 55)

    for symbol in sample:
        if max_calls is not None and call_count >= max_calls:
            break

        url = (
            f"{AV_BASE}/query?function=TIME_SERIES_DAILY_ADJUSTED"
            f"&symbol={symbol}&outputsize=full&apikey={api_key}"
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

        ts = data.get("Time Series (Daily)", {})
        rows = len(ts)
        total_rows += rows
        del data

        print(
            f"{call_count:>5} | {symbol:>8} | {round_trip:>9.2f}s | "
            f"{rows:>6} | {raw_size / 1024:>9.1f}"
        )

    elapsed_s = time.monotonic() - t_start

    if call_count < 1:
        print("\nNo successful calls to extrapolate from.")
        return

    avg_sec = elapsed_s / call_count
    avg_rows = total_rows / call_count
    est_total_time = total_symbols * avg_sec

    print()
    print("=" * 55)
    print("Observed:")
    print(f"  API calls:          {call_count}")
    print(f"  Total rows:         {total_rows:,}")
    print(f"  Avg round-trip:     {avg_sec:.2f}s")
    print(f"  Avg rows/call:      {avg_rows:.0f}")
    print()
    print("Extrapolation (full catalog):")
    print(f"  Total API calls:    {total_symbols:,}")
    print(f"  Est. total time:    ~{est_total_time / 3600:.1f} hours ({est_total_time / 60:.0f} min)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Estimate TIME_SERIES_DAILY_ADJUSTED API call count and time"
    )
    parser.add_argument(
        "--max-calls", type=int, default=None,
        help="Stop after this many successful calls (default: 10)",
    )
    parser.add_argument(
        "--api-tier", default="premium", choices=("standard", "premium"),
        help="API key tier (default: premium)",
    )
    args = parser.parse_args()
    main(args.max_calls, args.api_tier)
