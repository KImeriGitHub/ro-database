"""Estimate API calls and time for ETF_PROFILE historical fetch.

One API call per ETF symbol. Queries LISTING_STATUS to get the ETF list,
then samples real calls to measure round-trip time.

Usage:
    python tests/call_speedtests/estimate_etf_profile_calls.py
    python tests/call_speedtests/estimate_etf_profile_calls.py --max-calls 10
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


def _fetch_etf_symbols(api_key: str) -> list[str]:
    """Query LISTING_STATUS, return ETF symbols."""
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

    return combined.filter(pl.col("assetType") == "ETF")["symbol"].to_list()


def main(
    max_calls: int | None = None,
    api_tier: str = "premium",
) -> None:
    api_key = get_alpha_vantage_key(api_tier)

    symbols = _fetch_etf_symbols(api_key)
    total_symbols = len(symbols)

    print(f"ETFs: {total_symbols} symbols")
    print(f"Total API calls needed: {total_symbols:,} (1 per symbol)")
    print()

    sample_size = max_calls if max_calls is not None else 10
    step = max(1, len(symbols) // sample_size)
    sample = symbols[::step][:sample_size]

    call_count = 0
    last_call = 0.0
    t_start = time.monotonic()

    print(
        f"{'call':>5} | {'symbol':>8} | {'round_trip':>10} | "
        f"{'sectors':>7} | {'holdings':>8} | {'payload_kb':>10}"
    )
    print("-" * 65)

    for symbol in sample:
        if max_calls is not None and call_count >= max_calls:
            break

        url = (
            f"{AV_BASE}/query?function=ETF_PROFILE"
            f"&symbol={symbol}&apikey={api_key}"
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

        sectors = data.get("sectors", [])
        holdings = data.get("holdings", [])
        n_sectors = len(sectors) if isinstance(sectors, list) else 0
        n_holdings = len(holdings) if isinstance(holdings, list) else 0
        del data

        print(
            f"{call_count:>5} | {symbol:>8} | {round_trip:>9.2f}s | "
            f"{n_sectors:>7} | {n_holdings:>8} | {raw_size / 1024:>9.1f}"
        )

    elapsed_s = time.monotonic() - t_start

    if call_count < 1:
        print("\nNo successful calls to extrapolate from.")
        return

    avg_sec = elapsed_s / call_count
    est_total_time = total_symbols * avg_sec

    print()
    print("=" * 65)
    print("Observed:")
    print(f"  API calls:          {call_count}")
    print(f"  Avg round-trip:     {avg_sec:.2f}s")
    print()
    print("Extrapolation (full catalog):")
    print(f"  Total API calls:    {total_symbols:,}")
    print(f"  Est. total time:    ~{est_total_time / 3600:.1f} hours ({est_total_time / 60:.0f} min)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Estimate ETF_PROFILE API call count and time"
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
