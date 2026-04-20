"""Estimate API calls and time for fundamental endpoints.

Covers INCOME_STATEMENT, BALANCE_SHEET, CASH_FLOW, EARNINGS, and
EARNINGS_ESTIMATES (5 endpoints, 1 call each per symbol). Queries
LISTING_STATUS for the symbol list, then cycles through all five
endpoints during sampling to capture any per-endpoint differences.

Usage:
    python tests/call_speedtests/estimate_fundamentals_calls.py
    python tests/call_speedtests/estimate_fundamentals_calls.py --max-calls 15
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
MIN_INTERVAL = 60.0 / 74.0

ENDPOINTS = [
    ("INCOME_STATEMENT", "annualReports", "quarterlyReports"),
    ("BALANCE_SHEET", "annualReports", "quarterlyReports"),
    ("CASH_FLOW", "annualReports", "quarterlyReports"),
    ("EARNINGS", "annualEarnings", "quarterlyEarnings"),
    ("EARNINGS_ESTIMATES", "symbol", "estimates"),
]


def _fetch_stock_symbols(api_key: str) -> list[str]:
    """Query LISTING_STATUS, return stock symbols."""
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

    return combined.filter(pl.col("assetType") == "Stock")["symbol"].to_list()


def main(
    max_calls: int | None = None,
    api_tier: str = "premium",
) -> None:
    api_key = get_alpha_vantage_key(api_tier)

    symbols = _fetch_stock_symbols(api_key)
    total_symbols = len(symbols)
    total_calls = total_symbols * len(ENDPOINTS)

    print(f"Stocks: {total_symbols} symbols")
    print(f"Endpoints: {len(ENDPOINTS)} (income_statement, balance_sheet, cash_flow, earnings, earnings_estimates)")
    print(f"Total API calls needed: {total_calls:,} ({total_symbols} x {len(ENDPOINTS)})")
    print()

    sample_size = max_calls if max_calls is not None else 15

    call_count = 0
    last_call = 0.0
    t_start = time.monotonic()

    # Per-endpoint stats
    ep_stats: dict[str, list[float]] = {ep[0]: [] for ep in ENDPOINTS}

    print(
        f"{'call':>5} | {'symbol':>8} | {'endpoint':>20} | "
        f"{'round_trip':>10} | {'annual':>6} | {'quarterly':>9} | {'payload_kb':>10}"
    )
    print("-" * 85)

    sym_idx = 0
    ep_idx = 0

    while call_count < sample_size:
        if sym_idx >= len(symbols):
            break

        symbol = symbols[sym_idx]
        av_function, key_a, key_q = ENDPOINTS[ep_idx]

        url = (
            f"{AV_BASE}/query?function={av_function}"
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
            print(f"  {symbol}/{av_function}: request error: {e}")
            time.sleep(10)
            ep_idx = (ep_idx + 1) % len(ENDPOINTS)
            if ep_idx == 0:
                sym_idx += 1
            continue

        round_trip = time.monotonic() - t0

        throttle_msg = data.get("Note") or data.get("Information")
        if throttle_msg:
            print(f"  THROTTLED: {throttle_msg[:120]} -- waiting 60s")
            time.sleep(60)
            continue

        call_count += 1
        ep_stats[av_function].append(round_trip)

        # Count rows
        if av_function == "EARNINGS_ESTIMATES":
            estimates = data.get("estimates", [])
            annual = sum(1 for r in estimates if r.get("horizon") == "fiscal year")
            quarterly = sum(1 for r in estimates if r.get("horizon") == "fiscal quarter")
        else:
            annual = len(data.get(key_a, []))
            quarterly = len(data.get(key_q, []))
        del data

        print(
            f"{call_count:>5} | {symbol:>8} | {av_function:>20} | "
            f"{round_trip:>9.2f}s | {annual:>6} | {quarterly:>9} | "
            f"{raw_size / 1024:>9.1f}"
        )

        # Cycle: next endpoint, advance symbol after all 5
        ep_idx = (ep_idx + 1) % len(ENDPOINTS)
        if ep_idx == 0:
            sym_idx += 1

    elapsed_s = time.monotonic() - t_start

    if call_count < 1:
        print("\nNo successful calls to extrapolate from.")
        return

    avg_sec = elapsed_s / call_count
    est_total_time = total_calls * avg_sec

    print()
    print("=" * 85)
    print("Observed:")
    print(f"  API calls:          {call_count}")
    print(f"  Avg time/call:      {avg_sec:.2f}s")
    print()
    print("  Per-endpoint averages:")
    for ep_name, times in ep_stats.items():
        if times:
            avg = sum(times) / len(times)
            print(f"    {ep_name:<25} {avg:.2f}s  ({len(times)} calls)")
    print()
    print("Extrapolation (full catalog):")
    print(f"  Total API calls:    {total_calls:,}")
    print(f"  Est. total time:    ~{est_total_time / 3600:.1f} hours ({est_total_time / 60:.0f} min)")
    print()
    print(
        "Note: fundamental endpoints return small JSON payloads (~0.3s round-trip)."
        " The rate limiter (0.8s) is the bottleneck, not the request itself."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Estimate fundamental endpoints API call count and time"
    )
    parser.add_argument(
        "--max-calls", type=int, default=None,
        help="Stop after this many successful calls (default: 15)",
    )
    parser.add_argument(
        "--api-tier", default="premium", choices=("standard", "premium"),
        help="API key tier (default: premium)",
    )
    args = parser.parse_args()
    main(args.max_calls, args.api_tier)
