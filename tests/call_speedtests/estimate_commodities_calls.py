"""Estimate API calls and time for commodity endpoints.

Only 13 symbols total (hardcoded), split across three API patterns:
  - Daily standard (WTI, BRENT, NATURAL_GAS): ?function=SYMBOL&interval=daily
  - Monthly standard (COPPER, ALUMINUM, ...): ?function=SYMBOL&interval=monthly
  - Gold/Silver (XAU, XAG): ?function=GOLD_SILVER_HISTORY&symbol=GOLD|SILVER&interval=daily

Samples real calls to measure round-trip time per group.

Usage:
    python tests/call_speedtests/estimate_commodities_calls.py
    python tests/call_speedtests/estimate_commodities_calls.py --max-calls 6
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from maintainance_scripts.get_api_key import get_alpha_vantage_key

import requests

AV_BASE = "https://www.alphavantage.co"
MIN_INTERVAL = 60.0 / 74.9

COMMODITY_SYMBOLS = [
    "XAU", "XAG", "WTI", "BRENT", "NATURAL_GAS",
    "COPPER", "ALUMINUM", "WHEAT", "CORN",
    "COTTON", "SUGAR", "COFFEE", "ALL_COMMODITIES",
]

DAILY_SYMBOLS = {"WTI", "BRENT", "NATURAL_GAS"}
MONTHLY_SYMBOLS = {
    "COPPER", "ALUMINUM", "WHEAT", "CORN",
    "COTTON", "SUGAR", "COFFEE", "ALL_COMMODITIES",
}
GOLD_SILVER_MAP = {"XAU": "GOLD", "XAG": "SILVER"}


def _build_url(symbol: str, api_key: str) -> tuple[str, str]:
    """Return (url, group_label) for a commodity symbol."""
    if symbol in GOLD_SILVER_MAP:
        av_sym = GOLD_SILVER_MAP[symbol]
        url = (
            f"{AV_BASE}/query?function=GOLD_SILVER_HISTORY"
            f"&symbol={av_sym}&interval=daily&apikey={api_key}"
        )
        return url, "gold_silver"
    elif symbol in DAILY_SYMBOLS:
        url = f"{AV_BASE}/query?function={symbol}&interval=daily&apikey={api_key}"
        return url, "daily"
    elif symbol in MONTHLY_SYMBOLS:
        url = f"{AV_BASE}/query?function={symbol}&interval=monthly&apikey={api_key}"
        return url, "monthly"
    else:
        url = f"{AV_BASE}/query?function={symbol}&interval=monthly&apikey={api_key}"
        return url, "unknown"


def main(
    max_calls: int | None = None,
    api_tier: str = "premium",
) -> None:
    api_key = get_alpha_vantage_key(api_tier)

    symbols = COMMODITY_SYMBOLS
    total_symbols = len(symbols)

    n_daily = sum(1 for s in symbols if s in DAILY_SYMBOLS)
    n_monthly = sum(1 for s in symbols if s in MONTHLY_SYMBOLS)
    n_gs = sum(1 for s in symbols if s in GOLD_SILVER_MAP)

    print(f"Commodities: {total_symbols} symbols (hardcoded)")
    print(f"  Daily standard:  {n_daily} (WTI, BRENT, NATURAL_GAS)")
    print(f"  Monthly standard: {n_monthly} (COPPER, ALUMINUM, ...)")
    print(f"  Gold/Silver:     {n_gs} (XAU, XAG)")
    print(f"Total API calls needed: {total_symbols}")
    print()

    sample_size = max_calls if max_calls is not None else total_symbols
    sample = symbols[:sample_size]

    call_count = 0
    total_rows = 0
    last_call = 0.0
    t_start = time.monotonic()
    group_stats: dict[str, list[float]] = {}

    print(
        f"{'call':>5} | {'symbol':>16} | {'group':>12} | "
        f"{'round_trip':>10} | {'rows':>6} | {'payload_kb':>10}"
    )
    print("-" * 75)

    for symbol in sample:
        if max_calls is not None and call_count >= max_calls:
            break

        url, group = _build_url(symbol, api_key)

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
        group_stats.setdefault(group, []).append(round_trip)

        records = data.get("data", [])
        rows = len(records) if isinstance(records, list) else 0
        total_rows += rows
        del data

        print(
            f"{call_count:>5} | {symbol:>16} | {group:>12} | "
            f"{round_trip:>9.2f}s | {rows:>6} | {raw_size / 1024:>9.1f}"
        )

    elapsed_s = time.monotonic() - t_start

    if call_count < 1:
        print("\nNo successful calls to extrapolate from.")
        return

    avg_sec = elapsed_s / call_count
    est_total_time = total_symbols * avg_sec

    print()
    print("=" * 75)
    print("Observed:")
    print(f"  API calls:          {call_count}")
    print(f"  Total rows:         {total_rows:,}")
    print(f"  Avg round-trip:     {avg_sec:.2f}s")
    print()
    print("  Per-group averages:")
    for grp, times in sorted(group_stats.items()):
        avg = sum(times) / len(times)
        print(f"    {grp:<16} {avg:.2f}s  ({len(times)} calls)")
    print()
    print("Extrapolation (full catalog):")
    print(f"  Total API calls:    {total_symbols}")
    print(f"  Est. total time:    ~{est_total_time / 60:.1f} min ({est_total_time:.0f}s)")
    print()
    print("Note: only 13 total calls. This endpoint is negligible in the overall budget.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Estimate commodity endpoints API call count and time"
    )
    parser.add_argument(
        "--max-calls", type=int, default=None,
        help="Stop after this many successful calls (default: all 13)",
    )
    parser.add_argument(
        "--api-tier", default="premium", choices=("standard", "premium"),
        help="API key tier (default: premium)",
    )
    args = parser.parse_args()
    main(args.max_calls, args.api_tier)
