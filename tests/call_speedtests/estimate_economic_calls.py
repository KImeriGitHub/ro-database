"""Estimate API calls and time for economic indicator endpoints.

Only 15 indicators (hardcoded), each using a different AV function (and
sometimes extra params like interval/maturity). Samples real calls to
measure round-trip time.

Usage:
    python tests/call_speedtests/estimate_economic_calls.py
    python tests/call_speedtests/estimate_economic_calls.py --max-calls 5
"""

import argparse
import sys
import time
from pathlib import Path
from urllib.parse import urlencode

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from maintainance_scripts.get_api_key import get_alpha_vantage_key

import requests

AV_BASE = "https://www.alphavantage.co"
MIN_INTERVAL = 60.0 / 74.9

INDICATOR_CONFIG: dict[str, dict] = {
    "REAL_GDP":            {"function": "REAL_GDP",            "params": {"interval": "quarterly"}},
    "REAL_GDP_PER_CAPITA": {"function": "REAL_GDP_PER_CAPITA", "params": {}},
    "TREASURY_YIELD_30Y":  {"function": "TREASURY_YIELD",     "params": {"interval": "daily", "maturity": "30year"}},
    "TREASURY_YIELD_10Y":  {"function": "TREASURY_YIELD",     "params": {"interval": "daily", "maturity": "10year"}},
    "TREASURY_YIELD_7Y":   {"function": "TREASURY_YIELD",     "params": {"interval": "daily", "maturity": "7year"}},
    "TREASURY_YIELD_5Y":   {"function": "TREASURY_YIELD",     "params": {"interval": "daily", "maturity": "5year"}},
    "TREASURY_YIELD_2Y":   {"function": "TREASURY_YIELD",     "params": {"interval": "daily", "maturity": "2year"}},
    "TREASURY_YIELD_3M":   {"function": "TREASURY_YIELD",     "params": {"interval": "daily", "maturity": "3month"}},
    "FEDERAL_FUNDS_RATE":  {"function": "FEDERAL_FUNDS_RATE",  "params": {"interval": "daily"}},
    "CPI":                 {"function": "CPI",                 "params": {"interval": "monthly"}},
    "INFLATION":           {"function": "INFLATION",           "params": {}},
    "RETAIL_SALES":        {"function": "RETAIL_SALES",        "params": {}},
    "DURABLES":            {"function": "DURABLES",            "params": {}},
    "UNEMPLOYMENT":        {"function": "UNEMPLOYMENT",        "params": {}},
    "NONFARM_PAYROLL":     {"function": "NONFARM_PAYROLL",     "params": {}},
}


def main(
    max_calls: int | None = None,
    api_tier: str = "premium",
) -> None:
    api_key = get_alpha_vantage_key(api_tier)

    symbols = list(INDICATOR_CONFIG.keys())
    total_symbols = len(symbols)

    print(f"Economic indicators: {total_symbols} (hardcoded)")
    print(f"Total API calls needed: {total_symbols}")
    print()

    sample_size = max_calls if max_calls is not None else total_symbols
    sample = symbols[:sample_size]

    call_count = 0
    total_rows = 0
    last_call = 0.0
    t_start = time.monotonic()

    print(
        f"{'call':>5} | {'symbol':>22} | {'av_function':>20} | "
        f"{'round_trip':>10} | {'rows':>6} | {'payload_kb':>10}"
    )
    print("-" * 90)

    for symbol in sample:
        if max_calls is not None and call_count >= max_calls:
            break

        config = INDICATOR_CONFIG[symbol]

        query = {"function": config["function"], "apikey": api_key}
        query.update(config["params"])
        url = f"{AV_BASE}/query?{urlencode(query)}"

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

        records = data.get("data", [])
        rows = len(records) if isinstance(records, list) else 0
        total_rows += rows
        del data

        print(
            f"{call_count:>5} | {symbol:>22} | {config['function']:>20} | "
            f"{round_trip:>9.2f}s | {rows:>6} | {raw_size / 1024:>9.1f}"
        )

    elapsed_s = time.monotonic() - t_start

    if call_count < 1:
        print("\nNo successful calls to extrapolate from.")
        return

    avg_sec = elapsed_s / call_count
    est_total_time = total_symbols * avg_sec

    print()
    print("=" * 90)
    print("Observed:")
    print(f"  API calls:          {call_count}")
    print(f"  Total rows:         {total_rows:,}")
    print(f"  Avg round-trip:     {avg_sec:.2f}s")
    print()
    print("Extrapolation (full catalog):")
    print(f"  Total API calls:    {total_symbols}")
    print(f"  Est. total time:    ~{est_total_time / 60:.1f} min ({est_total_time:.0f}s)")
    print()
    print("Note: only ~15 total calls. This endpoint is negligible in the overall budget.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Estimate economic indicator API call count and time"
    )
    parser.add_argument(
        "--max-calls", type=int, default=None,
        help="Stop after this many successful calls (default: all 15)",
    )
    parser.add_argument(
        "--api-tier", default="premium", choices=("standard", "premium"),
        help="API key tier (default: premium)",
    )
    args = parser.parse_args()
    main(args.max_calls, args.api_tier)
