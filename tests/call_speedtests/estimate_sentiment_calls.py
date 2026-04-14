"""Estimate how many API calls NEWS_SENTIMENT backward pagination takes.

Runs real API calls (no ticker filter, limit=1000) from current UTC time
backward toward 2010-01-01 and logs how far each call reaches.
After the run, prints estimated total calls, time, and RAM for the full
fetch down to 2010-01-01.

Usage:
    python tests/call_speedtests/estimate_sentiment_calls.py                  # run until empty
    python tests/call_speedtests/estimate_sentiment_calls.py --max-calls 50   # cap at 50 calls
"""

import argparse
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from maintainance_scripts.get_api_key import get_alpha_vantage_key

import requests

AV_BASE = "https://www.alphavantage.co"
TIME_FROM = "20100101T0000"
TIME_FROM_DT = datetime(2010, 1, 1, 0, 0, tzinfo=timezone.utc)
MIN_INTERVAL = 60.0 / 74.9  # ~0.8s between calls

# Rough per-row memory estimate for the final polars DataFrame.
# 19 Float32 cols (4B each) = 76B, 1 Datetime (8B), ~9 String cols averaging
# ~100B each (title, url, summary dominate) = ~900B.  Total ~984B.
# Polars stores strings in a separate buffer with offsets, so actual usage is
# somewhat lower, but 1 KB/row is a safe upper-bound estimate.
BYTES_PER_ROW = 1024


def main(max_calls: int | None = None) -> None:
    api_key = get_alpha_vantage_key("premium")
    start_dt = datetime.now(timezone.utc)
    time_to = start_dt.strftime("%Y%m%dT%H%M")
    call_count = 0
    total_articles = 0
    total_ticker_rows = 0
    last_call = 0.0
    t_start = time.monotonic()

    # Total time span to cover (minutes)
    total_span_min = (start_dt - TIME_FROM_DT).total_seconds() / 60

    print(f"Starting backward pagination from {time_to}")
    print(f"time_from is always {TIME_FROM}")
    print(f"Total time span to cover: {total_span_min / 60 / 24:.0f} days")
    print()
    print(
        f"{'call':>5} | {'time_to':>15} | {'items':>6} | "
        f"{'oldest':>20} | {'ticker_rows':>12} | {'cumul_articles':>15}"
    )
    print("-" * 95)

    while time_to > TIME_FROM:
        if max_calls is not None and call_count >= max_calls:
            print(f"\nStopped after {max_calls} calls")
            break

        url = (
            f"{AV_BASE}/query?function=NEWS_SENTIMENT"
            f"&time_from={TIME_FROM}&time_to={time_to}"
            f"&limit=1000&apikey={api_key}"
        )

        # Rate limit: only sleep for remaining interval
        elapsed = time.monotonic() - last_call
        if elapsed < MIN_INTERVAL:
            time.sleep(MIN_INTERVAL - elapsed)
        last_call = time.monotonic()
        try:
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"Request error: {e}")
            time.sleep(10)
            continue

        # Throttle detection
        throttle_msg = data.get("Note") or data.get("Information")
        if throttle_msg:
            print(f"THROTTLED: {throttle_msg[:120]} -- waiting 60s")
            time.sleep(60)
            continue

        call_count += 1

        feed = data.get("feed", [])
        items = int(data.get("items", "0"))
        del data

        if not feed:
            print(f"\nEmpty feed at time_to={time_to}. Done.")
            break

        # Parse times and count ticker rows
        times = []
        batch_ticker_rows = 0
        for article in feed:
            tp = article.get("time_published", "")
            try:
                times.append(datetime.strptime(tp, "%Y%m%dT%H%M%S"))
            except (ValueError, TypeError):
                pass
            batch_ticker_rows += len(article.get("ticker_sentiment", []))
        del feed

        if not times:
            print(f"\nNo parseable times at time_to={time_to}. Done.")
            break

        oldest = min(times)
        total_articles += items
        total_ticker_rows += batch_ticker_rows

        print(
            f"{call_count:>5} | {time_to:>15} | {items:>6} | "
            f"{oldest.strftime('%Y-%m-%d %H:%M:%S'):>20} | "
            f"{batch_ticker_rows:>12} | {total_articles:>15}"
        )

        # Next time_to: truncate oldest to minute + 1 minute
        new_time_to = (
            oldest.replace(second=0, microsecond=0) + timedelta(minutes=1)
        ).strftime("%Y%m%dT%H%M")

        # Safety: ensure backward progress
        if new_time_to >= time_to:
            new_time_to = (
                oldest.replace(second=0, microsecond=0) - timedelta(minutes=1)
            ).strftime("%Y%m%dT%H%M")

        time_to = new_time_to

    elapsed_s = time.monotonic() - t_start
    elapsed_m = elapsed_s / 60

    print()
    print("=" * 95)
    print("Observed:")
    print(f"  API calls:        {call_count}")
    print(f"  Articles:         {total_articles}")
    print(f"  Ticker rows:      {total_ticker_rows}")
    print(f"  Reached time_to:  {time_to}")
    print(f"  Elapsed time:     {elapsed_m:.1f} min ({elapsed_s:.0f}s)")

    if call_count < 2:
        print("\nNot enough calls to extrapolate.")
        return

    # -- Extrapolation ---------------------------------------------------------
    # How much of the time span have we covered?
    try:
        reached_dt = datetime.strptime(time_to + "00", "%Y%m%dT%H%M%S").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        print("\nCould not parse reached time_to for extrapolation.")
        return

    covered_min = (start_dt - reached_dt).total_seconds() / 60
    remaining_min = (reached_dt - TIME_FROM_DT).total_seconds() / 60
    fraction_covered = covered_min / total_span_min if total_span_min > 0 else 1.0

    avg_sec_per_call = elapsed_s / call_count
    avg_rows_per_call = total_ticker_rows / call_count
    # minutes of history covered per API call
    min_per_call = covered_min / call_count

    est_remaining_calls = remaining_min / min_per_call if min_per_call > 0 else 0
    est_total_calls = call_count + est_remaining_calls
    est_total_time_s = est_total_calls * avg_sec_per_call
    est_total_rows = est_total_calls * avg_rows_per_call
    est_ram_bytes = est_total_rows * BYTES_PER_ROW

    print()
    print("Extrapolation to full 2010-01-01 fetch:")
    print(f"  Coverage so far:         {fraction_covered * 100:.1f}%")
    print(f"  Avg time per call:       {avg_sec_per_call:.2f}s")
    print(f"  Avg history per call:    {min_per_call:.0f} min ({min_per_call / 60 / 24:.1f} days)")
    print(f"  Avg ticker rows/call:    {avg_rows_per_call:.0f}")
    print()
    print(f"  Est. total API calls:    ~{est_total_calls:.0f}")
    print(f"  Est. total time:         ~{est_total_time_s / 3600:.1f} hours ({est_total_time_s / 60:.0f} min)")
    print(f"  Est. total ticker rows:  ~{est_total_rows:.0f}")
    print(f"  Est. RAM (DataFrame):    ~{est_ram_bytes / 1024 / 1024:.0f} MB ({est_ram_bytes / 1024 / 1024 / 1024:.2f} GB)")
    print()
    print(
        "Note: articles are denser in recent years. Older years likely have fewer"
        " articles per day, so calls will cover more time each. These estimates"
        " are upper bounds."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Estimate NEWS_SENTIMENT API call count"
    )
    parser.add_argument(
        "--max-calls", type=int, default=None,
        help="Stop after this many successful calls (default: run until empty)",
    )
    args = parser.parse_args()
    main(args.max_calls)
