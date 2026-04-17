"""Tests for the sliding-window RateLimiter in historical_data_setup._common.

Uses plain ``asyncio.run`` in sync wrappers to avoid a pytest-asyncio dependency.
"""

import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from historical_data_setup._common import RateLimiter


def test_budget_fits_without_sleep():
    """Fewer calls than the budget should complete near-instantly."""

    async def runner():
        rl = RateLimiter(calls_per_minute=10, window=1.0, min_gap=0.0)
        start = time.monotonic()
        for _ in range(10):
            await rl.wait()
        return time.monotonic() - start

    elapsed = asyncio.run(runner())
    # 10 calls with budget 10/window should not block.
    assert elapsed < 0.1, f"budget unexpectedly exceeded: {elapsed:.3f}s"


def test_exceeds_budget_forces_sleep():
    """The (N+1)th call in a window must sleep until the oldest falls off."""

    async def runner():
        rl = RateLimiter(calls_per_minute=5, window=1.0)
        start = time.monotonic()
        for _ in range(5):
            await rl.wait()
        # 6th call must wait until the oldest timestamp is > 1s old.
        await rl.wait()
        return time.monotonic() - start

    elapsed = asyncio.run(runner())
    assert elapsed >= 0.95, f"6th call did not wait long enough: {elapsed:.3f}s"
    # Allow some slack but rate limiter shouldn't overshoot dramatically.
    assert elapsed < 1.5, f"6th call waited too long: {elapsed:.3f}s"


def test_concurrent_waiters_obey_global_budget():
    """Many concurrent coroutines must collectively respect the window."""

    async def runner():
        rl = RateLimiter(calls_per_minute=10, window=1.0)
        # 20 concurrent callers, budget of 10 per 1s window -> ~2s minimum.
        start = time.monotonic()
        await asyncio.gather(*[rl.wait() for _ in range(20)])
        return time.monotonic() - start

    elapsed = asyncio.run(runner())
    assert elapsed >= 0.95, f"20 concurrent callers finished too fast: {elapsed:.3f}s"


def test_min_gap_enforces_spacing():
    """min_gap must prevent micro-bursts even when the window budget is ample."""

    async def runner():
        rl = RateLimiter(calls_per_minute=1000, window=60.0, min_gap=0.05)
        start = time.monotonic()
        timestamps: list[float] = []
        for _ in range(5):
            await rl.wait()
            timestamps.append(time.monotonic())
        return start, timestamps

    start, timestamps = asyncio.run(runner())
    # 5 calls spaced by >= 0.05s each -> at least 0.2s for calls 2..5.
    elapsed = timestamps[-1] - start
    assert elapsed >= 0.2, f"min_gap not enforced: {elapsed:.3f}s"
    # Consecutive gaps must each be >= min_gap (minus a small OS timer slack).
    for i in range(1, len(timestamps)):
        gap = timestamps[i] - timestamps[i - 1]
        assert gap >= 0.045, f"gap {i}: {gap:.4f}s < min_gap"


def test_window_slides_forward():
    """After the window passes, old timestamps must be evicted so new slots are free."""

    async def runner():
        rl = RateLimiter(calls_per_minute=3, window=0.5, min_gap=0.0)
        for _ in range(3):
            await rl.wait()
        # Budget full; wait beyond window naturally.
        await asyncio.sleep(0.6)
        start = time.monotonic()
        for _ in range(3):
            await rl.wait()
        return time.monotonic() - start

    elapsed = asyncio.run(runner())
    assert elapsed < 0.1, f"expected evicted slots to be free, took {elapsed:.3f}s"
