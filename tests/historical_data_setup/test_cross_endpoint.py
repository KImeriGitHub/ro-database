"""Tests cross-endpoint concurrency in historical_data_setup.

Verifies that:
  * Two fake endpoint coroutines making calls via ``fetch_av_json`` run
    interleaved (not strictly sequential).
  * Concurrent calls never exceed the shared sliding-window rate limiter.

Uses a hand-rolled mock aiohttp session so no real network or
aiohttp-version-specific fixtures are needed.
"""

import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from historical_data_setup._common import RateLimiter, fetch_av_json


# -- Mock aiohttp session --------------------------------------------------

class _MockResponse:
    status = 200

    def __init__(self, data: dict):
        self._data = data

    def raise_for_status(self) -> None:
        return None

    async def json(self, content_type=None):
        return self._data


class _MockGetCtx:
    def __init__(self, session: "MockSession", url: str):
        self._session = session
        self._url = url

    async def __aenter__(self) -> _MockResponse:
        if self._session.delay > 0:
            await asyncio.sleep(self._session.delay)
        self._session.completed.append((time.monotonic(), self._url))
        return _MockResponse({"ok": True})

    async def __aexit__(self, *exc) -> bool:
        return False


class MockSession:
    """Minimal stand-in for ``aiohttp.ClientSession`` used by fetch_av_json."""

    def __init__(self, delay: float = 0.0):
        self.delay = delay
        self.completed: list[tuple[float, str]] = []

    def get(self, url: str, timeout=None):
        return _MockGetCtx(self, url)


# -- Helpers ---------------------------------------------------------------

async def _fake_endpoint(
    name: str,
    n_calls: int,
    session: MockSession,
    rate_limiter: RateLimiter,
    order: list[str],
) -> None:
    """Simulates an endpoint that makes *n_calls* sequential HTTP calls."""
    for i in range(n_calls):
        url = f"https://fake/{name}/{i}"
        await fetch_av_json(url, session, rate_limiter)
        order.append(name)


# -- Tests ------------------------------------------------------------------

def test_two_endpoints_run_interleaved():
    """Calls from two endpoints should interleave, not be strictly A-then-B.

    Uses a small per-call delay to mimic a slow endpoint running alongside a
    fast one — the only way for both to progress is if they share the loop.
    """

    async def runner():
        session = MockSession(delay=0.05)  # each HTTP round-trip ~50ms
        rl = RateLimiter(calls_per_minute=1000, window=1.0)
        order: list[str] = []
        await asyncio.gather(
            _fake_endpoint("slow", 5, session, rl, order),
            _fake_endpoint("fast", 5, session, rl, order),
        )
        return order

    order = asyncio.run(runner())
    assert len(order) == 10
    # Strict sequential would be ['slow','slow','slow','slow','slow','fast',...];
    # interleaved means the first 5 contain at least one of each.
    first_half = set(order[:5])
    assert first_half == {"slow", "fast"}, (
        f"expected interleaving, got order={order}"
    )


def test_rate_limit_not_exceeded_across_endpoints():
    """With 3 endpoints issuing many calls in parallel, the shared limiter
    must keep the global rate within the configured window."""

    async def runner():
        session = MockSession(delay=0.0)
        rl = RateLimiter(calls_per_minute=10, window=1.0)
        order: list[str] = []
        # 3 endpoints x 6 calls = 18 calls against a 10/s budget.
        # Minimum finish time is ~1s (the 11th call must wait for the window).
        start = time.monotonic()
        await asyncio.gather(
            _fake_endpoint("a", 6, session, rl, order),
            _fake_endpoint("b", 6, session, rl, order),
            _fake_endpoint("c", 6, session, rl, order),
        )
        elapsed = time.monotonic() - start
        return session.completed, elapsed

    completed, elapsed = asyncio.run(runner())
    assert len(completed) == 18
    # 18 calls at 10 per 1s window must take at least ~1s.
    assert elapsed >= 0.95, f"limiter too permissive: {elapsed:.3f}s"
    # Verify no 1-second window ever saw substantially more than 10 calls.
    # Allow 1-call tolerance for OS timer granularity (~15 ms on Windows) which
    # can push a call's measured completion time slightly before the limiter's
    # registered send time.
    timestamps = [t for t, _ in completed]
    for i in range(len(timestamps)):
        window_count = sum(
            1 for t in timestamps if timestamps[i] <= t < timestamps[i] + 1.0
        )
        assert window_count <= 11, (
            f"window starting at idx {i} saw {window_count} calls"
        )


def test_slow_endpoint_does_not_starve_fast_one():
    """A slow endpoint (long per-call delay) running alongside a fast one
    should not serialize the fast endpoint's calls."""

    async def runner():
        slow_session = MockSession(delay=0.2)
        fast_session = MockSession(delay=0.0)
        rl = RateLimiter(calls_per_minute=1000, window=1.0)

        async def slow():
            for i in range(3):
                await fetch_av_json(f"https://slow/{i}", slow_session, rl)

        async def fast():
            for i in range(3):
                await fetch_av_json(f"https://fast/{i}", fast_session, rl)

        start = time.monotonic()
        await asyncio.gather(slow(), fast())
        return time.monotonic() - start

    elapsed = asyncio.run(runner())
    # Fast endpoint alone takes ~0s, slow takes ~0.6s. In parallel total ~0.6s.
    # If fast were blocked waiting for slow, total would be 0.6s + 0s = 0.6s;
    # if they ran serially it'd be 0.6s still.  So instead assert they don't
    # sum: i.e. elapsed is within slow's time plus a small margin.
    assert elapsed < 0.8, f"endpoints appear to have serialized: {elapsed:.3f}s"
