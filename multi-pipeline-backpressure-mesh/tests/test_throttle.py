"""Tests for TokenBucketThrottle."""
import asyncio
import time

import pytest

from mesh.throttle import TokenBucketThrottle


@pytest.mark.asyncio
async def test_full_rate_passes_immediately():
    t = TokenBucketThrottle(rate=10000.0)
    start = time.monotonic()
    for _ in range(100):
        await t.acquire()
    elapsed = time.monotonic() - start
    assert elapsed < 0.5, f"Expected near-instant but took {elapsed:.2f}s"


@pytest.mark.asyncio
async def test_throttle_factor_zero_delays():
    t = TokenBucketThrottle(rate=1000.0)
    t.set_throttle_factor(0.0)
    start = time.monotonic()
    await t.acquire()
    elapsed = time.monotonic() - start
    assert elapsed >= 0.05, "Zero factor should block (sleep 0.1s)"


@pytest.mark.asyncio
async def test_throttle_factor_adjusts_rate():
    t = TokenBucketThrottle(rate=1000.0)
    t.set_throttle_factor(0.5)
    assert t.effective_rate == 500.0


def test_set_throttle_factor_clamps():
    t = TokenBucketThrottle(rate=100.0)
    t.set_throttle_factor(2.0)
    assert t._factor == 1.0
    t.set_throttle_factor(-0.5)
    assert t._factor == 0.0


def test_reset_restores_baseline():
    t = TokenBucketThrottle(rate=500.0)
    t.set_throttle_factor(0.1)
    t.reset()
    assert t._factor == 1.0
    assert t._rate == 500.0
