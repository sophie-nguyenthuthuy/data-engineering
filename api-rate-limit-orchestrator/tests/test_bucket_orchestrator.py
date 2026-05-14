"""TokenBucket + Orchestrator tests, including multi-threaded property check."""

from __future__ import annotations

import threading
import time
from itertools import count

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from arlo.bucket import AcquireResult, TokenBucket
from arlo.orchestrator import AcquireTimeout, Orchestrator
from arlo.quota import Quota
from arlo.storage.inmemory import InMemoryStorage


def _bucket(capacity: float = 5.0, rps: float = 10.0, clock=None) -> TokenBucket:
    return TokenBucket(
        key="k",
        quota=Quota(capacity=capacity, refill_per_second=rps),
        storage=InMemoryStorage(),
        clock=clock or time.monotonic,
    )


# ------------------------------------------------------------ TokenBucket


def test_acquire_result_rejects_invariants():
    with pytest.raises(ValueError):
        AcquireResult(took=False, tokens_remaining=-1, suggested_wait=0)
    with pytest.raises(ValueError):
        AcquireResult(took=False, tokens_remaining=0, suggested_wait=-1)


def test_bucket_rejects_empty_key():
    with pytest.raises(ValueError):
        TokenBucket(
            key="",
            quota=Quota.per_second(1),
            storage=InMemoryStorage(),
        )


def test_acquire_rejects_zero_tokens():
    b = _bucket()
    with pytest.raises(ValueError):
        b.acquire(0.0)


def test_acquire_rejects_more_than_capacity():
    b = _bucket(capacity=5)
    with pytest.raises(ValueError):
        b.acquire(6.0)


def test_acquire_first_token_takes_and_leaves_capacity_minus_one():
    ticks = iter([0.0])
    b = _bucket(capacity=5, rps=1, clock=lambda: next(ticks))
    r = b.acquire(1.0)
    assert r.took
    assert r.tokens_remaining == 4.0
    assert r.suggested_wait == 0.0


def test_acquire_failure_reports_wait_proportional_to_deficit():
    counter = count(start=0)
    b = _bucket(capacity=1, rps=2, clock=lambda: float(next(counter)) * 0.0)
    b.acquire(1.0)  # drains
    r = b.acquire(1.0)
    assert not r.took
    # Refill 2/s → 0.5s to recover one token.
    assert r.suggested_wait == pytest.approx(0.5, rel=0.01)


# ----------------------------------------------------------- Orchestrator


def test_orchestrator_rejects_invariants():
    b = _bucket()
    with pytest.raises(ValueError):
        Orchestrator(bucket=b, max_wait=0)
    with pytest.raises(ValueError):
        Orchestrator(bucket=b, max_attempts=0)
    with pytest.raises(ValueError):
        Orchestrator(bucket=b, min_sleep=-1)


def test_orchestrator_returns_immediately_when_bucket_has_tokens():
    sleeps: list[float] = []
    o = Orchestrator(bucket=_bucket(), sleep=sleeps.append)
    out = o.wait_and_acquire()
    assert out.took
    assert sleeps == []


def test_orchestrator_waits_then_succeeds():
    """Drain the bucket; the next call must sleep, then succeed."""
    ts = iter([0.0, 0.0, 1.0])  # acquire(now=0); fail; succeed-after-1s
    b = _bucket(capacity=1, rps=1, clock=lambda: next(ts))
    b.acquire(1.0)  # drain
    sleeps: list[float] = []
    o = Orchestrator(bucket=b, max_wait=5.0, sleep=sleeps.append)
    out = o.wait_and_acquire()
    assert out.took
    assert len(sleeps) >= 1


def test_orchestrator_times_out_when_budget_exhausted():
    b = _bucket(capacity=1, rps=0.001)  # refills 1 token / 1000s — way too slow
    b.acquire(1.0)  # drain
    o = Orchestrator(bucket=b, max_wait=0.1, max_attempts=10, sleep=lambda _s: None)
    with pytest.raises(AcquireTimeout):
        o.wait_and_acquire()


# ----------------------------------------------------------- Concurrency


def test_concurrent_workers_never_exceed_quota():
    """N threads sharing one bucket take ≤ (capacity + refill * elapsed).

    Uses a *deterministic* fake clock so we can bound the simulated
    elapsed time exactly — avoids GIL-scheduling noise that makes
    wallclock bounds flaky.
    """
    import itertools

    rps = 10.0
    capacity = 5.0
    step = 0.01  # each clock read advances by 10 ms

    counter = itertools.count()
    clock_lock = threading.Lock()

    def fake_clock() -> float:
        with clock_lock:
            return next(counter) * step

    bucket = _bucket(capacity=capacity, rps=rps, clock=fake_clock)

    n_workers = 8
    iters_per_worker = 50
    counts = [0] * n_workers

    def worker(i: int) -> None:
        for _ in range(iters_per_worker):
            if bucket.acquire(1.0).took:
                counts[i] += 1

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    total = sum(counts)
    # Simulated max elapsed = clock-call-count * step. One clock call per acquire (in bucket).
    max_calls = next(counter)  # current counter value bounds advancement
    max_elapsed = max_calls * step
    upper = capacity + max_elapsed * rps
    assert total <= upper, f"served {total} > theoretical {upper}"


# ----------------------------------------------------------- Hypothesis


@settings(max_examples=30, deadline=None)
@given(
    cap=st.integers(1, 50),
    rps=st.integers(1, 100),
    requests=st.integers(0, 50),
)
def test_property_total_taken_never_exceeds_capacity_at_t0(cap, rps, requests):
    """With no elapsed time the bucket cannot serve more than its capacity."""
    counter = count(start=0)
    bucket = _bucket(capacity=cap, rps=rps, clock=lambda: float(next(counter)) * 0.0)
    taken = 0
    for _ in range(requests):
        if bucket.acquire(1.0).took:
            taken += 1
    assert taken <= cap
