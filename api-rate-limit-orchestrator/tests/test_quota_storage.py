"""Quota + storage backend tests."""

from __future__ import annotations

import pytest

from arlo.quota import Quota
from arlo.storage.base import BucketState
from arlo.storage.inmemory import InMemoryStorage

# ----------------------------------------------------------------- Quota


def test_quota_rejects_zero_capacity():
    with pytest.raises(ValueError):
        Quota(capacity=0, refill_per_second=1)


def test_quota_rejects_zero_refill():
    with pytest.raises(ValueError):
        Quota(capacity=1, refill_per_second=0)


def test_quota_per_second_helper():
    q = Quota.per_second(10)
    assert q.capacity == 10
    assert q.refill_per_second == 10


def test_quota_per_minute_helper():
    q = Quota.per_minute(60)
    assert q.refill_per_second == pytest.approx(1.0)


def test_quota_per_hour_helper():
    q = Quota.per_hour(3600)
    assert q.refill_per_second == pytest.approx(1.0)


# ----------------------------------------------------------- BucketState


def test_bucket_state_rejects_negative_tokens():
    with pytest.raises(ValueError):
        BucketState(tokens=-1.0, last_refill_ts=0.0)


def test_bucket_state_rejects_negative_timestamp():
    with pytest.raises(ValueError):
        BucketState(tokens=1.0, last_refill_ts=-1.0)


# ------------------------------------------------------------- InMemoryStorage


def test_first_take_initialises_full_bucket():
    s = InMemoryStorage()
    took, state = s.atomic_take("k", capacity=5, refill_per_second=1, requested=1, now=10.0)
    assert took
    assert state.tokens == 4.0
    assert state.last_refill_ts == 10.0


def test_take_more_than_available_returns_false():
    s = InMemoryStorage()
    s.atomic_take("k", capacity=2, refill_per_second=1, requested=2, now=10.0)
    took, state = s.atomic_take("k", capacity=2, refill_per_second=1, requested=1, now=10.0)
    assert not took
    assert state.tokens == 0.0


def test_refill_caps_at_capacity():
    s = InMemoryStorage()
    s.atomic_take("k", capacity=5, refill_per_second=1, requested=1, now=0.0)  # 4 left
    took, state = s.atomic_take("k", capacity=5, refill_per_second=1, requested=0, now=1_000.0)
    assert took  # zero-cost take always succeeds
    assert state.tokens == 5.0


def test_refill_partial_after_elapsed_time():
    s = InMemoryStorage()
    s.atomic_take("k", capacity=10, refill_per_second=2, requested=10, now=0.0)
    took, state = s.atomic_take("k", capacity=10, refill_per_second=2, requested=3, now=2.0)
    # 2.0s * 2 rps = 4 refilled; minus 3 taken = 1 remaining
    assert took
    assert state.tokens == pytest.approx(1.0)


def test_take_zero_is_always_allowed():
    s = InMemoryStorage()
    took, _state = s.atomic_take("k", capacity=1, refill_per_second=1, requested=0, now=0.0)
    assert took


def test_take_negative_rejected():
    s = InMemoryStorage()
    with pytest.raises(ValueError):
        s.atomic_take("k", capacity=1, refill_per_second=1, requested=-1, now=0.0)


def test_separate_keys_independent():
    s = InMemoryStorage()
    s.atomic_take("a", capacity=1, refill_per_second=1, requested=1, now=0.0)
    took, _ = s.atomic_take("b", capacity=1, refill_per_second=1, requested=1, now=0.0)
    assert took  # "b" wasn't drained by "a"
