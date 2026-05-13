"""Transport + retry-policy tests."""

from __future__ import annotations

import random

import pytest

from aph.retry import RetryError, RetryPolicy
from aph.transport import Response

# ----------------------------------------------------------------- Response


def test_response_is_success_2xx():
    assert Response(status=200, body={}).is_success()
    assert Response(status=299, body={}).is_success()
    assert not Response(status=300, body={}).is_success()


def test_response_is_retryable_codes():
    for code in (408, 425, 429, 500, 502, 503, 504):
        assert Response(status=code, body={}).is_retryable()
    for code in (200, 301, 400, 401, 404):
        assert not Response(status=code, body={}).is_retryable()


def test_response_header_lookup_case_insensitive():
    r = Response(status=200, body={}, headers={"Link": '<x>; rel="next"'})
    assert r.header("link") == r.header("LINK") == r.header("Link")
    assert r.header("missing") is None


# ----------------------------------------------------------------- RetryPolicy


def test_retry_validates_args():
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)
    with pytest.raises(ValueError):
        RetryPolicy(base=0)
    with pytest.raises(ValueError):
        RetryPolicy(multiplier=0)
    with pytest.raises(ValueError):
        RetryPolicy(cap=0.0001, base=0.1)


def test_retry_delay_no_jitter_is_exponential():
    p = RetryPolicy(base=0.1, multiplier=2.0, cap=100.0, jitter=False)
    assert p.delay(1) == pytest.approx(0.1)
    assert p.delay(2) == pytest.approx(0.2)
    assert p.delay(3) == pytest.approx(0.4)
    assert p.delay(4) == pytest.approx(0.8)


def test_retry_delay_caps_growth():
    p = RetryPolicy(base=1.0, multiplier=10.0, cap=5.0, jitter=False)
    assert p.delay(5) == pytest.approx(5.0)


def test_retry_delay_jitter_in_bounds():
    p = RetryPolicy(base=1.0, multiplier=2.0, cap=4.0, jitter=True, rng=random.Random(0))
    for k in range(1, 6):
        d = p.delay(k)
        assert 0.0 <= d <= min(1.0 * (2.0 ** (k - 1)), 4.0)


def test_retry_rejects_attempt_zero():
    with pytest.raises(ValueError):
        RetryPolicy().delay(0)


def test_retry_run_returns_first_success():
    p = RetryPolicy(max_attempts=3, jitter=False)
    calls = []

    def fn():
        calls.append(None)
        return 42

    assert p.run(fn) == 42
    assert len(calls) == 1


def test_retry_run_retries_on_exception_until_success():
    p = RetryPolicy(max_attempts=3, jitter=False)
    state = {"n": 0}

    def fn():
        state["n"] += 1
        if state["n"] < 3:
            raise ValueError("boom")
        return "ok"

    assert p.run(fn) == "ok"
    assert state["n"] == 3


def test_retry_run_raises_after_exhausting_budget():
    p = RetryPolicy(max_attempts=2, jitter=False)

    def fn():
        raise ValueError("boom")

    with pytest.raises(RetryError):
        p.run(fn)


def test_retry_run_failure_predicate_drives_retries():
    p = RetryPolicy(max_attempts=3, jitter=False)
    state = {"n": 0}

    def fn():
        state["n"] += 1
        return state["n"]

    out = p.run(fn, is_failure=lambda v: v < 2)
    assert out == 2
    assert state["n"] == 2


def test_retry_run_response_treats_503_as_retry():
    p = RetryPolicy(max_attempts=3, jitter=False)
    state = {"n": 0}

    def fn():
        state["n"] += 1
        if state["n"] < 3:
            return Response(status=503, body={})
        return Response(status=200, body={"ok": True})

    resp = p.run_response(fn)
    assert resp.status == 200
    assert state["n"] == 3


def test_retry_run_response_raises_when_never_succeeds():
    p = RetryPolicy(max_attempts=2, jitter=False)

    def fn():
        return Response(status=503, body={})

    with pytest.raises(RetryError):
        p.run_response(fn)
