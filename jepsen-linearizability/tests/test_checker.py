"""Unit tests for the linearizability checker.

These tests construct hand-crafted histories with known outcomes,
verifying the WGL checker against canonical examples from the literature.
"""

import pytest
from jepsen.core.history import Entry
from jepsen.core.checker import check
from jepsen.core.models import RegisterModel, QueueModel


def entry(idx, p, f, inv_val, resp_val, t_inv, t_res, ok=True):
    return Entry(
        index=idx, process=p, f=f,
        invoke_value=inv_val, response_value=resp_val,
        invoke_time=t_inv, response_time=t_res, ok=ok,
    )


# ── Register model ─────────────────────────────────────────────────────────────

class TestRegisterLinearizable:
    def test_empty_history(self):
        r = check([], RegisterModel())
        assert r.linearizable

    def test_single_write(self):
        e = [entry(0, 0, "write", ("x", 1), "ok", 0.0, 1.0)]
        r = check(e, RegisterModel())
        assert r.linearizable

    def test_write_then_read_sequential(self):
        # w(x=1) completes before r(x) starts → must read 1
        e = [
            entry(0, 0, "write", ("x", 1), "ok", 0.0, 1.0),
            entry(1, 1, "read",  "x",       1,    1.5, 2.0),
        ]
        r = check(e, RegisterModel())
        assert r.linearizable

    def test_stale_read_sequential(self):
        # Sequential: write x=1 completes, then read returns None → violation
        e = [
            entry(0, 0, "write", ("x", 1), "ok",  0.0, 1.0),
            entry(1, 1, "read",  "x",       None,  1.5, 2.0),
        ]
        r = check(e, RegisterModel())
        assert not r.linearizable

    def test_concurrent_ops_linearizable(self):
        # Write and read overlap in time — read returns the written value,
        # which is consistent with write being linearized before read.
        e = [
            entry(0, 0, "write", ("x", 42), "ok",  0.0, 2.0),
            entry(1, 1, "read",  "x",       42,     0.5, 1.5),
        ]
        r = check(e, RegisterModel())
        assert r.linearizable

    def test_concurrent_ops_nil_read_linearizable(self):
        # Read returns None because it is linearized before the write.
        e = [
            entry(0, 0, "write", ("x", 42), "ok",  0.0, 2.0),
            entry(1, 1, "read",  "x",       None,   0.5, 1.5),
        ]
        r = check(e, RegisterModel())
        assert r.linearizable

    def test_two_writes_read_returns_neither(self):
        # Concurrent writes, read returns an unexpected value → violation
        e = [
            entry(0, 0, "write", ("x", 1), "ok",  0.0, 2.0),
            entry(1, 1, "write", ("x", 2), "ok",  0.0, 2.0),
            entry(2, 2, "read",  "x",      3,      2.5, 3.0),
        ]
        r = check(e, RegisterModel())
        assert not r.linearizable

    def test_multi_key_isolation(self):
        # Write to x doesn't affect y
        e = [
            entry(0, 0, "write", ("x", 10), "ok",   0.0, 1.0),
            entry(1, 1, "read",  "y",        None,    1.5, 2.0),
        ]
        r = check(e, RegisterModel())
        assert r.linearizable

    def test_failed_write_no_state_change(self):
        # A failed write may or may not have taken effect.
        # Reading None afterward is still valid (write might not have applied).
        e = [
            entry(0, 0, "write", ("x", 99), "ok",   0.0, 1.0, ok=False),
            entry(1, 1, "read",  "x",        None,   1.5, 2.0),
        ]
        r = check(e, RegisterModel())
        assert r.linearizable


class TestRegisterNotLinearizable:
    def test_read_after_write_returns_wrong_value(self):
        e = [
            entry(0, 0, "write", ("x", 5), "ok",  0.0, 0.5),
            entry(1, 1, "read",  "x",      7,      0.6, 1.0),
        ]
        r = check(e, RegisterModel())
        assert not r.linearizable

    def test_classic_violation(self):
        # Classic anomaly: p0 writes x=1, p1 reads x=None after write completes.
        e = [
            entry(0, 0, "write", ("x", 1), "ok",  0.0, 1.0),
            entry(1, 1, "read",  "x",       None,   2.0, 3.0),
        ]
        r = check(e, RegisterModel())
        assert not r.linearizable

    def test_monotonic_read_violation(self):
        # p0 writes x=1 then x=2 sequentially.
        # p1 reads x=2, then later reads x=1 → non-monotonic.
        e = [
            entry(0, 0, "write", ("x", 1), "ok",  0.0, 0.5),
            entry(1, 0, "write", ("x", 2), "ok",  0.6, 1.1),
            entry(2, 1, "read",  "x",      2,      1.2, 1.5),
            entry(3, 1, "read",  "x",      1,      1.6, 2.0),
        ]
        r = check(e, RegisterModel())
        assert not r.linearizable


# ── Queue model ────────────────────────────────────────────────────────────────

class TestQueueModel:
    def test_enqueue_dequeue_linearizable(self):
        e = [
            entry(0, 0, "enqueue", "a", "ok",   0.0, 0.5),
            entry(1, 1, "dequeue", None, "a",    0.6, 1.0),
        ]
        r = check(e, QueueModel())
        assert r.linearizable

    def test_dequeue_empty_linearizable(self):
        e = [
            entry(0, 0, "dequeue", None, "empty", 0.0, 0.5),
        ]
        r = check(e, QueueModel())
        assert r.linearizable

    def test_wrong_dequeue_order(self):
        # FIFO violation: enqueue a then b, but dequeue returns b first
        e = [
            entry(0, 0, "enqueue", "a", "ok",  0.0, 0.5),
            entry(1, 0, "enqueue", "b", "ok",  0.6, 1.0),
            entry(2, 1, "dequeue", None, "b",   1.1, 1.5),
        ]
        r = check(e, QueueModel())
        assert not r.linearizable


# ── Linearization witness ──────────────────────────────────────────────────────

class TestLinearizationWitness:
    def test_linearization_populated_on_success(self):
        e = [
            entry(0, 0, "write", ("x", 1), "ok",  0.0, 1.0),
            entry(1, 1, "read",  "x",      1,      1.5, 2.0),
        ]
        r = check(e, RegisterModel())
        assert r.linearizable
        assert len(r.linearization) == 2

    def test_linearization_empty_on_failure(self):
        e = [
            entry(0, 0, "write", ("x", 1), "ok",  0.0, 0.5),
            entry(1, 1, "read",  "x",      999,    0.6, 1.0),
        ]
        r = check(e, RegisterModel())
        assert not r.linearizable
        assert r.linearization == []
