"""Tests for history recording and entry parsing."""

import time
import pytest
from jepsen.core.history import History, Op, Entry


class TestHistory:
    def test_record_and_retrieve(self):
        h = History()
        op = Op(process=0, type="invoke", f="read", value="x")
        h.record(op)
        assert len(h) == 1
        assert h.ops()[0].process == 0

    def test_index_assigned_sequentially(self):
        h = History()
        ops = [Op(i, "invoke", "read", "x") for i in range(5)]
        for op in ops:
            h.record(op)
        indices = [o.index for o in h.ops()]
        assert indices == list(range(5))

    def test_entry_pairing(self):
        h = History()
        h.record(Op(0, "invoke", "read", "x", time=0.0))
        h.record(Op(0, "ok",     "read", 42,   time=1.0))
        entries = h.entries()
        assert len(entries) == 1
        e = entries[0]
        assert e.f == "read"
        assert e.invoke_value == "x"
        assert e.response_value == 42
        assert e.ok is True

    def test_failed_op_pairing(self):
        h = History()
        h.record(Op(1, "invoke", "write", ("x", 5), time=0.0))
        h.record(Op(1, "fail",   "write", "error",  time=0.5))
        entries = h.entries()
        assert len(entries) == 1
        assert entries[0].ok is False

    def test_dangling_invoke_becomes_failed_entry(self):
        h = History()
        # process 0 invokes but never gets a response (crashed)
        h.record(Op(0, "invoke", "read", "x",   time=0.0))
        h.record(Op(1, "invoke", "read", "y",   time=0.5))
        h.record(Op(1, "ok",     "read", None,  time=1.0))
        entries = h.entries()
        assert len(entries) == 2
        # The dangling invoke for process 0 should appear as failed
        dangling = [e for e in entries if e.process == 0]
        assert len(dangling) == 1
        assert not dangling[0].ok

    def test_thread_safety(self):
        import threading
        h = History()
        threads = []
        for i in range(10):
            t = threading.Thread(
                target=lambda pid=i: h.record(Op(pid, "invoke", "read", "x"))
            )
            threads.append(t)
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(h) == 10
        indices = sorted(o.index for o in h.ops())
        assert indices == list(range(10))
