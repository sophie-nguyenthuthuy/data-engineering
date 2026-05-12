"""MVCC + Snapshot Isolation + transactions."""

from __future__ import annotations

import threading

import pytest

from art_mvcc.mvcc.tx import TxConflict, begin_tx
from art_mvcc.mvcc.version import VersionChain


class TestSnapshot:
    def test_basic_isolation(self, db):
        db.put(b"k", "v1")
        s = db.begin_snapshot()
        db.put(b"k", "v2")
        assert s.get(b"k") == "v1"
        s2 = db.begin_snapshot()
        assert s2.get(b"k") == "v2"

    def test_delete_visible_to_new_snapshots(self, db):
        db.put(b"k", "v1")
        s_before = db.begin_snapshot()
        db.delete(b"k")
        s_after = db.begin_snapshot()
        assert s_before.get(b"k") == "v1"
        assert s_after.get(b"k") is None

    def test_phantom_read_prevented(self, db):
        """Inserts after snapshot are invisible to the snapshot."""
        db.put(b"k1", "v1")
        s = db.begin_snapshot()
        db.put(b"k2", "v2")
        # Old snapshot still sees only k1
        result = s.scan_prefix(b"")
        keys = {k for k, _ in result}
        assert keys == {b"k1"}

    def test_long_running_snapshot_unaffected_by_many_writes(self, db):
        db.put(b"k", 0)
        s = db.begin_snapshot()
        for i in range(100):
            db.put(b"k", i + 1)
        assert s.get(b"k") == 0


class TestTransaction:
    def test_basic_commit(self, db):
        with begin_tx(db) as t:
            t.put(b"k", "v")
            t.commit()
        assert db.begin_snapshot().get(b"k") == "v"

    def test_rollback_discards_writes(self, db):
        db.put(b"k", "original")
        with begin_tx(db) as t:
            t.put(b"k", "rolled-back")
            t.rollback()
        assert db.begin_snapshot().get(b"k") == "original"

    def test_own_writes_visible_to_self(self, db):
        with begin_tx(db) as t:
            t.put(b"k", "v")
            assert t.get(b"k") == "v"
            t.commit()

    def test_other_writes_invisible_until_commit(self, db):
        t1 = begin_tx(db)
        t2 = begin_tx(db)
        t1.put(b"k", "v")
        assert t2.get(b"k") is None
        t1.commit()
        # Still invisible to t2 (its start_ts is older than commit_ts)
        assert t2.get(b"k") is None

    def test_first_committer_wins(self, db):
        t1 = begin_tx(db)
        t2 = begin_tx(db)
        t1.put(b"k", "from_t1")
        t2.put(b"k", "from_t2")
        t1.commit()
        with pytest.raises(TxConflict):
            t2.commit()
        # The earlier committer's value survives
        assert db.begin_snapshot().get(b"k") == "from_t1"

    def test_disjoint_writes_dont_conflict(self, db):
        t1 = begin_tx(db)
        t2 = begin_tx(db)
        t1.put(b"a", 1)
        t2.put(b"b", 2)
        t1.commit()
        t2.commit()  # should not raise
        s = db.begin_snapshot()
        assert s.get(b"a") == 1
        assert s.get(b"b") == 2

    def test_committed_after_use_blocks(self, db):
        with begin_tx(db) as t:
            t.put(b"k", "v")
            t.commit()
            with pytest.raises(RuntimeError):
                t.put(b"x", "y")

    def test_concurrent_writer_serialised_via_lock(self, db):
        """64 threads each increment a counter via CAS-pattern (retry on conflict)."""
        db.put(b"counter", 0)
        errors: list[BaseException] = []

        def worker() -> None:
            for _ in range(50):
                while True:
                    t = begin_tx(db)
                    cur = t.get(b"counter")
                    t.put(b"counter", (cur or 0) + 1)
                    try:
                        t.commit()
                        break
                    except TxConflict:
                        continue
                    except BaseException as e:
                        errors.append(e)
                        return

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        assert errors == []
        assert db.begin_snapshot().get(b"counter") == 8 * 50


class TestVersionChain:
    def test_empty_chain_returns_none(self):
        c = VersionChain()
        assert c.read_at(0) is None
        assert c.read_at(999) is None

    def test_tentative_invisible_to_snapshots(self):
        c = VersionChain()
        c.tentative_write(txn_id=1, value="pending")
        assert c.read_at(0) is None
        assert c.read_at(999) is None

    def test_gc_drops_old_versions(self):
        from art_mvcc.mvcc.version import Version
        c = VersionChain()
        # Build a chain: ts=10, ts=20, ts=30 (committed)
        for ts in (10, 20, 30):
            c._versions.insert(0, Version(commit_ts=ts, value=ts, txn_id=None))
        # GC anything strictly older than the base visible at ts=20
        dropped = c.gc_below(20)
        assert dropped == 1   # ts=10 dropped
        assert len(c) == 2


class TestGarbageCollection:
    def test_old_versions_reclaimed(self, db):
        for i in range(10):
            db.put(b"k", i)
        # Now reclaim everything below now-1
        dropped = db.gc(watermark_ts=db.now() - 1)
        assert dropped >= 8     # only base+head remain
        # But latest still visible
        assert db.begin_snapshot().get(b"k") == 9
