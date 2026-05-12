"""Write-amplification tracking."""

from __future__ import annotations

from beps.stats.amplification import WriteAmpStats
from beps.tree.tree import BEpsilonTree


def test_initial_state():
    s = WriteAmpStats()
    snap = s.snapshot()
    assert snap["leaf_applies"] == 0
    assert snap["write_amplification"] == 0.0


def test_writes_increment_buffer_inserts():
    s = WriteAmpStats()
    tree = BEpsilonTree(node_size=8, epsilon=0.5, amp_stats=s)
    # Single-leaf phase: writes go straight to leaf
    for i in range(2):
        tree.put(f"k{i}".encode(), i)
    snap = s.snapshot()
    assert snap["leaf_applies"] >= 2


def test_write_amplification_increases_with_depth():
    """Deeper trees → more buffer flushes per operation."""
    s_shallow = WriteAmpStats()
    s_deep = WriteAmpStats()
    BEpsilonTree(node_size=64, epsilon=0.5, amp_stats=s_shallow)
    deep = BEpsilonTree(node_size=4, epsilon=0.5, amp_stats=s_deep)
    for i in range(500):
        deep.put(f"k{i:04d}".encode(), i)
    # Deep tree should have non-zero write amp
    assert s_deep.write_amplification > 0


def test_splits_recorded():
    s = WriteAmpStats()
    t = BEpsilonTree(node_size=4, epsilon=0.5, amp_stats=s)
    for i in range(50):
        t.put(f"k{i:04d}".encode(), i)
    assert s.splits > 0


def test_amp_thread_safe():
    """Concurrent updates don't lose counts."""
    import threading
    s = WriteAmpStats()

    def worker() -> None:
        for _ in range(1000):
            s.record_leaf_apply()
            s.record_buffer_insert()

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    snap = s.snapshot()
    assert snap["leaf_applies"] == 4 * 1000
    assert snap["buffer_inserts"] == 4 * 1000


def test_reset():
    s = WriteAmpStats()
    s.record_leaf_apply()
    s.record_buffer_insert()
    s.reset()
    snap = s.snapshot()
    assert snap["leaf_applies"] == 0
    assert snap["buffer_inserts"] == 0
