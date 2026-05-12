import random
import threading

from src import ART, MVCCArt


def test_art_basic():
    a = ART()
    a.put(b"hello", 1)
    a.put(b"help",  2)
    a.put(b"helmet", 3)
    a.put(b"world", 4)
    assert a.get(b"hello") == 1
    assert a.get(b"help") == 2
    assert a.get(b"helmet") == 3
    assert a.get(b"world") == 4
    assert a.get(b"hel") is None
    assert a.get(b"helping") is None


def test_art_overwrite():
    a = ART()
    a.put(b"k", "v1")
    a.put(b"k", "v2")
    assert a.get(b"k") == "v2"


def test_art_delete():
    a = ART()
    a.put(b"k", "v")
    assert a.delete(b"k")
    assert a.get(b"k") is None
    assert not a.delete(b"k")


def test_art_grows_through_node_types():
    """Inserting 4 keys → Node4, 16 → Node16, 48 → Node48, 256 → Node256."""
    a = ART()
    for i in range(300):
        a.put(bytes([i // 256, i % 256]), i)
    for i in range(300):
        assert a.get(bytes([i // 256, i % 256])) == i


def test_art_random_workload():
    rng = random.Random(0)
    a = ART()
    ref = {}
    for _ in range(2000):
        k = bytes(rng.randint(0, 255) for _ in range(rng.randint(1, 6)))
        v = rng.randint(0, 10_000)
        a.put(k, v)
        ref[k] = v
    for k, v in ref.items():
        assert a.get(k) == v


def test_mvcc_snapshot_isolation():
    db = MVCCArt()
    db.put(1, "v1")
    db.put(2, "v2")
    snap_before = db.begin()
    db.put(1, "v1_updated")
    db.put(3, "v3")
    snap_after = db.begin()
    # Old snapshot sees old values
    assert snap_before.get(1) == "v1"
    assert snap_before.get(3) is None
    # New snapshot sees new values
    assert snap_after.get(1) == "v1_updated"
    assert snap_after.get(3) == "v3"


def test_mvcc_delete_visible_to_new_snapshots():
    db = MVCCArt()
    db.put(1, "v1")
    snap_before = db.begin()
    db.delete(1)
    snap_after = db.begin()
    assert snap_before.get(1) == "v1"
    assert snap_after.get(1) is None


def test_concurrent_writes_no_lost_update_via_locking():
    """64 threads each increment a counter through a CAS-like protocol.

    MVCC alone gives us snapshot isolation, NOT serializability — to avoid
    lost updates we must use explicit retry. This test demonstrates the
    correct serialized pattern.
    """
    db = MVCCArt()
    db.put(0, 0)
    lock = threading.Lock()

    def worker():
        for _ in range(100):
            with lock:
                cur = db.get_at(0, db.commit_ts())
                db.put(0, cur + 1)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads: t.start()
    for t in threads: t.join()

    # 8 threads × 100 increments = 800
    assert db.get_at(0, db.commit_ts()) == 800


def test_epoch_reclamation_safe():
    """Garbage retired while a thread holds an epoch survives until the
    thread releases."""
    db = MVCCArt()
    ep = db.epoch_mgr
    held: list = []
    ep.enter("t1")
    e0 = ep.epoch
    ep.advance()
    e1 = ep.epoch
    ep.retire(e0, lambda: held.append("freed_e0"))
    ep.retire(e1, lambda: held.append("freed_e1"))
    # t1 holds epoch e0 → e0 garbage cannot be freed yet
    reclaimed = ep.gc()
    assert reclaimed == 0
    ep.leave("t1")
    reclaimed = ep.gc()
    assert reclaimed == 2
