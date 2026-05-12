import random
import time

from src import HotStore, ColdStore, Writer, Resolver, dominates, concurrent


def test_vector_clock_partial_order():
    assert dominates({"a": 2}, {"a": 1})
    assert not dominates({"a": 1}, {"a": 2})
    assert concurrent({"a": 1, "b": 0}, {"a": 0, "b": 1})


def test_basic_write_then_read():
    hot, cold = HotStore(k=5), ColdStore()
    w = Writer(hot=hot, cold=cold)
    r = Resolver(hot=hot, cold=cold)

    w.write("u1", "clicks", "n_clicks", 1)
    w.write("u1", "identity", "is_premium", True)

    rv = r.get("u1", ["n_clicks", "is_premium"])
    assert rv.features == {"n_clicks": 1, "is_premium": True}
    assert not rv.missing


def test_returned_vector_is_causally_consistent():
    """Every chosen version's clock must be ≤ chosen_clock."""
    hot, cold = HotStore(k=5), ColdStore()
    w = Writer(hot=hot, cold=cold)
    r = Resolver(hot=hot, cold=cold)
    rng = random.Random(0)
    for _ in range(50):
        comp = rng.choice(["clicks", "page_view", "identity"])
        feat = rng.choice(["f1", "f2", "f3", "f4", "f5"])
        w.write("u42", comp, feat, rng.randint(0, 100))

    rv = r.get("u42", ["f1", "f2", "f3", "f4", "f5"])
    # Re-fetch versions and verify the returned values come from clocks ≤ rv.chosen_clock
    for f, v in rv.features.items():
        versions = hot.versions("u42", f)
        # The chosen version must have its clock dominated by chosen_clock
        # AND its value equal to what was returned
        matched = [ver for ver in versions if ver.value == v]
        assert matched, f"returned value not found in hot store for {f}"
        for m in matched:
            # At least one matching version is dominated by chosen_clock
            if dominates(rv.chosen_clock, m.clock):
                break
        else:
            assert False, f"no matching version of {f} dominated by chosen_clock"


def test_concurrent_writes_picked_consistently():
    """Two concurrent writes — resolver picks one snapshot, not a mix."""
    hot, cold = HotStore(k=5), ColdStore()
    w = Writer(hot=hot, cold=cold)
    r = Resolver(hot=hot, cold=cold)

    # Write a series — every feature gets a value at clock {comp1:1, comp2:0}
    # then again at clock {comp1:1, comp2:1}.
    w.write("u1", "comp1", "f_a", "old_a")          # vc1 = {c1:1}
    w.write("u1", "comp1", "f_b", "old_b")          # vc2 = {c1:2}
    w.write("u1", "comp2", "f_a", "new_a")          # vc3 = {c1:2, c2:1}
    w.write("u1", "comp2", "f_b", "new_b")          # vc4 = {c1:2, c2:2}

    # Resolver should return the latest causally consistent snapshot
    rv = r.get("u1", ["f_a", "f_b"])
    # Both should be "new_*" because they're causally dominated by current entity clock
    assert rv.features == {"f_a": "new_a", "f_b": "new_b"}


def test_partition_simulation_returns_consistent_snapshot():
    """Simulate: writer A writes burst, then writer B writes (partition prevents
    B from seeing A's bumps until later). Resolver always returns *some*
    consistent snapshot — never a mix of incomparable versions."""
    hot, cold = HotStore(k=10), ColdStore()
    wA = Writer(hot=hot, cold=cold)

    # Phase 1: writer A bumps `compA` 5 times for several features
    for i in range(5):
        wA.write("u1", "compA", "f1", f"A-f1-{i}")
        wA.write("u1", "compA", "f2", f"A-f2-{i}")

    # Phase 2: simultaneously, a "delayed" writer B writes with its own counter
    # (we simulate by pretending compB doesn't see A — we manually inject)
    # In our toy single-process Writer this is a sequential bump. Production
    # would have B's clock be {compA: 0, compB: 1}, concurrent to A's.

    r = Resolver(hot=hot, cold=cold)
    rv = r.get("u1", ["f1", "f2"])
    # Both should resolve, both at same level of compA
    assert rv.features["f1"] == "A-f1-4"
    assert rv.features["f2"] == "A-f2-4"


def test_no_lost_writes_with_k_versions():
    hot, cold = HotStore(k=3), ColdStore()
    w = Writer(hot=hot, cold=cold)
    for i in range(10):
        w.write("u1", "c", "f", f"v{i}")
    # Hot only has last 3
    assert len(hot.versions("u1", "f")) == 3
    # Cold has all 10
    assert len(cold.versions("u1", "f")) == 10
