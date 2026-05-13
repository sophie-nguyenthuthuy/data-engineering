"""Writer + Resolver end-to-end tests."""

from __future__ import annotations

import random
import threading

import pytest

from cfs.clock.vector_clock import dominates
from cfs.serving.resolver import Resolver
from cfs.store.cold import ColdStore
from cfs.store.hot import HotStore
from cfs.writer import Writer


def _stack(k: int = 8) -> tuple[HotStore, ColdStore, Writer, Resolver]:
    hot, cold = HotStore(k=k), ColdStore()
    return hot, cold, Writer(hot=hot, cold=cold), Resolver(hot=hot, cold=cold)


# ----------------------------------------------------------------- writer


def test_writer_rejects_empty_entity():
    _, _, w, _ = _stack()
    with pytest.raises(ValueError):
        w.write("", "c", "f", 0)


def test_writer_rejects_empty_component():
    _, _, w, _ = _stack()
    with pytest.raises(ValueError):
        w.write("u", "", "f", 0)


def test_writer_rejects_empty_feature():
    _, _, w, _ = _stack()
    with pytest.raises(ValueError):
        w.write("u", "c", "", 0)


def test_writer_bumps_per_component():
    _, _, w, _ = _stack()
    c1 = w.write("u1", "clicks", "f", 1, wall=1.0)
    c2 = w.write("u1", "clicks", "f", 2, wall=2.0)
    c3 = w.write("u1", "identity", "g", "p", wall=3.0)
    assert c1 == {"clicks": 1}
    assert c2 == {"clicks": 2}
    assert c3 == {"clicks": 2, "identity": 1}


def test_writer_current_clock_unknown_entity_empty():
    _, _, w, _ = _stack()
    assert w.current_clock("nobody") == {}


# --------------------------------------------------------------- resolver


def test_resolver_rejects_empty_entity():
    _, _, _, r = _stack()
    with pytest.raises(ValueError):
        r.get("", ["f"])


def test_resolver_empty_feature_list_is_trivially_complete():
    _, _, _, r = _stack()
    rv = r.get("u1", [])
    assert rv.features == {} and rv.missing == [] and rv.is_complete()


def test_resolver_returns_latest_consistent_snapshot():
    _, _, w, r = _stack()
    w.write("u1", "clicks", "n_clicks", 1, wall=1.0)
    w.write("u1", "identity", "is_premium", True, wall=2.0)
    rv = r.get("u1", ["n_clicks", "is_premium"])
    assert rv.features == {"n_clicks": 1, "is_premium": True}
    assert rv.is_complete()


def test_resolver_picks_latest_value_for_same_feature():
    _, _, w, r = _stack()
    w.write("u1", "clicks", "n_clicks", 1, wall=1.0)
    w.write("u1", "clicks", "n_clicks", 2, wall=2.0)
    w.write("u1", "clicks", "n_clicks", 3, wall=3.0)
    rv = r.get("u1", ["n_clicks"])
    assert rv.features == {"n_clicks": 3}


def test_resolver_marks_unknown_feature_as_missing():
    _, _, w, r = _stack()
    w.write("u1", "clicks", "f", 1, wall=1.0)
    rv = r.get("u1", ["f", "g"])
    assert rv.features == {"f": 1}
    assert rv.missing == ["g"]


def test_resolver_verifies_chosen_clock_dominates_each_version():
    _, _, w, r = _stack()
    rng = random.Random(0)
    for _ in range(50):
        comp = rng.choice(["clicks", "pageview", "identity"])
        feat = rng.choice(["f1", "f2", "f3", "f4", "f5"])
        w.write("u42", comp, feat, rng.randint(0, 100), wall=rng.uniform(0, 100))
    rv = r.get("u42", ["f1", "f2", "f3", "f4", "f5"])
    assert r.verify("u42", rv)


def test_resolver_falls_back_to_cold_after_hot_eviction():
    hot = HotStore(k=2)
    cold = ColdStore()
    w = Writer(hot=hot, cold=cold)
    r = Resolver(hot=hot, cold=cold)
    for i in range(10):
        w.write("u1", "c", "f", i, wall=float(i))
    # Hot retains only the last 2; cold has all 10.
    assert len(hot.versions("u1", "f")) == 2
    assert len(cold.versions("u1", "f")) == 10
    rv = r.get("u1", ["f"])
    # Latest value (i=9) is reachable through hot.
    assert rv.features["f"] == 9


def test_resolver_uses_cold_when_hot_only_has_recent_unrelated_writes():
    """If a feature is never written via Writer but pre-loaded into cold,
    the resolver still finds it as long as the entity clock dominates the
    cold version's clock."""
    hot, cold = HotStore(k=5), ColdStore()
    w = Writer(hot=hot, cold=cold)
    # Pre-populate cold with a feature clocked at compA=1.
    cold.write("u1", "old_feature", "fossil", clock={"compA": 1}, wall=0.5)
    # Now drive the entity clock forward via the writer.
    w.write("u1", "compA", "fresh_feature", "new", wall=1.0)
    r = Resolver(hot=hot, cold=cold)
    rv = r.get("u1", ["old_feature", "fresh_feature"])
    assert rv.features == {"old_feature": "fossil", "fresh_feature": "new"}


def test_resolver_chosen_clock_dominates_all_returned_versions():
    _, _, w, r = _stack()
    w.write("u1", "compA", "f1", "x", wall=1.0)
    w.write("u1", "compB", "f2", "y", wall=2.0)
    rv = r.get("u1", ["f1", "f2"])
    for f, value in rv.features.items():
        for v in [vv for vv in w.hot.versions("u1", f) if vv.value == value]:
            assert dominates(rv.chosen_clock, v.clock)


def test_resolver_returns_empty_chosen_clock_when_only_missing():
    _, _, _, r = _stack()
    rv = r.get("u1", ["never_written"])
    assert rv.features == {}
    assert rv.chosen_clock == {}
    assert rv.missing == ["never_written"]


# --------------------------------------------------- multi-thread writes


def test_concurrent_writers_yield_causally_consistent_snapshot():
    hot, cold = HotStore(k=200), ColdStore()
    w = Writer(hot=hot, cold=cold)
    r = Resolver(hot=hot, cold=cold)

    def producer(comp: str) -> None:
        for i in range(100):
            w.write("u1", comp, f"feat-{i % 5}", value=(comp, i), wall=float(i))

    threads = [
        threading.Thread(target=producer, args=("compA",)),
        threading.Thread(target=producer, args=("compB",)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    rv = r.get("u1", [f"feat-{i}" for i in range(5)])
    assert r.verify("u1", rv)
