import random

from src import BEpsilonTree, EpsilonTuner


def test_get_missing():
    t = BEpsilonTree(epsilon=0.5)
    assert t.get(42) is None


def test_put_then_get():
    t = BEpsilonTree(epsilon=0.5)
    t.put(1, "one")
    t.put(2, "two")
    assert t.get(1) == "one"
    assert t.get(2) == "two"
    assert t.get(3) is None


def test_overwrite():
    t = BEpsilonTree(epsilon=0.5)
    t.put(1, "v1")
    t.put(1, "v2")
    assert t.get(1) == "v2"


def test_delete():
    t = BEpsilonTree(epsilon=0.5)
    t.put(1, "v1")
    t.delete(1)
    assert t.get(1) is None


def test_many_puts_and_gets_random():
    rng = random.Random(0)
    t = BEpsilonTree(epsilon=0.5)
    ref = {}
    for _ in range(2000):
        k = rng.randint(0, 500)
        v = rng.randint(0, 10_000)
        t.put(k, v)
        ref[k] = v
    for k in ref:
        assert t.get(k) == ref[k], f"mismatch for key {k}"
    # Random non-existent
    for k in range(501, 600):
        if k not in ref:
            assert t.get(k) is None


def test_mixed_workload():
    rng = random.Random(1)
    t = BEpsilonTree(epsilon=0.5)
    ref = {}
    for _ in range(2000):
        op = rng.choice(["put", "put", "put", "del"])  # 75% put, 25% del
        k = rng.randint(0, 100)
        if op == "put":
            v = rng.randint(0, 10_000)
            t.put(k, v)
            ref[k] = v
        else:
            t.delete(k)
            ref.pop(k, None)
    for k in range(0, 100):
        assert t.get(k) == ref.get(k)


def test_tree_grows_in_depth():
    """With many inserts, tree must split to depth > 1."""
    t = BEpsilonTree(epsilon=0.5)
    for i in range(500):
        t.put(i, i)
    assert t.depth() > 1
    # All keys retrievable
    for i in range(500):
        assert t.get(i) == i


def test_epsilon_tuner_adapts_to_workload():
    tuner = EpsilonTuner(window=100, hysteresis=0.01)
    # Read-heavy
    for _ in range(100):
        tuner.observe("read")
    eps_read = tuner.recommend()
    # Write-heavy
    tuner._events.clear()
    for _ in range(100):
        tuner.observe("write")
    eps_write = tuner.recommend()
    assert eps_write > eps_read
    assert 0 < eps_read < 0.5
    assert eps_write > 0.5
