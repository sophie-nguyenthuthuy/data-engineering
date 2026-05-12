import numpy as np

from src import (
    Query, WorkloadProfile, Action, UCBPolicy, heuristic_action,
    apply_layout, expected_pages, reward, z_order_index,
)


def test_zorder_basic():
    coords = np.array([[0, 0], [1, 0], [0, 1], [1, 1]], dtype=np.uint64)
    idx = z_order_index(coords)
    # Z-order: (0,0)=0, (1,0)=1, (0,1)=2, (1,1)=3
    assert list(idx) == [0, 1, 2, 3]


def test_workload_profile_tracks_frequency():
    p = WorkloadProfile(columns=["a", "b", "c"])
    for _ in range(10):
        p.observe(Query({"a": ("=", 1)}))
    for _ in range(3):
        p.observe(Query({"b": ("range", 0, 5)}))
    assert p.freq("a") > p.freq("b")
    assert p.freq("c") == 0


def test_heuristic_action_picks_top_cols():
    p = WorkloadProfile(columns=["a", "b", "c"])
    for _ in range(20):
        p.observe(Query({"a": ("=", 1), "b": ("=", 2)}))
    action = heuristic_action(p)
    assert action.kind in {"zorder", "hilbert", "sortkey"}
    # Both a and b should appear
    assert "a" in action.cols
    assert "b" in action.cols


def test_apply_layout_sortkey_orders_rows():
    data = np.array([[3, 1], [1, 2], [2, 0]], dtype=np.int64)
    perm = apply_layout(data, ["x", "y"], Action("sortkey", ("x",)))
    sorted_data = data[perm]
    assert list(sorted_data[:, 0]) == [1, 2, 3]


def test_layout_reduces_pages_for_relevant_query():
    """Z-order on (x,y) should reduce pages scanned for a box query."""
    rng = np.random.default_rng(0)
    n = 1000
    data = rng.integers(0, 100, size=(n, 2))
    cols = ["x", "y"]
    # Box query: x in [40,60], y in [40,60]
    q = Query({"x": ("range", 40, 60), "y": ("range", 40, 60)})

    pages_noop  = expected_pages(data, cols, Action("noop", ()), [q])
    pages_zord  = expected_pages(data, cols, Action("zorder", ("x", "y")), [q])

    assert pages_zord < pages_noop, f"zorder didn't help: {pages_zord} >= {pages_noop}"


def test_ucb_converges_to_best_action():
    """UCB1 should converge to the highest-reward arm."""
    actions = [
        Action("noop", ()),
        Action("zorder", ("x", "y")),
        Action("sortkey", ("x",)),
    ]
    policy = UCBPolicy(actions=actions, c=0.5)
    # Reward: zorder is best (1.0), sortkey medium (0.5), noop worst (0.1)
    rewards = {repr(actions[0]): 0.1, repr(actions[1]): 1.0, repr(actions[2]): 0.5}
    for _ in range(200):
        a = policy.choose()
        policy.update(a, rewards[repr(a)])
    # Most plays should be on zorder
    assert policy._plays[repr(actions[1])] > policy._plays[repr(actions[0])]
    assert policy._plays[repr(actions[1])] > policy._plays[repr(actions[2])]


def test_reward_penalises_useless_rewrite():
    """Rewriting when noop is already optimal should yield negative reward."""
    rng = np.random.default_rng(7)
    data = rng.integers(0, 100, size=(500, 2))
    cols = ["x", "y"]
    q = Query({"x": ("=", 50)})

    r_noop = reward(data, cols, Action("noop", ()), [q] * 100)
    r_useless = reward(data, cols, Action("hilbert", ("x", "y")), [q] * 100, io_cost=10_000)
    assert r_useless < r_noop
