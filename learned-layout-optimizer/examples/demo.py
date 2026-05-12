"""Demo: bandit converges to optimal layout for a 2-D range workload."""
from __future__ import annotations

import numpy as np

from src import (
    Query, WorkloadProfile, Action, UCBPolicy, heuristic_action,
    expected_pages, reward,
)


def main():
    rng = np.random.default_rng(0)
    n = 2000
    data = rng.integers(0, 100, size=(n, 2))
    cols = ["x", "y"]

    # Workload: mostly 2D box queries with some 1D ranges
    workload = []
    for _ in range(50):
        lo_x = rng.integers(0, 80); hi_x = lo_x + rng.integers(5, 20)
        lo_y = rng.integers(0, 80); hi_y = lo_y + rng.integers(5, 20)
        workload.append(Query({"x": ("range", int(lo_x), int(hi_x)),
                               "y": ("range", int(lo_y), int(hi_y))}))
    for _ in range(20):
        v = int(rng.integers(0, 100))
        workload.append(Query({"x": ("=", v)}))

    print("=== Layout candidates ===")
    actions = [
        Action("noop", ()),
        Action("sortkey", ("x",)),
        Action("zorder", ("x", "y")),
        Action("hilbert", ("x", "y")),
    ]
    for a in actions:
        pages = expected_pages(data, cols, a, workload)
        print(f"  {repr(a):<25}  expected pages/query = {pages:.2f}")

    print("\n=== Heuristic recommendation ===")
    profile = WorkloadProfile(columns=cols)
    for q in workload:
        profile.observe(q)
    print(f"  Heuristic picks: {heuristic_action(profile)}")

    print("\n=== UCB1 bandit (200 trials) ===")
    policy = UCBPolicy(actions=actions, c=0.5)
    for _ in range(200):
        a = policy.choose()
        r = reward(data, cols, a, workload, io_cost=50.0)
        policy.update(a, r)

    print(f"  Plays per action:")
    for a in actions:
        print(f"    {repr(a):<25} plays={policy._plays[repr(a)]:>3}  "
              f"mean reward={policy.mean(a):>6.2f}")


if __name__ == "__main__":
    main()
