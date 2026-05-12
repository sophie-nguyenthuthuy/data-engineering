"""Demo: each IVM variant in action."""
from __future__ import annotations

from src import RowNumberIVM, CorrelatedSubqueryIVM, MaxOfSum, StrategyController


def main():
    print("=== ROW_NUMBER() OVER (PARTITION BY user ORDER BY ts) ===")
    ivm = RowNumberIVM()
    # User u1 click stream
    print("  Insert clicks for u1 out of order:")
    for t, rid in [(10, "c1"), (50, "c5"), (30, "c3"), (20, "c2"), (40, "c4")]:
        deltas = ivm.insert("u1", t, rid)
        print(f"    +({t}, {rid})  →  affected ranks: {deltas}")

    print("\n=== Correlated subquery: orders > per-customer AVG ===")
    cq = CorrelatedSubqueryIVM()
    events = [("c1", 100), ("c1", 200), ("c2", 50), ("c1", 500), ("c2", 150)]
    for cust, amt in events:
        result = cq.insert(cust, amt)
        print(f"  +({cust}, {amt})  added={result['added']}  removed={result['removed']}")
    print(f"  Final qualifying: {cq.qualifying()}")

    print("\n=== MAX(SUM(amount)) GROUP BY date ===")
    mos = MaxOfSum()
    events = [("d1", 100), ("d2", 50), ("d1", 80), ("d3", 200), ("d2", 200)]
    for date, amt in events:
        v, d = mos.insert(date, amt)
        print(f"  +({date}, {amt})  →  MAX = {v} on {d}")

    print("\n=== Strategy switcher under varying delta size ===")
    sc = StrategyController(alpha=0.5, beta=0.3)
    state_size = 10_000
    for delta in (10, 100, 1_000, 10_000, 100, 10):
        s = sc.decide(delta, state_size)
        print(f"  Δ={delta:>6}, |state|={state_size}  →  strategy = {s}")


if __name__ == "__main__":
    main()
