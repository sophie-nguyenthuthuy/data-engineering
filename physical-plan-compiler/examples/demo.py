"""Demo: compile several logical plans, show chosen engines + costs."""
from __future__ import annotations

from src import Source, Filter, Aggregate, Join, plan


def show(label, lp):
    p = plan(lp)
    print(f"\n=== {label} ===")
    print(f"  Logical: {lp}")
    print(f"  Plan:    {p.op}")
    print(f"  Cost:    {p.total_cost:.1f}   Output bytes: {p.output_bytes:,.0f}")


def main():
    show("Tiny filter", Filter(Source("small", 1_000), "x > 0"))
    show("Huge filter", Filter(Source("big", 100_000_000), "x > 0"))
    show("Medium aggregate",
         Aggregate(Filter(Source("events", 10_000_000), "type='click'", 0.1),
                   group_by=["user"], aggs=["count(*)"]))
    show("Streaming-ish (huge join)",
         Join(Source("clicks", 1_000_000_000),
              Source("impressions", 5_000_000_000),
              join_key=["ad_id"]))


if __name__ == "__main__":
    main()
