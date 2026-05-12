"""End-to-end demo of the Volcano-to-Push Adaptive Query Engine.

Run with:  python examples/demo.py
"""
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from adaptive_engine import (
    AdaptiveEngine,
    AggregateNode,
    Catalog,
    FilterNode,
    HashJoinNode,
    LimitNode,
    ProjectNode,
    ScanNode,
    SortNode,
    eq,
    gt,
    plan_repr,
)
from adaptive_engine.optimizer import Optimizer
from adaptive_engine.push import PushCompiler
from adaptive_engine.volcano import VolcanoExecutor


# ------------------------------------------------------------------
# Seed data
# ------------------------------------------------------------------

random.seed(42)

PRODUCTS = [
    {
        "product_id": i,
        "name": f"Product-{i}",
        "category_id": i % 8,
        "price": round(random.uniform(5.0, 500.0), 2),
        "stock": random.randint(0, 200),
    }
    for i in range(1, 10_001)
]

CATEGORIES = [
    {"category_id": i, "label": f"Category-{i}", "discount_pct": i * 5}
    for i in range(8)
]

ORDERS = [
    {
        "order_id": i,
        "product_id": random.randint(1, 10_000),
        "quantity": random.randint(1, 10),
        "status": random.choice(["pending", "shipped", "cancelled"]),
    }
    for i in range(1, 50_001)
]


def build_catalog() -> Catalog:
    catalog = Catalog()
    # NOTE: deliberately underestimate rows to trigger adaptive switching
    catalog.create_table("products", PRODUCTS, estimated_rows=50)
    catalog.create_table("categories", CATEGORIES)
    catalog.create_table("orders", ORDERS, estimated_rows=100)
    return catalog


# ------------------------------------------------------------------
# Demo 1: pure Volcano vs pure Push
# ------------------------------------------------------------------

def demo_volcano_vs_push(catalog: Catalog) -> None:
    print("\n" + "=" * 60)
    print("DEMO 1: Volcano vs Push — filter + project")
    print("=" * 60)

    plan = FilterNode(
        child=ScanNode(table="products"),
        predicate=gt("price", 400.0),
        selectivity=0.05,
    )
    plan = ProjectNode(child=plan, columns=["product_id", "name", "price"])

    plan = Optimizer(catalog).optimize(plan)
    print("\nLogical plan:")
    print(plan_repr(plan))

    import time

    # Volcano
    t0 = time.perf_counter()
    volcano_rows = VolcanoExecutor(catalog).execute(plan)
    volcano_ms = (time.perf_counter() - t0) * 1000

    # Push
    t0 = time.perf_counter()
    push_rows = PushCompiler(catalog).compile(plan).run()
    push_ms = (time.perf_counter() - t0) * 1000

    print(f"\nVolcano: {len(volcano_rows)} rows in {volcano_ms:.2f} ms")
    print(f"Push:    {len(push_rows)} rows in {push_ms:.2f} ms")
    assert sorted(r["product_id"] for r in volcano_rows) == sorted(r["product_id"] for r in push_rows)
    print("✓ Both executors produce identical results")


# ------------------------------------------------------------------
# Demo 2: Adaptive engine detects hot path and mode-switches
# ------------------------------------------------------------------

def demo_adaptive_mode_switch(catalog: Catalog) -> None:
    print("\n" + "=" * 60)
    print("DEMO 2: Adaptive mode switch (estimated_rows=50, actual=10000)")
    print("=" * 60)

    # Query: filter expensive products, join with categories, aggregate
    plan = AggregateNode(
        child=HashJoinNode(
            left=FilterNode(
                child=ScanNode(table="products"),
                predicate=gt("price", 100.0),
                selectivity=0.8,
            ),
            right=ScanNode(table="categories"),
            left_key="category_id",
            right_key="category_id",
        ),
        group_by=["label"],
        aggregates=[
            ("product_count", "count", "product_id"),
            ("avg_price", "avg", "price"),
            ("total_stock", "sum", "stock"),
        ],
    )

    engine = AdaptiveEngine(
        catalog,
        hot_threshold=10.0,   # switch when actual > 10× estimate
        check_interval=25,
    )
    rows, report = engine.execute(plan)

    print(f"\nResults: {len(rows)} category groups")
    for r in sorted(rows, key=lambda x: x["label"]):
        print(
            f"  {r['label']:14s} → "
            f"count={r['product_count']:5d}  "
            f"avg_price=${r['avg_price']:6.2f}  "
            f"stock={r['total_stock']}"
        )

    print()
    print(report)


# ------------------------------------------------------------------
# Demo 3: Runtime re-optimization — join order / plan rewrite
# ------------------------------------------------------------------

def demo_reoptimization(catalog: Catalog) -> None:
    print("\n" + "=" * 60)
    print("DEMO 3: Runtime re-optimization (order side-swap + filter merge)")
    print("=" * 60)

    # Build a plan where estimates are wildly off:
    # orders has estimated_rows=100 but actual=50000
    plan = SortNode(
        child=ProjectNode(
            child=HashJoinNode(
                left=FilterNode(
                    child=ScanNode(table="orders"),
                    predicate=eq("status", "shipped"),
                    selectivity=0.8,
                ),
                right=ScanNode(table="products"),
                left_key="product_id",
                right_key="product_id",
            ),
            columns=["order_id", "name", "price", "quantity", "status"],
        ),
        order_by=[("price", False)],
    )

    engine = AdaptiveEngine(
        catalog,
        hot_threshold=10.0,
        check_interval=50,
        max_reopt_rounds=2,
    )
    rows, report = engine.execute(plan)

    print(f"\nQuery returned {len(rows)} shipped orders (sorted by price desc)")
    print(f"Top 5 by price:")
    for r in rows[:5]:
        print(f"  order={r['order_id']}  product={r['name']}  price=${r['price']}  qty={r['quantity']}")

    print()
    print(report)


# ------------------------------------------------------------------
# Demo 4: Volcano-only for small datasets (no switching overhead)
# ------------------------------------------------------------------

def demo_small_dataset() -> None:
    print("\n" + "=" * 60)
    print("DEMO 4: Small dataset — stays in Volcano mode (no switch)")
    print("=" * 60)

    catalog = Catalog()
    catalog.create_table(
        "tiny",
        [{"id": i, "val": i * 2} for i in range(50)],
        estimated_rows=50,
    )

    plan = FilterNode(
        child=ScanNode(table="tiny"),
        predicate=gt("val", 40),
        selectivity=0.5,
    )

    engine = AdaptiveEngine(catalog, hot_threshold=10.0, check_interval=10)
    rows, report = engine.execute(plan)

    print(f"\nResults: {len(rows)} rows, elapsed={report.elapsed_ms:.2f}ms")
    print(f"Mode switches: {len(report.mode_switches)}")
    print(f"Re-opt rounds: {report.reopt_rounds}")
    assert report.reopt_rounds == 0, "Should stay in volcano for accurate estimates"
    print("✓ No mode switches for well-estimated small query")


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

if __name__ == "__main__":
    print("Volcano-to-Push Adaptive Query Engine — Demo")
    catalog = build_catalog()
    demo_volcano_vs_push(catalog)
    demo_adaptive_mode_switch(catalog)
    demo_reoptimization(catalog)
    demo_small_dataset()
    print("\n✓ All demos complete")
