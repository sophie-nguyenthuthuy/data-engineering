"""Example 1 — GROUP BY aggregations with retractions.

Scenario: e-commerce orders arriving in real time.
View: revenue and order count per product category.

Key points demonstrated:
  - SUM, COUNT, AVG, MIN, MAX all maintained incrementally.
  - When an order is cancelled (retracted), the view self-corrects without
    recomputing from scratch.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ivm import IVMEngine
import ivm.aggregates as agg


def main():
    engine = IVMEngine()
    orders = engine.source("orders")

    # ----------------------------------------------------------------
    # View: per-category revenue summary
    # ----------------------------------------------------------------
    summary = (
        orders
        .filter(lambda r: r["status"] != "cancelled")
        .group_by(
            key_columns=["category"],
            aggregates={
                "total_revenue": agg.Sum("amount"),
                "order_count":   agg.Count(),
                "avg_order":     agg.Avg("amount"),
                "min_order":     agg.Min("amount"),
                "max_order":     agg.Max("amount"),
            },
        )
    )
    engine.register_view("category_summary", summary)

    print("=" * 60)
    print("GROUP BY example — e-commerce order stream")
    print("=" * 60)

    # Ingest a batch of orders
    batch = [
        {"id": 1, "category": "books",       "amount": 25,  "status": "completed"},
        {"id": 2, "category": "electronics", "amount": 299, "status": "completed"},
        {"id": 3, "category": "books",       "amount": 18,  "status": "completed"},
        {"id": 4, "category": "clothing",    "amount": 75,  "status": "completed"},
        {"id": 5, "category": "electronics", "amount": 149, "status": "completed"},
    ]
    for order in batch:
        engine.ingest("orders", order, timestamp=1000)

    print("\nAfter initial 5 orders:")
    for row in sorted(engine.query("category_summary"), key=lambda r: r["category"]):
        print(f"  {row['category']:12s}  revenue={row['total_revenue']:>6.2f}  "
              f"count={row['order_count']}  avg={row['avg_order']:>6.2f}  "
              f"min={row['min_order']}  max={row['max_order']}")

    # ----------------------------------------------------------------
    # Simulate a return / cancellation — retract order #3
    # ----------------------------------------------------------------
    print("\nCustomer returns order #3 (books, $18)…")
    engine.retract("orders",
                   {"id": 3, "category": "books", "amount": 18, "status": "completed"},
                   timestamp=2000)

    print("\nAfter retraction:")
    for row in sorted(engine.query("category_summary"), key=lambda r: r["category"]):
        print(f"  {row['category']:12s}  revenue={row['total_revenue']:>6.2f}  "
              f"count={row['order_count']}  avg={row['avg_order']:>6.2f}  "
              f"min={row['min_order']}  max={row['max_order']}")

    # ----------------------------------------------------------------
    # Demonstrate delta log — only the changed rows were emitted
    # ----------------------------------------------------------------
    print("\nDelta log (last 4 deltas):")
    for delta in engine.recent_deltas("category_summary", n=4):
        print(f"  {delta}")

    # ----------------------------------------------------------------
    # All books orders retracted → group disappears from view
    # ----------------------------------------------------------------
    print("\nRetracting the last books order…")
    engine.retract("orders",
                   {"id": 1, "category": "books", "amount": 25, "status": "completed"},
                   timestamp=3000)

    cats = [r["category"] for r in engine.query("category_summary")]
    print(f"  Remaining categories: {sorted(cats)}")
    assert "books" not in cats, "books group should have been removed"
    print("  'books' group correctly removed when count reached 0 ✓")


if __name__ == "__main__":
    main()
