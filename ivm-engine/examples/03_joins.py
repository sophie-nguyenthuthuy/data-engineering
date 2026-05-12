"""Example 3 — Incremental multi-table joins.

Scenario: orders joined with products (enrichment), and users joined with
subscriptions (LEFT JOIN to keep users with no subscription).

Demonstrates:
  - INNER JOIN: delta from either side propagates correctly.
  - LEFT JOIN: unmatched rows are emitted with NULL right columns.
  - Retraction from the right side updates previously joined rows.
  - Multi-hop: join result is further grouped by category.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ivm import IVMEngine
import ivm.aggregates as agg


def main():
    print("=" * 60)
    print("Incremental joins example")
    print("=" * 60)

    engine = IVMEngine()
    orders   = engine.source("orders")
    products = engine.source("products")

    # ----------------------------------------------------------------
    # INNER JOIN: orders ⋈ products on product_id
    # ----------------------------------------------------------------
    joined = orders.join(products, left_key="product_id", right_key="product_id")
    engine.register_view("enriched_orders", joined)

    # Downstream: revenue grouped by category (composed on top of the join)
    category_revenue = joined.group_by(
        ["category"],
        {"revenue": agg.Sum("amount"), "orders": agg.Count()},
    )
    engine.register_view("category_revenue", category_revenue)

    # Load the product catalog first
    catalog = [
        {"product_id": "p1", "name": "Laptop",   "category": "electronics"},
        {"product_id": "p2", "name": "Novel",     "category": "books"},
        {"product_id": "p3", "name": "Headphones","category": "electronics"},
    ]
    for p in catalog:
        engine.ingest("products", p, timestamp=0)

    print("\nProduct catalog loaded.  No orders yet.")
    print(f"  enriched_orders rows: {engine.row_count('enriched_orders')}")

    # Now orders arrive
    order_batch = [
        {"order_id": "o1", "product_id": "p1", "amount": 999},
        {"order_id": "o2", "product_id": "p2", "amount": 15},
        {"order_id": "o3", "product_id": "p3", "amount": 199},
        {"order_id": "o4", "product_id": "p1", "amount": 1100},
    ]
    for o in order_batch:
        engine.ingest("orders", o, timestamp=1000)

    print("\nAfter 4 orders:")
    for row in sorted(engine.query("enriched_orders"), key=lambda r: r["order_id"]):
        print(f"  {row['order_id']}  {row['name']:12s}  "
              f"category={row['category']}  amount=${row['amount']}")

    print("\nCategory revenue view (computed on top of join):")
    for row in sorted(engine.query("category_revenue"), key=lambda r: r["category"]):
        print(f"  {row['category']:12s}  revenue=${row['revenue']}  orders={row['orders']}")

    # ----------------------------------------------------------------
    # Right-side retraction: product p3 is discontinued
    # ----------------------------------------------------------------
    print("\nProduct p3 (Headphones) discontinued — retracted from catalog…")
    engine.retract("products",
                   {"product_id": "p3", "name": "Headphones", "category": "electronics"},
                   timestamp=2000)

    print("enriched_orders after right retraction:")
    for row in sorted(engine.query("enriched_orders"), key=lambda r: r["order_id"]):
        print(f"  {row['order_id']}  product={row['product_id']}  amount=${row['amount']}")

    print("category_revenue after right retraction:")
    for row in sorted(engine.query("category_revenue"), key=lambda r: r["category"]):
        print(f"  {row['category']:12s}  revenue=${row['revenue']}  orders={row['orders']}")

    # ----------------------------------------------------------------
    # LEFT JOIN: users ⋈ subscriptions — keep users without subs
    # ----------------------------------------------------------------
    print("\n" + "-" * 60)
    print("LEFT JOIN: users with optional subscriptions")
    engine2 = IVMEngine()
    users = engine2.source("users")
    subs  = engine2.source("subscriptions")

    left_joined = users.join(subs, left_key="user_id", right_key="user_id",
                             join_type="left")
    engine2.register_view("user_subs", left_joined)

    for u in [{"user_id": "alice"}, {"user_id": "bob"}, {"user_id": "carol"}]:
        engine2.ingest("users", u, timestamp=0)

    print("\nUsers loaded, no subscriptions yet:")
    for row in sorted(engine2.query("user_subs"), key=lambda r: r["user_id"]):
        print(f"  {row}")

    engine2.ingest("subscriptions", {"user_id": "alice", "plan": "pro"}, timestamp=1000)
    print("\nAfter alice subscribes:")
    for row in sorted(engine2.query("user_subs"), key=lambda r: r["user_id"]):
        print(f"  {row}")

    engine2.retract("subscriptions", {"user_id": "alice", "plan": "pro"}, timestamp=2000)
    print("\nAfter alice cancels subscription:")
    for row in sorted(engine2.query("user_subs"), key=lambda r: r["user_id"]):
        print(f"  {row}")


if __name__ == "__main__":
    main()
