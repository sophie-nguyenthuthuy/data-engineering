"""
Query Federation Engine — interactive demo.

Spins up four in-memory mock data sources (Postgres, MongoDB, S3 Parquet,
REST API) and runs a series of increasingly complex federated SQL queries,
printing the results and the query plan for each.

Run with:
    python examples/demo.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow running from the repo root without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd

from federation.catalog import SourceType
from federation.engine import FederationEngine

# ──────────────────────────────────────────────────────────────────────────────
# 1. Build mock datasets
# ──────────────────────────────────────────────────────────────────────────────

orders_df = pd.DataFrame(
    {
        "id":         [1, 2, 3, 4, 5, 6],
        "user_id":    [10, 20, 10, 30, 20, 10],
        "total":      [99.9, 250.0, 30.0, 500.0, 75.5, 420.0],
        "status":     ["shipped", "pending", "shipped", "cancelled", "shipped", "shipped"],
        "created_at": pd.to_datetime(
            ["2024-01-10", "2024-02-01", "2024-03-15",
             "2024-04-01", "2024-04-20", "2024-05-01"]
        ),
    }
)

users_df = pd.DataFrame(
    {
        "id":      [10, 20, 30],
        "name":    ["Alice", "Bob", "Carol"],
        "country": ["US", "UK", "US"],
        "tier":    ["gold", "silver", "gold"],
    }
)

events_df = pd.DataFrame(
    {
        "order_id":   [1, 1, 2, 3, 5, 6],
        "event_type": ["view", "purchase", "view", "purchase", "purchase", "purchase"],
        "channel":    ["web", "web", "mobile", "web", "app", "web"],
    }
)

products_df = pd.DataFrame(
    {
        "id":       [101, 102, 103, 104],
        "name":     ["Widget", "Gadget", "Doohickey", "Thingamajig"],
        "category": ["electronics", "electronics", "hardware", "hardware"],
        "price":    [19.99, 49.99, 9.99, 34.99],
    }
)

# ──────────────────────────────────────────────────────────────────────────────
# 2. Create the engine with mock data
# ──────────────────────────────────────────────────────────────────────────────

engine = FederationEngine.__new__(FederationEngine)

from federation.catalog import SchemaCatalog
from federation.executor import Executor
from federation.planner import CostBasedOptimizer, QueryPlanner

engine.catalog = SchemaCatalog()
engine._planner = QueryPlanner(engine.catalog)
engine._optimizer = CostBasedOptimizer()
engine._executor = Executor(engine.catalog)

engine.register_mock_table("postgres",   "orders",   SourceType.POSTGRES,   orders_df)
engine.register_mock_table("mongodb",    "users",    SourceType.MONGODB,    users_df)
engine.register_mock_table("s3_parquet", "events",   SourceType.S3_PARQUET, events_df)
engine.register_mock_table("rest_api",   "products", SourceType.REST_API,   products_df)

# ──────────────────────────────────────────────────────────────────────────────
# 3. Demo queries
# ──────────────────────────────────────────────────────────────────────────────

def banner(title: str) -> None:
    width = 72
    print("\n" + "═" * width)
    print(f"  {title}")
    print("═" * width)


def run(label: str, sql: str, show_plan: bool = False) -> None:
    banner(label)
    print(f"SQL:\n  {sql.strip()}\n")

    if show_plan:
        plan_text = engine.explain(sql)
        print("Query Plan:")
        for line in plan_text.splitlines():
            print("  " + line)
        print()

    df, stats = engine.query(sql)
    print(df.to_string(index=False) if not df.empty else "(no rows)")
    print()
    print(stats.summary())


# ── Demo 1: single source with predicate pushdown ────────────────────────────
run(
    "Demo 1 — Postgres: shipped orders over $50 (predicate pushed to source)",
    "SELECT id, total, status FROM postgres.orders WHERE status = 'shipped' AND total > 50",
    show_plan=True,
)

# ── Demo 2: MongoDB filter ────────────────────────────────────────────────────
run(
    "Demo 2 — MongoDB: gold-tier users in the US",
    "SELECT name, country, tier FROM mongodb.users WHERE country = 'US' AND tier = 'gold'",
)

# ── Demo 3: S3 Parquet filter ─────────────────────────────────────────────────
run(
    "Demo 3 — S3 Parquet: purchase events (row-group pruning pushed to PyArrow)",
    "SELECT order_id, event_type, channel FROM s3_parquet.events WHERE event_type = 'purchase'",
)

# ── Demo 4: two-source join (Postgres × MongoDB) ─────────────────────────────
run(
    "Demo 4 — Cross-source JOIN: orders enriched with user names (Postgres × MongoDB)",
    """
    SELECT o.id, o.total, o.status, u.name, u.country
    FROM postgres.orders o
    JOIN mongodb.users u ON o.user_id = u.id
    WHERE o.status = 'shipped'
    """,
    show_plan=True,
)

# ── Demo 5: three-source join ─────────────────────────────────────────────────
run(
    "Demo 5 — Three-source JOIN: orders + users + events (Postgres × MongoDB × S3)",
    """
    SELECT o.id, u.name, e.event_type, e.channel
    FROM postgres.orders o
    JOIN mongodb.users u     ON o.user_id  = u.id
    JOIN s3_parquet.events e ON e.order_id = o.id
    WHERE e.event_type = 'purchase'
    """,
    show_plan=True,
)

# ── Demo 6: all four sources ──────────────────────────────────────────────────
run(
    "Demo 6 — All four sources: orders + users + events + products",
    """
    SELECT o.id, u.name, e.channel, p.name, p.category
    FROM postgres.orders o
    JOIN mongodb.users u     ON o.user_id  = u.id
    JOIN s3_parquet.events e ON e.order_id = o.id
    JOIN rest_api.products p ON p.id       = o.id
    """,
)

# ── Demo 7: LIMIT ─────────────────────────────────────────────────────────────
run(
    "Demo 7 — LIMIT pushed to source scan",
    "SELECT id, total FROM postgres.orders LIMIT 3",
)

print("\n✓ All demos complete.\n")
