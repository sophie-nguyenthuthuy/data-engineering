"""
Seeds the warehouse with realistic demo tables and metadata.
Run: python -m demo.seed
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime, timedelta, timezone
import random

from warehouse.core.warehouse import SelfDescribingWarehouse
from warehouse.core.registry import TableMeta, ColumnMeta


def seed(wh: SelfDescribingWarehouse) -> None:
    _create_and_register_tables(wh)
    _seed_lineage(wh)
    _seed_quality(wh)
    _seed_freshness(wh)
    _seed_usage(wh)
    _seed_incidents(wh)


# ------------------------------------------------------------------ #
#  Tables                                                              #
# ------------------------------------------------------------------ #

def _create_and_register_tables(wh: SelfDescribingWarehouse) -> None:
    # --- orders (fact table) ---
    wh.create_table("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id    TEXT NOT NULL,
            customer_id TEXT NOT NULL,
            order_date  TEXT NOT NULL,
            revenue     REAL NOT NULL,
            region      TEXT NOT NULL,
            product_sku TEXT NOT NULL,
            status      TEXT NOT NULL
        )
    """)
    wh.insert_many("orders", [
        {
            "order_id":    f"ORD-{1000+i}",
            "customer_id": f"CUS-{random.randint(1, 300):04d}",
            "order_date":  (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
            "revenue":     round(random.uniform(10, 5000), 2),
            "region":      random.choice(["APAC", "EMEA", "NA", "LATAM"]),
            "product_sku": random.choice(["PRD-A", "PRD-B", "PRD-C", "PRD-D"]),
            "status":      random.choice(["completed", "completed", "completed", "refunded", "pending"]),
        }
        for i in range(2000)
    ])
    wh.registry.register_table(TableMeta(
        table_name="orders",
        description="All customer orders including revenue, region, product, and fulfilment status. Primary fact table for revenue analysis.",
        owner="data-engineering@acme.com",
        domain="finance",
        source_system="Stripe",
        update_frequency="hourly",
        tags=["revenue", "orders", "fact-table", "stripe", "finance"],
        columns=[
            ColumnMeta("order_id",    "TEXT",  "Unique order identifier",                    is_nullable=False),
            ColumnMeta("customer_id", "TEXT",  "Reference to customers table",               is_nullable=False),
            ColumnMeta("order_date",  "TEXT",  "ISO-8601 order date",                        is_nullable=False),
            ColumnMeta("revenue",     "REAL",  "Gross revenue in USD",                       is_nullable=False),
            ColumnMeta("region",      "TEXT",  "Geographic region (APAC/EMEA/NA/LATAM)",     sample_values=["APAC","EMEA","NA","LATAM"]),
            ColumnMeta("product_sku", "TEXT",  "Product SKU",                                sample_values=["PRD-A","PRD-B"]),
            ColumnMeta("status",      "TEXT",  "Order status: completed/refunded/pending",   sample_values=["completed","refunded"]),
        ],
    ))

    # --- monthly_revenue_summary (aggregate) ---
    wh.create_table("""
        CREATE TABLE IF NOT EXISTS monthly_revenue_summary (
            month       TEXT NOT NULL,
            region      TEXT NOT NULL,
            total_revenue REAL NOT NULL,
            order_count   INTEGER NOT NULL,
            avg_order_value REAL NOT NULL
        )
    """)
    months = [(2024, m) for m in range(1, 13)] + [(2025, m) for m in range(1, 5)]
    rows = []
    for yr, mo in months:
        for region in ["APAC", "EMEA", "NA", "LATAM"]:
            count = random.randint(50, 400)
            revenue = round(random.uniform(20000, 200000), 2)
            rows.append({
                "month": f"{yr}-{mo:02d}",
                "region": region,
                "total_revenue": revenue,
                "order_count": count,
                "avg_order_value": round(revenue / count, 2),
            })
    wh.insert_many("monthly_revenue_summary", rows)
    wh.registry.register_table(TableMeta(
        table_name="monthly_revenue_summary",
        description="Pre-aggregated monthly revenue by region. Updated nightly from orders. Best table for monthly revenue dashboards and exec reporting.",
        owner="analytics@acme.com",
        domain="finance",
        source_system="dbt/internal",
        update_frequency="daily",
        tags=["revenue", "monthly", "aggregate", "summary", "finance", "reporting"],
        columns=[
            ColumnMeta("month",           "TEXT", "YYYY-MM format",             is_nullable=False),
            ColumnMeta("region",          "TEXT", "Geographic region",          is_nullable=False),
            ColumnMeta("total_revenue",   "REAL", "Sum of gross revenue (USD)", is_nullable=False),
            ColumnMeta("order_count",     "INTEGER", "Number of completed orders", is_nullable=False),
            ColumnMeta("avg_order_value", "REAL", "Average revenue per order",  is_nullable=False),
        ],
    ))

    # --- customers ---
    wh.create_table("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id   TEXT NOT NULL,
            email         TEXT NOT NULL,
            name          TEXT NOT NULL,
            country       TEXT NOT NULL,
            segment       TEXT NOT NULL,
            created_at    TEXT NOT NULL
        )
    """)
    wh.insert_many("customers", [
        {
            "customer_id": f"CUS-{i:04d}",
            "email":       f"user{i}@example.com",
            "name":        f"Customer {i}",
            "country":     random.choice(["US", "UK", "DE", "JP", "AU", "BR"]),
            "segment":     random.choice(["enterprise", "smb", "individual"]),
            "created_at":  (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1500))).isoformat(),
        }
        for i in range(1, 301)
    ])
    wh.registry.register_table(TableMeta(
        table_name="customers",
        description="Master customer dimension. Contains PII (email, name). Sourced from CRM. Use for customer segmentation, cohort analysis, and joining to orders.",
        owner="data-engineering@acme.com",
        domain="product",
        source_system="Salesforce CRM",
        update_frequency="daily",
        tags=["customers", "dimension", "crm", "pii", "segmentation"],
        columns=[
            ColumnMeta("customer_id", "TEXT",  "Unique customer ID",       is_nullable=False),
            ColumnMeta("email",       "TEXT",  "Customer email address",   is_pii=True, is_nullable=False),
            ColumnMeta("name",        "TEXT",  "Full name",                is_pii=True),
            ColumnMeta("country",     "TEXT",  "ISO country code",         sample_values=["US","UK","DE"]),
            ColumnMeta("segment",     "TEXT",  "enterprise/smb/individual",sample_values=["enterprise","smb"]),
            ColumnMeta("created_at",  "TEXT",  "Account creation date"),
        ],
    ))

    # --- events (user behaviour) ---
    wh.create_table("""
        CREATE TABLE IF NOT EXISTS events (
            event_id      TEXT NOT NULL,
            customer_id   TEXT,
            event_type    TEXT NOT NULL,
            occurred_at   TEXT NOT NULL,
            page          TEXT,
            session_id    TEXT
        )
    """)
    wh.insert_many("events", [
        {
            "event_id":    f"EVT-{i}",
            "customer_id": f"CUS-{random.randint(1,300):04d}" if random.random() > 0.1 else None,
            "event_type":  random.choice(["page_view", "add_to_cart", "checkout_start", "purchase"]),
            "occurred_at": (datetime(2024, 1, 1) + timedelta(hours=random.randint(0, 8760))).isoformat(),
            "page":        random.choice(["/home", "/products", "/checkout", "/account"]),
            "session_id":  f"SES-{random.randint(1, 5000)}",
        }
        for i in range(10000)
    ])
    wh.registry.register_table(TableMeta(
        table_name="events",
        description="Raw clickstream and behavioural events from the web app. Very large table — prefer events_daily_summary for aggregated analysis. Contains anonymous events.",
        owner="product-analytics@acme.com",
        domain="product",
        source_system="Segment",
        update_frequency="realtime",
        tags=["events", "clickstream", "raw", "behaviour", "large"],
        columns=[
            ColumnMeta("event_id",    "TEXT", "Unique event ID",           is_nullable=False),
            ColumnMeta("customer_id", "TEXT", "Nullable for anon users",   is_nullable=True),
            ColumnMeta("event_type",  "TEXT", "page_view/add_to_cart/etc", is_nullable=False, sample_values=["page_view","purchase"]),
            ColumnMeta("occurred_at", "TEXT", "ISO-8601 event timestamp",  is_nullable=False),
            ColumnMeta("page",        "TEXT", "URL path",                  sample_values=["/home","/checkout"]),
            ColumnMeta("session_id",  "TEXT", "Browser session identifier"),
        ],
    ))

    # --- products ---
    wh.create_table("""
        CREATE TABLE IF NOT EXISTS products (
            sku           TEXT NOT NULL,
            name          TEXT NOT NULL,
            category      TEXT NOT NULL,
            unit_cost     REAL NOT NULL,
            list_price    REAL NOT NULL,
            is_active     INTEGER NOT NULL
        )
    """)
    wh.insert_many("products", [
        {"sku": "PRD-A", "name": "Widget Alpha",   "category": "Widgets",    "unit_cost": 5.00,  "list_price": 49.99,  "is_active": 1},
        {"sku": "PRD-B", "name": "Widget Beta",    "category": "Widgets",    "unit_cost": 8.50,  "list_price": 79.99,  "is_active": 1},
        {"sku": "PRD-C", "name": "Gadget Gamma",   "category": "Gadgets",    "unit_cost": 22.00, "list_price": 149.99, "is_active": 1},
        {"sku": "PRD-D", "name": "Service Delta",  "category": "Services",   "unit_cost": 0.00,  "list_price": 299.00, "is_active": 1},
        {"sku": "PRD-E", "name": "Legacy Epsilon", "category": "Deprecated", "unit_cost": 15.00, "list_price": 99.99,  "is_active": 0},
    ])
    wh.registry.register_table(TableMeta(
        table_name="products",
        description="Product catalogue dimension. Contains SKUs, pricing, and categories. Small/static table — safe to join freely.",
        owner="data-engineering@acme.com",
        domain="product",
        source_system="ERP",
        update_frequency="weekly",
        tags=["products", "catalogue", "dimension", "pricing"],
        columns=[
            ColumnMeta("sku",        "TEXT", "Product SKU (PK)",             is_nullable=False),
            ColumnMeta("name",       "TEXT", "Human-readable product name",  is_nullable=False),
            ColumnMeta("category",   "TEXT", "Product category",             sample_values=["Widgets","Gadgets","Services"]),
            ColumnMeta("unit_cost",  "REAL", "Internal COGS per unit (USD)", is_nullable=False),
            ColumnMeta("list_price", "REAL", "List price (USD)",             is_nullable=False),
            ColumnMeta("is_active",  "INTEGER", "1=active, 0=discontinued",  is_nullable=False),
        ],
    ))

    # --- revenue_by_product (deprecated) ---
    wh.create_table("""
        CREATE TABLE IF NOT EXISTS revenue_by_product_old (
            product_sku TEXT,
            total_revenue REAL
        )
    """)
    wh.registry.register_table(TableMeta(
        table_name="revenue_by_product_old",
        description="Legacy product revenue rollup. Replaced by monthly_revenue_summary joined to products.",
        owner="legacy@acme.com",
        domain="finance",
        source_system="legacy-ETL",
        update_frequency="never",
        tags=["deprecated", "legacy", "revenue"],
        columns=[
            ColumnMeta("product_sku",   "TEXT", "Product SKU"),
            ColumnMeta("total_revenue", "REAL", "Total revenue"),
        ],
    ))
    wh.registry.deprecate_table(
        "revenue_by_product_old",
        "Replaced by monthly_revenue_summary. Will be dropped 2025-Q3.",
    )


# ------------------------------------------------------------------ #
#  Lineage                                                             #
# ------------------------------------------------------------------ #

def _seed_lineage(wh: SelfDescribingWarehouse) -> None:
    wh.lineage.add_edge("orders",    "monthly_revenue_summary",
                        "dbt model: aggregates orders by month+region, filters completed status")
    wh.lineage.add_edge("customers", "monthly_revenue_summary",
                        "joined to enrich region from customer.country when region is null")
    wh.lineage.add_edge("orders",    "revenue_by_product_old",
                        "legacy ETL: sum(revenue) group by product_sku")
    wh.lineage.add_edge("events",    "orders",
                        "purchase events are mirrored into orders via Kafka pipeline")
    wh.lineage.add_edge("products",  "monthly_revenue_summary",
                        "joined to bring in product category labels")


# ------------------------------------------------------------------ #
#  Quality                                                             #
# ------------------------------------------------------------------ #

def _seed_quality(wh: SelfDescribingWarehouse) -> None:
    tables = ["orders", "monthly_revenue_summary", "customers", "events", "products"]
    for table in tables:
        for _ in range(3):
            wh.quality.run(table)


# ------------------------------------------------------------------ #
#  Freshness                                                           #
# ------------------------------------------------------------------ #

def _seed_freshness(wh: SelfDescribingWarehouse) -> None:
    now = datetime.now(timezone.utc)
    freshness_config = {
        "orders":                   (now - timedelta(minutes=30),  1),
        "monthly_revenue_summary":  (now - timedelta(hours=3),    24),
        "customers":                (now - timedelta(hours=18),   24),
        "events":                   (now - timedelta(minutes=5),   1),
        "products":                 (now - timedelta(days=3),     168),  # weekly
    }
    for table, (last_updated, expected_hours) in freshness_config.items():
        wh.freshness.record(table, last_updated.isoformat(), expected_hours)


# ------------------------------------------------------------------ #
#  Usage                                                               #
# ------------------------------------------------------------------ #

def _seed_usage(wh: SelfDescribingWarehouse) -> None:
    users = ["alice@acme.com", "bob@acme.com", "carol@acme.com", "dave@acme.com", "eve@acme.com"]
    usage_weights = {
        "monthly_revenue_summary": 40,
        "orders": 25,
        "customers": 15,
        "events": 10,
        "products": 10,
    }
    sample_queries = {
        "monthly_revenue_summary": "SELECT month, SUM(total_revenue) FROM monthly_revenue_summary WHERE region='NA' GROUP BY month",
        "orders":                  "SELECT * FROM orders WHERE order_date >= '2024-01-01' AND status='completed'",
        "customers":               "SELECT segment, COUNT(*) FROM customers GROUP BY segment",
        "events":                  "SELECT event_type, COUNT(*) FROM events GROUP BY event_type",
        "products":                "SELECT * FROM products WHERE is_active=1",
    }
    for table, weight in usage_weights.items():
        for _ in range(weight):
            wh.usage.record(
                table,
                queried_by=random.choice(users),
                query=sample_queries.get(table, ""),
                execution_ms=random.randint(10, 3000),
            )


# ------------------------------------------------------------------ #
#  Incidents                                                           #
# ------------------------------------------------------------------ #

def _seed_incidents(wh: SelfDescribingWarehouse) -> None:
    # resolved old incident on events
    inc_id = wh.incidents.open(
        "events",
        "Missing 6-hour window of events due to Segment outage (2024-08-14 02:00-08:00 UTC)",
        severity="high",
    )
    wh.incidents.resolve(
        inc_id,
        root_cause="Segment API rate limit hit during peak traffic; replay completed from S3 backup",
        resolved_by="platform-oncall@acme.com",
    )

    # open low-severity incident on products
    wh.incidents.open(
        "products",
        "PRD-E list_price not updated after price change; off by $10",
        severity="low",
    )

    # resolved incident on orders
    inc2 = wh.incidents.open(
        "orders",
        "Duplicate orders ingested for region=LATAM during Kafka consumer restart (2024-11-02)",
        severity="medium",
    )
    wh.incidents.resolve(
        inc2,
        root_cause="Missing idempotency key in Kafka consumer; dedup job run manually",
        resolved_by="data-engineering@acme.com",
    )


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="warehouse.db")
    args = parser.parse_args()

    print(f"Seeding demo warehouse → {args.db}")
    wh = SelfDescribingWarehouse(db_path=args.db)
    seed(wh)
    wh.close()
    print("Done. Run: python -m warehouse.cli.main --db warehouse.db catalog")
