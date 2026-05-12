"""
Seed the catalog with a realistic demo:

  raw layer       → staging layer → reporting layer
  ─────────────────────────────────────────────────
  raw.users       →  stg.users   →  rpt.user_summary
  raw.orders      →  stg.orders  ↗
  raw.products    →  stg.products (standalone)

Each layer has a SQLite database. After seeding the data,
the script registers the source in the catalog, triggers a scan,
and registers lineage jobs with real SQL so column-level lineage
is auto-extracted by sqlglot.

Usage:
  python demo/seed.py
"""
import sys
import os
import sqlite3
import random
import string
from pathlib import Path

# Make sure the project root is on the path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

import requests

BASE = "http://localhost:8000"

# ── Helper ─────────────────────────────────────────────────────────────────

def post(path, body):
    r = requests.post(f"{BASE}{path}", json=body, timeout=30)
    if r.status_code not in (200, 201):
        print(f"  ✗ POST {path}: {r.status_code} {r.text[:200]}")
        return None
    return r.json()


def get(path):
    r = requests.get(f"{BASE}{path}", timeout=10)
    return r.json() if r.ok else None


# ── Create demo SQLite databases ───────────────────────────────────────────

def rand_str(n=8):
    return ''.join(random.choices(string.ascii_lowercase, k=n))


def create_raw_db(path: str):
    con = sqlite3.connect(path)
    cur = con.cursor()

    cur.execute("DROP TABLE IF EXISTS users")
    cur.execute("""
        CREATE TABLE users (
            user_id     INTEGER PRIMARY KEY,
            first_name  TEXT NOT NULL,
            last_name   TEXT NOT NULL,
            email       TEXT NOT NULL,
            phone       TEXT,
            date_of_birth TEXT,
            address     TEXT,
            zip_code    TEXT,
            created_at  TEXT
        )
    """)

    cur.execute("DROP TABLE IF EXISTS orders")
    cur.execute("""
        CREATE TABLE orders (
            order_id    INTEGER PRIMARY KEY,
            user_id     INTEGER NOT NULL,
            product_id  INTEGER NOT NULL,
            quantity    INTEGER,
            unit_price  REAL,
            total_amount REAL,
            order_date  TEXT,
            status      TEXT
        )
    """)

    cur.execute("DROP TABLE IF EXISTS products")
    cur.execute("""
        CREATE TABLE products (
            product_id  INTEGER PRIMARY KEY,
            product_name TEXT NOT NULL,
            category    TEXT,
            unit_price  REAL,
            sku         TEXT
        )
    """)

    # Seed data
    names = [("Alice","Smith"),("Bob","Jones"),("Carol","White"),("Dave","Brown"),("Eve","Davis")]
    for i, (fn, ln) in enumerate(names, 1):
        cur.execute("INSERT INTO users VALUES (?,?,?,?,?,?,?,?,?)", (
            i, fn, ln, f"{fn.lower()}@example.com",
            f"555-{random.randint(1000,9999)}",
            f"199{random.randint(0,9)}-0{random.randint(1,9)}-{random.randint(10,28)}",
            f"{random.randint(10,999)} Main St",
            f"{random.randint(10000,99999)}",
            "2024-01-01",
        ))

    products = [("Widget A","Electronics",9.99,"WA001"),
                ("Gadget B","Electronics",49.99,"GB002"),
                ("Thingamajig C","Misc",4.99,"TC003")]
    for i, (name, cat, price, sku) in enumerate(products, 1):
        cur.execute("INSERT INTO products VALUES (?,?,?,?,?)", (i, name, cat, price, sku))

    for i in range(1, 11):
        uid = random.randint(1, 5)
        pid = random.randint(1, 3)
        qty = random.randint(1, 5)
        price = random.choice([9.99, 49.99, 4.99])
        cur.execute("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)", (
            i, uid, pid, qty, price, round(qty * price, 2),
            "2024-06-01", random.choice(["completed","pending","shipped"])
        ))

    con.commit()
    con.close()
    print(f"  ✓ Raw DB created at {path}")


def create_staging_db(path: str):
    con = sqlite3.connect(path)
    cur = con.cursor()

    cur.execute("DROP TABLE IF EXISTS stg_users")
    cur.execute("""
        CREATE TABLE stg_users (
            user_id     INTEGER PRIMARY KEY,
            full_name   TEXT,
            email       TEXT,
            phone       TEXT,
            created_at  TEXT
        )
    """)

    cur.execute("DROP TABLE IF EXISTS stg_orders")
    cur.execute("""
        CREATE TABLE stg_orders (
            order_id     INTEGER PRIMARY KEY,
            user_id      INTEGER,
            total_amount REAL,
            order_date   TEXT,
            status       TEXT
        )
    """)

    cur.execute("DROP TABLE IF EXISTS stg_products")
    cur.execute("""
        CREATE TABLE stg_products (
            product_id   INTEGER PRIMARY KEY,
            product_name TEXT,
            category     TEXT,
            unit_price   REAL
        )
    """)

    con.commit()
    con.close()
    print(f"  ✓ Staging DB created at {path}")


def create_reporting_db(path: str):
    con = sqlite3.connect(path)
    cur = con.cursor()

    cur.execute("DROP TABLE IF EXISTS user_summary")
    cur.execute("""
        CREATE TABLE user_summary (
            user_id       INTEGER PRIMARY KEY,
            full_name     TEXT,
            email         TEXT,
            total_orders  INTEGER,
            total_spent   REAL,
            last_order_at TEXT
        )
    """)

    con.commit()
    con.close()
    print(f"  ✓ Reporting DB created at {path}")


# ── Register sources & scan ────────────────────────────────────────────────

def register_and_scan(name, engine, conn_str, description):
    # Delete existing source with same name
    existing = get("/api/sources")
    for s in (existing or []):
        if s["name"] == name:
            requests.delete(f"{BASE}/api/sources/{s['id']}", timeout=10)

    src = post("/api/sources", {
        "name": name,
        "engine_type": engine,
        "connection_string": conn_str,
        "description": description,
    })
    if not src:
        return None
    print(f"  ✓ Registered source: {name} (id={src['id']})")

    result = post(f"/api/sources/{src['id']}/scan", {})
    if result:
        print(f"    → Scanned: {result['tables']} tables, {result['columns']} columns, {result['pii_columns']} PII cols")
    return src


# ── Lineage jobs ───────────────────────────────────────────────────────────

LINEAGE_JOBS = [
    {
        "name": "raw_to_stg_users",
        "description": "Combine first/last name and clean fields into staging users",
        "job_type": "sql",
        "dialect": "sqlite",
        "sql_query": """
INSERT INTO stg_users (user_id, full_name, email, phone, created_at)
SELECT
    user_id,
    first_name || ' ' || last_name AS full_name,
    email,
    phone,
    created_at
FROM users
""",
        "tags": ["etl", "raw-to-staging"],
    },
    {
        "name": "raw_to_stg_orders",
        "description": "Select order fields into staging",
        "job_type": "sql",
        "dialect": "sqlite",
        "sql_query": """
INSERT INTO stg_orders (order_id, user_id, total_amount, order_date, status)
SELECT
    order_id,
    user_id,
    total_amount,
    order_date,
    status
FROM orders
""",
        "tags": ["etl", "raw-to-staging"],
    },
    {
        "name": "raw_to_stg_products",
        "description": "Load products into staging layer",
        "job_type": "sql",
        "dialect": "sqlite",
        "sql_query": """
INSERT INTO stg_products (product_id, product_name, category, unit_price)
SELECT product_id, product_name, category, unit_price
FROM products
""",
        "tags": ["etl"],
    },
    {
        "name": "stg_to_rpt_user_summary",
        "description": "Aggregate orders per user into reporting summary",
        "job_type": "sql",
        "dialect": "sqlite",
        "sql_query": """
INSERT INTO user_summary (user_id, full_name, email, total_orders, total_spent, last_order_at)
SELECT
    u.user_id,
    u.full_name,
    u.email,
    COUNT(o.order_id)   AS total_orders,
    SUM(o.total_amount) AS total_spent,
    MAX(o.order_date)   AS last_order_at
FROM stg_users u
LEFT JOIN stg_orders o ON u.user_id = o.user_id
GROUP BY u.user_id, u.full_name, u.email
""",
        "tags": ["aggregation", "reporting"],
    },
]


def register_jobs():
    for job in LINEAGE_JOBS:
        # Delete existing job with same name
        existing = get("/api/jobs")
        for j in (existing or []):
            if j["name"] == job["name"]:
                requests.delete(f"{BASE}/api/jobs/{j['id']}", timeout=10)

        result = post("/api/jobs", job)
        if result:
            print(f"  ✓ Registered lineage job: {job['name']}")


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    print("\n🌱  Seeding demo data...\n")

    # Create DBs in project root
    raw_path     = str(ROOT / "demo_raw.db")
    staging_path = str(ROOT / "demo_staging.db")
    rpt_path     = str(ROOT / "demo_reporting.db")

    print("Creating SQLite databases:")
    create_raw_db(raw_path)
    create_staging_db(staging_path)
    create_reporting_db(rpt_path)

    print("\nRegistering & scanning sources:")
    register_and_scan("raw_layer",       "sqlite", f"sqlite:///{raw_path}",     "Raw ingestion layer")
    register_and_scan("staging_layer",   "sqlite", f"sqlite:///{staging_path}", "Cleaned staging layer")
    register_and_scan("reporting_layer", "sqlite", f"sqlite:///{rpt_path}",     "Business reporting layer")

    print("\nRegistering lineage jobs:")
    register_jobs()

    print("\n✅  Demo seeded! Open http://localhost:8000\n")


if __name__ == "__main__":
    main()
