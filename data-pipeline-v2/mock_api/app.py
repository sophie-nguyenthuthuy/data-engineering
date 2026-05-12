"""Deterministic mock API with pagination and updated_at watermarks.

Exposes /customers, /products, /orders. Seeded once at boot so responses are
stable across restarts; every call to /orders/tick advances a handful of rows
so the Airflow DAG has something fresh to pick up incrementally.
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Optional

from faker import Faker
from fastapi import FastAPI, HTTPException, Query

SEED = 42
N_CUSTOMERS = 500
N_PRODUCTS = 75
N_ORDERS = 5_000

fake = Faker()
Faker.seed(SEED)
random.seed(SEED)

app = FastAPI(title="mock-api", version="2.0")
_lock = Lock()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _seed():
    start = _now() - timedelta(days=90)
    customers = [
        {
            "id": i,
            "email": fake.unique.email(),
            "full_name": fake.name(),
            "country": fake.country_code(),
            "created_at": (start + timedelta(minutes=i)).isoformat(),
            "updated_at": (start + timedelta(minutes=i)).isoformat(),
        }
        for i in range(1, N_CUSTOMERS + 1)
    ]
    categories = ["books", "electronics", "apparel", "home", "grocery"]
    products = [
        {
            "id": i,
            "sku": f"SKU-{i:05d}",
            "name": fake.unique.catch_phrase(),
            "category": random.choice(categories),
            "price_cents": random.randint(200, 50_000),
            "updated_at": start.isoformat(),
        }
        for i in range(1, N_PRODUCTS + 1)
    ]
    statuses = ["pending", "paid", "shipped", "delivered", "cancelled"]
    orders = []
    for i in range(1, N_ORDERS + 1):
        p = products[random.randint(0, N_PRODUCTS - 1)]
        qty = random.randint(1, 5)
        ts = start + timedelta(minutes=i * 10 + random.randint(0, 9))
        orders.append({
            "id": i,
            "customer_id": random.randint(1, N_CUSTOMERS),
            "product_id": p["id"],
            "quantity": qty,
            "amount_cents": p["price_cents"] * qty,
            "status": random.choice(statuses),
            "ordered_at": ts.isoformat(),
            "updated_at": ts.isoformat(),
        })
    return customers, products, orders


CUSTOMERS, PRODUCTS, ORDERS = _seed()


def _paginate(items, limit, offset, updated_since):
    if updated_since:
        items = [r for r in items if r["updated_at"] >= updated_since]
    items = sorted(items, key=lambda r: (r["updated_at"], r["id"]))
    total = len(items)
    sliced = items[offset : offset + limit]
    return {"total": total, "limit": limit, "offset": offset, "items": sliced}


@app.get("/healthz")
def healthz():
    return {"ok": True, "ts": _now().isoformat()}


@app.get("/customers")
def list_customers(
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    updated_since: Optional[str] = None,
):
    return _paginate(CUSTOMERS, limit, offset, updated_since)


@app.get("/products")
def list_products(
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    updated_since: Optional[str] = None,
):
    return _paginate(PRODUCTS, limit, offset, updated_since)


@app.get("/orders")
def list_orders(
    limit: int = Query(500, ge=1, le=2000),
    offset: int = Query(0, ge=0),
    updated_since: Optional[str] = None,
):
    return _paginate(ORDERS, limit, offset, updated_since)


@app.post("/orders/tick")
def tick(n: int = Query(25, ge=1, le=500)):
    """Advance updated_at / status on n random rows so the next DAG run has work."""
    with _lock:
        now_iso = _now().isoformat()
        touched = 0
        for _ in range(n):
            o = ORDERS[random.randint(0, N_ORDERS - 1)]
            o["status"] = random.choice(["paid", "shipped", "delivered"])
            o["updated_at"] = now_iso
            touched += 1
        return {"touched": touched, "at": now_iso}


@app.get("/orders/{order_id}")
def get_order(order_id: int):
    for o in ORDERS:
        if o["id"] == order_id:
            return o
    raise HTTPException(status_code=404, detail="not found")
