"""Synthetic dataset generator for benchmark tables."""

from __future__ import annotations

import random
import time
from datetime import datetime, timezone

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from cow_mor_bench.data.schemas import (
    EVENTS_SCHEMA,
    INVENTORY_SCHEMA,
    ORDERS_SCHEMA,
    SCHEMA_REGISTRY,
)

_RNG = np.random.default_rng(42)

STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
REGIONS = ["us-east", "us-west", "eu-central", "ap-southeast", "sa-east"]
EVENT_TYPES = ["page_view", "click", "search", "add_to_cart", "checkout", "purchase"]
PAGES = ["/home", "/products", "/cart", "/checkout", "/account", "/search"]


def _ts_now_us() -> int:
    return int(time.time() * 1_000_000)


def generate_orders(
    n_rows: int,
    start_id: int = 1,
    seed: int | None = None,
) -> pa.Table:
    rng = np.random.default_rng(seed or 42)
    now_us = _ts_now_us()
    unit_prices = rng.uniform(1.0, 500.0, n_rows).round(2)
    quantities = rng.integers(1, 50, n_rows)
    created_offsets = rng.integers(0, 86_400_000_000, n_rows)  # up to 1 day ago

    return pa.table({
        "order_id": pa.array(range(start_id, start_id + n_rows), type=pa.int64()),
        "customer_id": pa.array(rng.integers(1, 100_000, n_rows), type=pa.int64()),
        "product_id": pa.array(rng.integers(1, 10_000, n_rows), type=pa.int64()),
        "quantity": pa.array(quantities.astype(np.int32), type=pa.int32()),
        "unit_price": pa.array(unit_prices, type=pa.float64()),
        "total_amount": pa.array((unit_prices * quantities).round(2), type=pa.float64()),
        "status": pa.array(rng.choice(STATUSES, n_rows).tolist(), type=pa.string()),
        "region": pa.array(rng.choice(REGIONS, n_rows).tolist(), type=pa.string()),
        "created_at": pa.array(now_us - created_offsets, type=pa.timestamp("us")),
        "updated_at": pa.array([now_us] * n_rows, type=pa.timestamp("us")),
    })


def generate_events(
    n_rows: int,
    start_id: int = 1,
    seed: int | None = None,
) -> pa.Table:
    rng = np.random.default_rng(seed or 42)
    now_us = _ts_now_us()
    offsets = rng.integers(0, 3_600_000_000, n_rows)

    return pa.table({
        "event_id": pa.array(range(start_id, start_id + n_rows), type=pa.int64()),
        "user_id": pa.array(rng.integers(1, 1_000_000, n_rows), type=pa.int64()),
        "session_id": pa.array(
            [f"sess-{rng.integers(1, 999_999):06d}" for _ in range(n_rows)],
            type=pa.string(),
        ),
        "event_type": pa.array(rng.choice(EVENT_TYPES, n_rows).tolist(), type=pa.string()),
        "page": pa.array(rng.choice(PAGES, n_rows).tolist(), type=pa.string()),
        "duration_ms": pa.array(rng.integers(10, 30_000, n_rows), type=pa.int64()),
        "timestamp": pa.array(now_us - offsets, type=pa.timestamp("us")),
    })


def generate_inventory(
    n_products: int,
    n_warehouses: int = 5,
    seed: int | None = None,
) -> pa.Table:
    rng = np.random.default_rng(seed or 42)
    now_us = _ts_now_us()
    n_rows = n_products * n_warehouses
    product_ids = np.repeat(np.arange(1, n_products + 1), n_warehouses)
    warehouse_ids = np.tile(np.arange(1, n_warehouses + 1), n_products)

    return pa.table({
        "product_id": pa.array(product_ids, type=pa.int64()),
        "warehouse_id": pa.array(warehouse_ids.astype(np.int32), type=pa.int32()),
        "stock_qty": pa.array(rng.integers(0, 10_000, n_rows), type=pa.int64()),
        "reserved_qty": pa.array(rng.integers(0, 1_000, n_rows), type=pa.int64()),
        "reorder_point": pa.array(rng.integers(50, 500, n_rows), type=pa.int64()),
        "last_updated": pa.array([now_us] * n_rows, type=pa.timestamp("us")),
    })


def generate_table(schema_name: str, n_rows: int, start_id: int = 1, seed: int = 42) -> pa.Table:
    if schema_name == "orders":
        return generate_orders(n_rows, start_id, seed)
    if schema_name == "events":
        return generate_events(n_rows, start_id, seed)
    if schema_name == "inventory":
        return generate_inventory(n_rows, seed=seed)
    raise ValueError(f"Unknown schema: {schema_name}")


def generate_update_batch(
    table: pa.Table,
    update_fraction: float,
    schema_name: str,
    seed: int = 0,
) -> pa.Table:
    """Return a subset of rows with mutated fields (simulates UPDATE workload)."""
    rng = np.random.default_rng(seed)
    n = max(1, int(len(table) * update_fraction))
    indices = rng.choice(len(table), size=n, replace=False)
    subset = table.take(indices)

    now_us = _ts_now_us()

    if schema_name == "orders":
        new_statuses = rng.choice(STATUSES, len(subset)).tolist()
        updated = subset.set_column(
            subset.schema.get_field_index("status"),
            "status",
            pa.array(new_statuses, type=pa.string()),
        )
        updated = updated.set_column(
            updated.schema.get_field_index("updated_at"),
            "updated_at",
            pa.array([now_us] * len(updated), type=pa.timestamp("us")),
        )
        return updated

    if schema_name == "inventory":
        new_stock = rng.integers(0, 10_000, len(subset)).astype(np.int64)
        updated = subset.set_column(
            subset.schema.get_field_index("stock_qty"),
            "stock_qty",
            pa.array(new_stock, type=pa.int64()),
        )
        updated = updated.set_column(
            updated.schema.get_field_index("last_updated"),
            "last_updated",
            pa.array([now_us] * len(updated), type=pa.timestamp("us")),
        )
        return updated

    return subset


def primary_key_for(schema_name: str) -> str:
    return {"orders": "order_id", "events": "event_id", "inventory": "product_id"}[schema_name]
