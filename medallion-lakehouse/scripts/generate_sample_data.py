"""Synthetic e-commerce dataset generator.

Emits four CSVs (customers, products, orders, order_items) sized so the
resulting gold star schema exercises joins + SCD2 history without being
absurdly large.

Usage:
    python scripts/generate_sample_data.py --rows 100000 --out sample_data
"""

from __future__ import annotations

import argparse
import csv
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path


CATEGORIES = ["apparel", "home", "electronics", "grocery", "other"]
STATUSES = ["placed", "shipped", "delivered", "returned", "cancelled"]
STATUS_WEIGHTS = [0.05, 0.15, 0.70, 0.05, 0.05]
COUNTRIES = ["US", "GB", "DE", "FR", "VN", "JP", "BR"]
CITIES = {
    "US": ["Seattle", "Austin", "NYC"],
    "GB": ["London", "Manchester"],
    "DE": ["Berlin", "Munich"],
    "FR": ["Paris", "Lyon"],
    "VN": ["Hanoi", "HCMC"],
    "JP": ["Tokyo", "Osaka"],
    "BR": ["São Paulo", "Rio"],
}


@dataclass(frozen=True)
class Args:
    rows: int
    out: Path
    seed: int


def parse_args() -> Args:
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=100_000, help="order lines to emit")
    p.add_argument("--out", type=Path, default=Path("sample_data"))
    p.add_argument("--seed", type=int, default=42)
    ns = p.parse_args()
    return Args(rows=ns.rows, out=ns.out, seed=ns.seed)


def write_csv(path: Path, header: list[str], rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def gen_customers(n: int, rng: random.Random):
    header = ["customer_id", "email", "address_line1", "city", "country", "created_at"]
    rows = []
    for i in range(n):
        country = rng.choice(COUNTRIES)
        rows.append([
            f"C{i:07d}",
            f"user{i}@example.com",
            f"{rng.randint(1, 9999)} Main St",
            rng.choice(CITIES[country]),
            country,
            (datetime(2020, 1, 1) + timedelta(days=rng.randint(0, 1800))).isoformat(),
        ])
    return header, rows


def gen_products(n: int, rng: random.Random):
    header = ["product_id", "unit_price", "category"]
    rows = []
    for i in range(n):
        rows.append([
            f"P{i:06d}",
            f"{rng.uniform(2.0, 499.99):.2f}",
            rng.choice(CATEGORIES),
        ])
    return header, rows


def gen_orders_and_items(
    line_count: int, n_customers: int, n_products: int, rng: random.Random
):
    orders_header = ["order_id", "customer_id", "order_date", "status"]
    items_header = ["order_id", "product_id", "quantity", "unit_price", "line_total"]
    orders, items = [], []

    start = date(2024, 1, 1)
    order_idx = 0
    remaining = line_count
    while remaining > 0:
        order_id = f"O{order_idx:09d}"
        order_idx += 1
        customer_id = f"C{rng.randint(0, n_customers - 1):07d}"
        order_date = (start + timedelta(days=rng.randint(0, 450))).isoformat()
        status = rng.choices(STATUSES, STATUS_WEIGHTS)[0]
        orders.append([order_id, customer_id, order_date, status])

        n_lines = min(rng.randint(1, 5), remaining)
        for _ in range(n_lines):
            product_id = f"P{rng.randint(0, n_products - 1):06d}"
            qty = rng.randint(1, 4)
            price = round(rng.uniform(2.0, 499.99), 2)
            items.append([order_id, product_id, qty, f"{price:.2f}", f"{qty * price:.2f}"])
        remaining -= n_lines

    return orders_header, orders, items_header, items


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)

    n_customers = max(100, args.rows // 20)
    n_products = max(50, args.rows // 100)

    c_h, c_rows = gen_customers(n_customers, rng)
    p_h, p_rows = gen_products(n_products, rng)
    o_h, o_rows, i_h, i_rows = gen_orders_and_items(args.rows, n_customers, n_products, rng)

    write_csv(args.out / "customers" / "customers.csv", c_h, c_rows)
    write_csv(args.out / "products" / "products.csv", p_h, p_rows)
    write_csv(args.out / "orders" / "orders.csv", o_h, o_rows)
    write_csv(args.out / "order_items" / "order_items.csv", i_h, i_rows)

    print(f"wrote {len(c_rows)} customers, {len(p_rows)} products, "
          f"{len(o_rows)} orders, {len(i_rows)} order_items to {args.out}")


if __name__ == "__main__":
    main()
