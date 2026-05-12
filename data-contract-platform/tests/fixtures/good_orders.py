"""Generate test fixture data as CSV."""

import csv
import random
from pathlib import Path

STATUSES = ["pending", "confirmed", "shipped", "delivered"]


def generate(path: Path, rows: int = 500) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "order_id", "customer_id", "order_date", "status",
                "total_amount", "item_count", "discount_pct",
            ],
        )
        writer.writeheader()
        for i in range(rows):
            writer.writerow({
                "order_id": f"ORD-{i:06d}",
                "customer_id": f"CUST-{random.randint(1, 1000):04d}",
                "order_date": "2026-05-01T10:00:00Z",
                "status": random.choice(STATUSES),
                "total_amount": round(random.uniform(10, 500), 2),
                "item_count": random.randint(1, 10),
                "discount_pct": round(random.uniform(0, 30), 1) if random.random() > 0.5 else "",
            })


if __name__ == "__main__":
    generate(Path(__file__).parent / "good_orders.csv")
    print("Generated good_orders.csv")
