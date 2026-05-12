#!/usr/bin/env python3
"""
Seeds both regions with sample accounts for demo purposes.

Usage:
    python scripts/seed_data.py
"""
import asyncio
import httpx
import random

REGION_A = "http://localhost:8001"
REGION_B = "http://localhost:8002"

OWNERS = [
    "Acme Corp", "Globex Inc", "Initech Ltd", "Umbrella Corp",
    "Wayne Enterprises", "Stark Industries", "Oscorp", "Cyberdyne Systems",
]

CURRENCIES = ["USD", "EUR", "GBP", "SGD", "JPY"]


async def seed(base_url: str, region_label: str, n: int = 6):
    async with httpx.AsyncClient(timeout=10) as client:
        print(f"\nSeeding {n} accounts on {region_label} ({base_url}) …")
        for i in range(n):
            payload = {
                "owner": random.choice(OWNERS),
                "balance": round(random.uniform(500, 50000), 2),
                "currency": random.choice(CURRENCIES),
                "tags": random.sample(["vip", "gold", "enterprise", "retail", "new"], 2),
                "metadata": {"source": region_label, "tier": random.choice(["standard", "premium"])},
            }
            r = await client.post(f"{base_url}/accounts", json=payload)
            if r.status_code == 201:
                a = r.json()
                print(f"  [{region_label}] {a['account_id'][:8]}… {a['owner']:<22} {a['currency']} {a['balance']:>10,.2f}")
            else:
                print(f"  ERROR {r.status_code}: {r.text}")


async def main():
    await asyncio.gather(
        seed(REGION_A, "region-a", n=6),
        seed(REGION_B, "region-b", n=6),
    )
    print("\nWaiting 5 s for cross-region replication …")
    await asyncio.sleep(5)
    print("Done — open the dashboards:")
    print(f"  {REGION_A}/dashboard")
    print(f"  {REGION_B}/dashboard\n")


if __name__ == "__main__":
    asyncio.run(main())
