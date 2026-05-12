#!/usr/bin/env python3
"""
Conflict simulation script.

Writes to BOTH regions simultaneously to produce genuine concurrent conflicts,
then waits for replication to converge and prints the results.

Usage:
    python scripts/simulate_conflict.py [--strategy lww|crdt|business]

Requires both Docker Compose nodes to be running:
    docker compose up --build -d
"""
import argparse
import asyncio
import time
import httpx

REGION_A = "http://localhost:8001"
REGION_B = "http://localhost:8002"


async def run(strategy: str):
    async with httpx.AsyncClient(timeout=10) as client:
        print(f"\n{'='*60}")
        print(f"  Multi-Region Conflict Simulation  —  strategy: {strategy}")
        print(f"{'='*60}\n")

        # ── 1. Create an account on region-a ─────────────────────────
        print("► Creating account on region-a …")
        r = await client.post(f"{REGION_A}/accounts", json={
            "owner": "Global Corp",
            "balance": 1000.0,
            "currency": "USD",
            "tags": ["enterprise"],
            "metadata": {"tier": "gold"},
        })
        r.raise_for_status()
        acc = r.json()
        acc_id = acc["account_id"]
        print(f"  Created: {acc_id}  balance={acc['balance']}")

        # ── 2. Wait for replication to region-b ─────────────────────
        print("\n► Waiting 5 s for replication to region-b …")
        await asyncio.sleep(5)

        # Confirm region-b has it
        r = await client.get(f"{REGION_B}/accounts/{acc_id}")
        if r.status_code == 200:
            print(f"  region-b confirmed:  balance={r.json()['balance']}")
        else:
            print("  WARNING: region-b doesn't have it yet — increase wait time")

        # ── 3. Produce a CONCURRENT conflict ─────────────────────────
        print("\n► Sending CONCURRENT writes to both regions (no sleep between them) …")
        t0 = time.time()
        r_a, r_b = await asyncio.gather(
            client.patch(f"{REGION_A}/accounts/{acc_id}/balance",
                         json={"delta": +500.0, "note": "region-a deposit"}),
            client.patch(f"{REGION_B}/accounts/{acc_id}/balance",
                         json={"delta": -200.0, "note": "region-b withdrawal"}),
        )
        r_a.raise_for_status()
        r_b.raise_for_status()
        print(f"  region-a balance after write: {r_a.json()['balance']}")
        print(f"  region-b balance after write: {r_b.json()['balance']}")
        print(f"  Both writes completed in {(time.time()-t0)*1000:.0f} ms")

        # ── 4. Also conflict on tags ──────────────────────────────────
        print("\n► Concurrent tag writes …")
        await asyncio.gather(
            client.put(f"{REGION_A}/accounts/{acc_id}/tags",
                       json=["enterprise", "vip"]),
            client.put(f"{REGION_B}/accounts/{acc_id}/tags",
                       json=["enterprise", "platinum"]),
        )

        # ── 5. Wait for convergence ───────────────────────────────────
        print("\n► Waiting 8 s for convergence …")
        await asyncio.sleep(8)

        # ── 6. Final state on both regions ───────────────────────────
        r_a = await client.get(f"{REGION_A}/accounts/{acc_id}")
        r_b = await client.get(f"{REGION_B}/accounts/{acc_id}")
        fa = r_a.json()
        fb = r_b.json()

        print(f"\n{'─'*60}")
        print("  CONVERGED STATE")
        print(f"{'─'*60}")
        print(f"  region-a  balance={fa['balance']:<10}  tags={fa['tags']}")
        print(f"  region-b  balance={fb['balance']:<10}  tags={fb['tags']}")
        print()

        if fa["balance"] == fb["balance"]:
            print("  ✅ Balances CONVERGED")
        else:
            print("  ⏳ Balances still diverging — try waiting longer")

        if set(fa["tags"]) == set(fb["tags"]):
            print("  ✅ Tags CONVERGED")
        else:
            print("  ⏳ Tags still diverging")

        # ── 7. Health snapshot ────────────────────────────────────────
        h_a = (await client.get(f"{REGION_A}/health")).json()
        h_b = (await client.get(f"{REGION_B}/health")).json()
        print(f"\n  region-a: conflicts_resolved={h_a['conflicts_resolved']}  "
              f"lag={h_a['max_lag_seconds']} s  status={h_a['status']}")
        print(f"  region-b: conflicts_resolved={h_b['conflicts_resolved']}  "
              f"lag={h_b['max_lag_seconds']} s  status={h_b['status']}")
        print(f"\n  Dashboard:  {REGION_A}/dashboard  (region-a)")
        print(f"  Dashboard:  {REGION_B}/dashboard  (region-b)\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", default="lww",
                        choices=["lww", "crdt", "business"])
    args = parser.parse_args()
    asyncio.run(run(args.strategy))
