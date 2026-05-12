#!/usr/bin/env python3
"""
Simulate realistic CDC workload on the source database.

Runs a mix of inserts, updates, and deletes across users, orders, and order_items.
Also introduces deliberate out-of-order scenarios by using concurrent connections.
"""

import argparse
import random
import time
import threading
import psycopg2
from datetime import datetime

DSN = "postgresql://cdc_source:cdc_secret@localhost:5432/transactional_db"
STATUSES = ["pending", "processing", "completed", "cancelled"]
SKUS = ["WIDGET-001", "GADGET-002", "PREMIUM-001", "BASIC-001", "DELUXE-003"]


def get_conn():
    return psycopg2.connect(DSN)


# ------------------------------------------------------------------
# Workload functions
# ------------------------------------------------------------------

def create_user(conn):
    ts = int(time.time())
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users (email, username) VALUES (%s, %s) RETURNING id",
            (f"user_{ts}_{random.randint(1000,9999)}@example.com", f"user_{ts}"),
        )
        user_id = cur.fetchone()[0]
    conn.commit()
    return user_id


def update_user_status(conn, user_id: int):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE users SET status = %s WHERE id = %s",
            (random.choice(["active", "inactive", "suspended"]), user_id),
        )
    conn.commit()


def create_order(conn, user_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO orders (user_id, status, total_amount) VALUES (%s, %s, %s) RETURNING id",
            (user_id, "pending", round(random.uniform(10, 500), 2)),
        )
        order_id = cur.fetchone()[0]
        # Add 1-3 items
        for _ in range(random.randint(1, 3)):
            qty = random.randint(1, 5)
            price = round(random.uniform(5, 100), 2)
            cur.execute(
                "INSERT INTO order_items (order_id, sku, quantity, unit_price) VALUES (%s, %s, %s, %s)",
                (order_id, random.choice(SKUS), qty, price),
            )
            cur.execute(
                "UPDATE orders SET total_amount = total_amount + %s WHERE id = %s",
                (qty * price, order_id),
            )
    conn.commit()
    return order_id


def advance_order_status(conn, order_id: int):
    with conn.cursor() as cur:
        cur.execute("SELECT status FROM orders WHERE id = %s", (order_id,))
        row = cur.fetchone()
        if not row:
            return
        current = row[0]
        transitions = {"pending": "processing", "processing": "completed"}
        next_status = transitions.get(current)
        if next_status:
            cur.execute("UPDATE orders SET status = %s WHERE id = %s", (next_status, order_id))
    conn.commit()


def delete_cancelled_orders(conn):
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM orders WHERE status = 'cancelled' AND updated_at < NOW() - INTERVAL '1 minute' RETURNING id"
        )
        deleted = cur.fetchall()
    conn.commit()
    if deleted:
        print(f"  [cleanup] Deleted {len(deleted)} cancelled orders")


# ------------------------------------------------------------------
# Out-of-order simulation: two concurrent transactions where the
# "later" logical operation commits first (interleaved WAL writes)
# ------------------------------------------------------------------

def simulate_out_of_order():
    """
    Open two connections, start both transactions, then commit in reverse order.
    This produces LSN gaps that test the ReorderBuffer.
    """
    conn1, conn2 = get_conn(), get_conn()
    conn1.autocommit = False
    conn2.autocommit = False
    try:
        with conn1.cursor() as c1, conn2.cursor() as c2:
            c1.execute("UPDATE users SET username = username WHERE id = 1")
            c2.execute("UPDATE users SET username = username WHERE id = 2")
            # conn2 commits first → its LSN arrives in Kafka before conn1's
            conn2.commit()
            time.sleep(0.05)
            conn1.commit()
        print("  [out-of-order] Committed tx2 before tx1 — ReorderBuffer will resequence")
    finally:
        conn1.close()
        conn2.close()


# ------------------------------------------------------------------
# Main loop
# ------------------------------------------------------------------

def run(duration_seconds: int, ops_per_second: float):
    print(f"Running simulation for {duration_seconds}s at ~{ops_per_second} ops/sec")
    conn = get_conn()

    # Collect existing IDs
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM users")
        user_ids = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT id FROM orders")
        order_ids = [r[0] for r in cur.fetchall()]

    deadline = time.monotonic() + duration_seconds
    op_count = 0

    while time.monotonic() < deadline:
        op = random.choices(
            ["create_user", "update_user", "create_order", "advance_order", "out_of_order", "cleanup"],
            weights=[10, 15, 20, 25, 10, 5],
        )[0]

        try:
            if op == "create_user":
                uid = create_user(conn)
                user_ids.append(uid)
                print(f"  [+] user {uid}")

            elif op == "update_user" and user_ids:
                uid = random.choice(user_ids)
                update_user_status(conn, uid)
                print(f"  [~] user {uid} status updated")

            elif op == "create_order" and user_ids:
                uid = random.choice(user_ids)
                oid = create_order(conn, uid)
                order_ids.append(oid)
                print(f"  [+] order {oid} for user {uid}")

            elif op == "advance_order" and order_ids:
                oid = random.choice(order_ids)
                advance_order_status(conn, oid)
                print(f"  [~] order {oid} status advanced")

            elif op == "out_of_order":
                simulate_out_of_order()

            elif op == "cleanup":
                delete_cancelled_orders(conn)

        except psycopg2.Error as exc:
            print(f"  [!] DB error: {exc}")
            conn = get_conn()

        op_count += 1
        time.sleep(1.0 / ops_per_second)

    conn.close()
    print(f"\nDone — {op_count} operations in {duration_seconds}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int,   default=120, help="Run duration in seconds")
    parser.add_argument("--rate",     type=float, default=2.0, help="Operations per second")
    args = parser.parse_args()
    run(args.duration, args.rate)
