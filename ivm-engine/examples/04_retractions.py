"""Example 4 — Retractions: correcting previously emitted results.

This is the core hard part of IVM.  When an upstream fact changes, every
derived view that depended on it must be corrected with minimum recomputation.

Scenario: financial transaction stream where some transactions are later
found to be fraudulent and must be reversed.

Demonstrates:
  - Simple value correction (update = retract old + insert new).
  - Retraction propagating through a filter.
  - Retraction propagating through GROUP BY (aggregate correction).
  - Retraction propagating through a JOIN (enriched record disappears).
  - Delta log shows the retraction/assertion pairs.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ivm import IVMEngine
import ivm.aggregates as agg


def divider(msg: str) -> None:
    print(f"\n{'─' * 55}")
    print(f"  {msg}")
    print("─" * 55)


def main():
    engine = IVMEngine()
    txns     = engine.source("transactions")
    accounts = engine.source("accounts")

    # View 1: total spend per merchant (GROUP BY)
    merchant_totals = (
        txns
        .group_by(["merchant"], {"total": agg.Sum("amount"), "count": agg.Count()})
    )
    engine.register_view("merchant_totals", merchant_totals)

    # View 2: enriched transactions (JOIN with accounts)
    enriched = txns.join(accounts, left_key="account_id", right_key="account_id")
    engine.register_view("enriched_txns", enriched)

    # Load accounts
    for acc in [
        {"account_id": "A1", "owner": "Alice", "tier": "gold"},
        {"account_id": "A2", "owner": "Bob",   "tier": "silver"},
    ]:
        engine.ingest("accounts", acc, timestamp=0)

    # Initial transactions
    initial_txns = [
        {"txn_id": "T1", "account_id": "A1", "merchant": "Amazon",  "amount": 120},
        {"txn_id": "T2", "account_id": "A2", "merchant": "Netflix",  "amount": 15},
        {"txn_id": "T3", "account_id": "A1", "merchant": "Amazon",  "amount": 85},
        {"txn_id": "T4", "account_id": "A2", "merchant": "Spotify",  "amount": 10},
        {"txn_id": "T5", "account_id": "A1", "merchant": "Amazon",  "amount": 250},
    ]
    for t in initial_txns:
        engine.ingest("transactions", t, timestamp=1000)

    divider("Initial state")
    print("Merchant totals:")
    for row in sorted(engine.query("merchant_totals"), key=lambda r: r["merchant"]):
        print(f"  {row['merchant']:10s}  total=${row['total']}  count={row['count']}")

    print("\nEnriched transactions:")
    for row in sorted(engine.query("enriched_txns"), key=lambda r: r["txn_id"]):
        print(f"  {row['txn_id']}  {row['owner']:6s}  "
              f"{row['merchant']:10s}  ${row['amount']}")

    # ----------------------------------------------------------------
    # Fraud detection: T3 and T5 flagged as fraudulent — retract them
    # ----------------------------------------------------------------
    divider("Fraud reversal: retracting T3 and T5")

    fraudulent = [
        {"txn_id": "T3", "account_id": "A1", "merchant": "Amazon",  "amount": 85},
        {"txn_id": "T5", "account_id": "A1", "merchant": "Amazon",  "amount": 250},
    ]
    for t in fraudulent:
        engine.retract("transactions", t, timestamp=2000)

    print("Merchant totals after fraud reversal:")
    for row in sorted(engine.query("merchant_totals"), key=lambda r: r["merchant"]):
        print(f"  {row['merchant']:10s}  total=${row['total']}  count={row['count']}")

    print("\nEnriched transactions after fraud reversal:")
    for row in sorted(engine.query("enriched_txns"), key=lambda r: r["txn_id"]):
        print(f"  {row['txn_id']}  {row['owner']:6s}  "
              f"{row['merchant']:10s}  ${row['amount']}")

    # ----------------------------------------------------------------
    # Value correction: T2 was recorded with wrong amount — correct it
    # ----------------------------------------------------------------
    divider("Value correction: T2 amount was $15 → should be $150")

    # "Update" = retract old value, insert corrected value
    engine.retract("transactions",
                   {"txn_id": "T2", "account_id": "A2", "merchant": "Netflix", "amount": 15},
                   timestamp=3000)
    engine.ingest("transactions",
                  {"txn_id": "T2", "account_id": "A2", "merchant": "Netflix", "amount": 150},
                  timestamp=3000)

    print("Merchant totals after correction:")
    for row in sorted(engine.query("merchant_totals"), key=lambda r: r["merchant"]):
        print(f"  {row['merchant']:10s}  total=${row['total']}  count={row['count']}")

    # ----------------------------------------------------------------
    # Delta log analysis — only deltas, not full recomputes
    # ----------------------------------------------------------------
    divider("Delta log for merchant_totals (last 8 entries)")
    for delta in engine.recent_deltas("merchant_totals", n=8):
        sign = "INSERT" if delta.diff > 0 else "RETRACT"
        print(f"  [{sign}] {delta.record['merchant']:10s} "
              f"total={delta.record['total']}  count={delta.record['count']}")

    total_deltas = len(engine.delta_log("merchant_totals"))
    total_inputs = len(initial_txns) + len(fraudulent) + 2  # +2 for T2 correction
    print(f"\nTotal deltas emitted: {total_deltas}  "
          f"(for {total_inputs} input events — each input produces ≤2 deltas)")

    # ----------------------------------------------------------------
    # Verify correctness
    # ----------------------------------------------------------------
    divider("Correctness check")
    totals = {r["merchant"]: r["total"] for r in engine.query("merchant_totals")}
    assert totals["Amazon"]  == 120,  f"Expected 120, got {totals['Amazon']}"
    assert totals["Netflix"] == 150,  f"Expected 150, got {totals['Netflix']}"
    assert totals["Spotify"] == 10,   f"Expected 10, got {totals['Spotify']}"
    print("  All aggregate values correct after retractions and corrections ✓")


if __name__ == "__main__":
    main()
