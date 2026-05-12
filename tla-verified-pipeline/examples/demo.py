"""Demo: replay an event log; surface incidents.

Includes one "good" run and one "buggy" run where Debezium racing ahead
exceeds max_lag.
"""
from __future__ import annotations

from src import Monitor


def main():
    # Good run
    good = [
        {"action": "pg_insert",         "record": "alice_signup"},
        {"action": "debezium_publish",  "record": "alice_signup"},
        {"action": "flink_consume"},
        {"action": "warehouse_load",    "record": "alice_signup"},
        {"action": "reverse_etl",       "record": "alice_signup"},
    ]
    m = Monitor(max_lag=3)
    m.replay(good)
    print("=== Good run ===")
    print(f"  pg:        {m.state.pg}")
    print(f"  warehouse: {m.state.warehouse}")
    print(f"  rev_etl:   {m.state.rev_etl}")
    print(f"  incidents: {m.incidents}")

    # Bad run: Debezium publishes 5 records but Flink never consumes
    bad = []
    for i in range(5):
        rid = f"signup_{i}"
        bad.append({"action": "pg_insert", "record": rid})
        bad.append({"action": "debezium_publish", "record": rid})

    print("\n=== Bad run (kafka lag explodes) ===")
    m2 = Monitor(max_lag=2)
    m2.replay(bad)
    for inc in m2.incidents[:3]:
        print(f"  Incident @ step {inc.step}: {inc.violations}")
        print(f"    kafka has {len(inc.state_snapshot['kafka'])} pending")
    print(f"  ...({len(m2.incidents)} total incidents)")


if __name__ == "__main__":
    main()
