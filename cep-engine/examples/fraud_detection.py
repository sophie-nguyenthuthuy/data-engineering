"""
Fraud detection demo — three-step sequence:
  1. Three consecutive LOGIN_FAILURE events for the same account
  2. A PASSWORD_RESET within 10 s
  3. A WITHDRAWAL >= $500 within 30 s of the failures

Run:
    python examples/fraud_detection.py
"""

import time

from cep import CEPEngine, Pattern, make_event


class E:
    LOGIN_SUCCESS = 1
    LOGIN_FAILURE = 2
    PASSWORD_RESET = 3
    WITHDRAWAL = 4
    DEPOSIT = 5


fraud_pattern = (
    Pattern("card_fraud")
    .begin(E.LOGIN_FAILURE, count=3)
    .then(E.PASSWORD_RESET, max_gap_ns=10_000_000_000)
    .then(E.WITHDRAWAL, value_gte=500.0, max_gap_ns=30_000_000_000)
    .total_window(60_000_000_000)
)

engine = CEPEngine()
engine.register(fraud_pattern)

alerts: list[dict] = []


@engine.on_match("card_fraud")
def handle_fraud(entity_id: int, pattern_name: str, ts_ns: int):
    alerts.append({"entity": entity_id, "pattern": pattern_name, "ts_ns": ts_ns})
    print(f"[ALERT] {pattern_name} fired for account {entity_id} at {ts_ns}")


def simulate(base_ns: int = 0):
    account = 1001
    t = base_ns

    def ev(type_id, value=0.0, dt_ns=0):
        nonlocal t
        t += dt_ns
        return make_event(type_id, account, value=value, timestamp=t)

    print("--- Normal activity (no alert expected) ---")
    engine.push(ev(E.LOGIN_SUCCESS))
    engine.push(ev(E.DEPOSIT, value=200.0, dt_ns=1_000_000))
    print(f"  alerts so far: {len(alerts)}")

    print("\n--- Fraud sequence ---")
    t0 = t + 1_000_000_000
    t = t0

    engine.push(ev(E.LOGIN_FAILURE, dt_ns=0))             # fail 1
    engine.push(ev(E.LOGIN_FAILURE, dt_ns=500_000_000))   # fail 2
    engine.push(ev(E.LOGIN_FAILURE, dt_ns=500_000_000))   # fail 3  → step advances
    engine.push(ev(E.PASSWORD_RESET, dt_ns=3_000_000_000))
    engine.push(ev(E.WITHDRAWAL, value=750.0, dt_ns=5_000_000_000))

    print(f"\n  alerts so far: {len(alerts)}")
    assert len(alerts) == 1, f"Expected 1 alert, got {len(alerts)}"
    print("\n[PASS] fraud_detection example")


def benchmark(n: int = 100_000):
    account = 9999
    ev = make_event(E.LOGIN_SUCCESS, account)
    start = time.perf_counter_ns()
    for _ in range(n):
        engine.push(ev)
    elapsed = time.perf_counter_ns() - start
    ns_per_event = elapsed / n
    print(f"\nThroughput benchmark: {n:,} events in {elapsed/1e6:.1f} ms")
    print(f"  {ns_per_event:.0f} ns/event  ({1e9/ns_per_event:,.0f} events/s)")
    print(f"  sub-ms per event: {'YES' if ns_per_event < 1_000_000 else 'NO'}")


if __name__ == "__main__":
    simulate(base_ns=1_000_000_000_000)
    benchmark()
