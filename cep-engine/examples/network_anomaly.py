"""
Network anomaly demo — detect port-scan followed by brute-force SSH:
  1. 5+ ICMP_PROBE events from same source IP within 2 s
  2. 3+ SSH_AUTH_FAIL within 5 s
  3. SSH_AUTH_SUCCESS (compromise indicator) within 10 s

Source IP is encoded as entity_id (int(ipaddress.ip_address(ip))).

Run:
    python examples/network_anomaly.py
"""

import ipaddress
import time

from cep import CEPEngine, Pattern, make_event


class NetEvents:
    ICMP_PROBE = 10
    SSH_AUTH_FAIL = 11
    SSH_AUTH_SUCCESS = 12
    HTTP_GET = 13


def ip_to_id(ip: str) -> int:
    return int(ipaddress.ip_address(ip))


scan_then_compromise = (
    Pattern("scan_compromise")
    .begin(NetEvents.ICMP_PROBE, count=5, within_ns=2_000_000_000)
    .then(NetEvents.SSH_AUTH_FAIL, count=3, max_gap_ns=5_000_000_000)
    .then(NetEvents.SSH_AUTH_SUCCESS, max_gap_ns=10_000_000_000)
    .total_window(30_000_000_000)
)

engine = CEPEngine()
engine.register(scan_then_compromise)

alerts: list[dict] = []


@engine.on_match("scan_compromise")
def handle(entity_id: int, pattern_name: str, ts_ns: int):
    ip = str(ipaddress.ip_address(entity_id))
    alerts.append({"ip": ip, "ts_ns": ts_ns})
    print(f"[ALERT] Likely compromise from {ip} at t={ts_ns}")


def simulate():
    attacker = ip_to_id("10.0.0.55")
    benign = ip_to_id("192.168.1.1")
    t = 1_700_000_000_000_000_000  # arbitrary start ns

    def push(src, type_id, dt_ms=100, value=0.0):
        nonlocal t
        t += dt_ms * 1_000_000
        return engine.push(make_event(type_id, src, value=value, timestamp=t))

    print("--- Benign traffic ---")
    for _ in range(3):
        push(benign, NetEvents.HTTP_GET)
    print(f"  alerts: {len(alerts)}")

    print("\n--- Attack sequence ---")
    for _ in range(5):
        push(attacker, NetEvents.ICMP_PROBE, dt_ms=50)   # port scan
    for _ in range(3):
        push(attacker, NetEvents.SSH_AUTH_FAIL, dt_ms=200)
    push(attacker, NetEvents.SSH_AUTH_SUCCESS, dt_ms=300)

    print(f"  alerts: {len(alerts)}")
    assert len(alerts) == 1
    print("\n[PASS] network_anomaly example")


if __name__ == "__main__":
    simulate()
