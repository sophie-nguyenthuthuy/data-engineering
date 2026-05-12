"""
Interactive demo: HLC vs wall-clock anomalies in a multi-region pipeline.

Shows:
  1. Causal inversion produced by NTP-drifted wall clocks.
  2. The same scenario with HLC — zero inversions.
  3. Stale-read protection via causal_get.

Run with:
    python demo/demo_anomalies.py
"""
from __future__ import annotations

import time

from hlc_store.anomaly import find_causal_inversions
from hlc_store.clock import HybridLogicalClock
from hlc_store.region import CausalEvent, Region
from hlc_store.store import MetadataStore
from hlc_store.timestamp import HLCTimestamp

HR = "─" * 65


def header(title: str) -> None:
    print(f"\n{'═' * 65}")
    print(f"  {title}")
    print(f"{'═' * 65}")


def show_events(events: list[CausalEvent], label: str) -> None:
    by_id = {e.event_id: e for e in events}
    print(f"\n  {label}")
    print(f"  {'event id':<20} {'node':<12} {'timestamp':<22} {'causal parent'}")
    print(f"  {'─'*20} {'─'*12} {'─'*22} {'─'*20}")
    for e in sorted(events, key=lambda x: x.wall_ms_at_event):
        parent = by_id.get(e.caused_by)
        parent_ts = str(parent.ts) if parent else "(root)"
        print(f"  {e.event_id:<20} {e.node_id:<12} {str(e.ts):<22} {parent_ts}")


def show_inversions(events: list[CausalEvent]) -> None:
    inversions = find_causal_inversions(events)
    if not inversions:
        print("\n  ✓  No causal inversions detected.")
    else:
        print(f"\n  ✗  {len(inversions)} causal inversion(s) detected:")
        for inv in inversions:
            print(f"     cause   {inv.cause.event_id}  ts={inv.cause.ts}")
            print(f"     effect  {inv.effect.event_id}  ts={inv.effect.ts}  ← LOWER than cause!")


# ─── Demo 1: causal inversion ─────────────────────────────────────────────────

def demo_causal_inversion() -> None:
    header("DEMO 1 — Causal Inversion")
    print("""
  Scenario: us-east writes a config update, then replicates to eu-west
  and ap-south.  eu-west's NTP clock is drifted -300ms behind real time.
  In a wall-clock system, eu-west's receive timestamp is lower than the
  source timestamp, inverting the causal order.
""")

    drifts = {"us-east": 0, "eu-west": -300, "ap-south": +50}

    for kind, use_hlc in [("Wall-clock system (NTP only)", False), ("HLC system", True)]:
        print(f"  [{kind}]")
        us = Region("us-east", drift_ms=drifts["us-east"], use_hlc=use_hlc)
        eu = Region("eu-west", drift_ms=drifts["eu-west"], use_hlc=use_hlc)
        ap = Region("ap-south", drift_ms=drifts["ap-south"], use_hlc=use_hlc)

        e1 = us.write("db-config", '{"host":"primary","port":5432}')
        e2 = us.replicate_to(eu, "db-config", caused_by_event=e1)
        e3 = eu.replicate_to(ap, "db-config", caused_by_event=e2)

        all_events = us.events() + eu.events() + ap.events()
        show_events(all_events, f"Events ({kind})")
        show_inversions(all_events)
        print()


# ─── Demo 2: NTP clock jump ────────────────────────────────────────────────────

def demo_clock_jump() -> None:
    header("DEMO 2 — NTP Clock Correction (Jump Backward)")
    print("""
  Scenario: eu-west's NTP daemon corrects the clock backward by 500ms
  mid-operation.  In a wall-clock system this breaks monotonicity for
  all subsequent events.  HLC absorbs the jump via the logical counter.
""")

    for kind, use_hlc in [("Wall-clock", False), ("HLC", True)]:
        print(f"  [{kind}]")
        primary = Region("primary", drift_ms=0, use_hlc=use_hlc)
        replica = Region("replica", drift_ms=0, use_hlc=use_hlc)

        e1 = primary.write("schema", "v1")
        e2 = primary.replicate_to(replica, "schema", caused_by_event=e1)

        # NTP correction: clock jumps backward
        replica.drift_ms = -500
        print(f"    *** NTP correction on replica: clock jumps -500ms ***")

        e3 = replica.write("schema", "v2", caused_by_event=None)
        e4 = replica.replicate_to(primary, "schema", caused_by_event=e3)

        all_events = primary.events() + replica.events()
        show_events(all_events, f"Events after clock jump ({kind})")
        show_inversions(all_events)
        print()


# ─── Demo 3: stale-read protection ────────────────────────────────────────────

def demo_stale_read() -> None:
    header("DEMO 3 — Stale-Read Protection via causal_get")
    print("""
  Scenario: a client writes endpoint=http://new-host on the primary.
  It then reads from a replica that is 200ms behind.  With a plain
  `get`, the replica might return the old value.  With `causal_get`
  supplying the write timestamp, the replica blocks until it has
  applied the write and always returns the correct value.
""")

    primary_clk = HybridLogicalClock("primary")
    primary = MetadataStore(primary_clk)
    replica_clk = HybridLogicalClock("replica", drift_ms=-200)
    replica = MetadataStore(replica_clk)

    _ = primary.put("endpoint", "http://old-host")
    write_ts = primary.put("endpoint", "http://new-host")
    print(f"  Client wrote 'http://new-host' at timestamp {write_ts}")

    # Replicate to replica
    replica.put("endpoint", "http://new-host", remote_ts=write_ts)

    result = replica.causal_get("endpoint", after=write_ts, timeout_s=1.0)
    assert result is not None
    value, ts = result
    print(f"  causal_get returned '{value}' at {ts}")
    assert value == "http://new-host"
    print("  ✓  No stale read — causal_get blocked until replica was up-to-date.")


# ─── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    demo_causal_inversion()
    demo_clock_jump()
    demo_stale_read()
    print(f"\n{'═' * 65}")
    print("  All demos complete.")
    print(f"{'═' * 65}\n")


if __name__ == "__main__":
    main()
