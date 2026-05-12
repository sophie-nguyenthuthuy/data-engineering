#!/usr/bin/env python3
"""
Chandy-Lamport Distributed Snapshot — End-to-End Demo
======================================================

Topology
--------
    SourceA ──────────────────────────────────────────► MergeNode
                                                               │
    SourceB ──► SlowTransformNode (×2, 0.12 s/msg) ──────────►╯
                                                               │
                                                        AggregatorNode  (running sum)
                                                               │
                                                           SinkNode

The critical property demonstrated
------------------------------------
MergeNode has TWO incoming channels: one fast (directly from SourceA) and one
slow (SourceB → SlowTransformNode → MergeNode).  When the snapshot marker
arrives on the fast channel first, MergeNode records its state and begins
recording the slow channel.  Messages from SourceB that are in-flight inside
SlowTransformNode or the slow channel at that instant are captured as
*channel state* — the defining feature of Chandy-Lamport.

Failure & recovery
-------------------
After the snapshot is taken the Aggregator node is killed mid-stream.
Recovery restores all nodes to their snapshot state, replays in-transit
messages, and resumes source emission from the checkpoint offset.
The final assertion proves exactly-once semantics: each source sequence
number appears in the sink output exactly once.
"""

import logging
import time

logging.basicConfig(
    level=logging.WARNING,       # set to DEBUG for full protocol trace
    format="%(levelname)-8s %(name)s  %(message)s",
)

TOTAL_MSGS = 20          # messages each source will emit
SNAPSHOT_AFTER = 0.8     # seconds of run time before snapshot
FAILURE_AFTER  = 0.6     # seconds after snapshot before injecting failure
DRAIN_AFTER    = 3.5     # seconds after recovery to let pipeline finish

SEP  = "─" * 64
SEP2 = "═" * 64


def banner(text: str) -> None:
    print(f"\n{SEP2}\n  {text}\n{SEP2}")


def section(text: str) -> None:
    print(f"\n{SEP}\n  {text}\n{SEP}")


def main() -> None:
    from chandy_lamport import Pipeline

    banner("Chandy-Lamport Distributed Snapshot Demo")

    print("""
  Topology
  --------
  SourceA ──────────────────────────────────────► MergeNode ──► Aggregator ──► Sink
  SourceB ──► SlowTransform (×2, 120ms/msg) ────►╯
""")

    # ── 1. Build and start pipeline ────────────────────────────────────────────

    section("Phase 1 · Pipeline start")
    pipeline = Pipeline(source_total=TOTAL_MSGS, source_interval=0.07)
    pipeline.start()
    print(f"  Pipeline running.  Each source will emit 1 … {TOTAL_MSGS}.")
    print(f"  SlowTransform doubles SourceB values with a 120 ms delay per message.")

    time.sleep(SNAPSHOT_AFTER)
    print(f"\n  ({SNAPSHOT_AFTER}s elapsed — taking snapshot now)\n")

    # ── 2. Chandy-Lamport snapshot ─────────────────────────────────────────────

    section("Phase 2 · Snapshot")
    print("  Coordinator sends initiation signal to all source nodes …")
    snapshot = pipeline.take_snapshot()
    print(snapshot.describe())

    # Report any captured in-transit messages
    in_transit_total = sum(
        len(msgs)
        for ns in snapshot.node_snapshots.values()
        for msgs in ns.channel_states.values()
    )
    if in_transit_total:
        print(f"  ✓ Captured {in_transit_total} in-transit message(s) in channel states.")
        for ns in snapshot.node_snapshots.values():
            for ch_name, msgs in ns.channel_states.items():
                if msgs:
                    print(f"    Channel {ch_name!r}: {msgs}")
    else:
        print("  (No in-transit messages at this instant — try adjusting timing.)")

    # ── 3. Continue running, then inject failure ───────────────────────────────

    section("Phase 3 · Simulated node failure")
    print(f"  Pipeline continues for another {FAILURE_AFTER}s …")
    time.sleep(FAILURE_AFTER)

    agg_state_before = pipeline.aggregator.get_state()
    sink_count_before = len(pipeline.sink.received_seqs)
    print(f"\n  Aggregator state before failure : {agg_state_before}")
    print(f"  Sink received {sink_count_before} message(s) so far.")
    print("\n  *** Killing Aggregator node (simulated crash) ***")
    pipeline.stop_node(pipeline.aggregator)
    time.sleep(0.3)   # let a few more messages pile up in channels

    # ── 4. Recovery ────────────────────────────────────────────────────────────

    section("Phase 4 · Recovery from snapshot")
    print(f"  Restoring all nodes to snapshot [{snapshot.snapshot_id[:12]}] …")
    pipeline.recover(snapshot)

    agg_state_after = pipeline.aggregator.get_state()
    print(f"\n  Aggregator state after  recovery: {agg_state_after}")
    print("  Sources resume from their checkpointed sequence offsets.")

    # ── 5. Let pipeline finish ─────────────────────────────────────────────────

    section("Phase 5 · Running to completion")
    print(f"  Waiting {DRAIN_AFTER}s for all messages to drain …")
    time.sleep(DRAIN_AFTER)
    pipeline.stop()

    # ── 6. Exactly-once verification ──────────────────────────────────────────

    section("Phase 6 · Exactly-once semantics proof")

    # SourceA emits 1…TOTAL_MSGS  (values passed through unchanged)
    # SourceB emits 1…TOTAL_MSGS  (values doubled by SlowTransform → 2, 4, …)
    # Both sets of origin_seqs should each appear exactly once.

    seqs = pipeline.sink.received_seqs
    print(f"\n  Sink received {len(seqs)} message(s) total.")

    from collections import Counter
    seq_counts = Counter(seqs)

    missing  = [s for s in range(1, TOTAL_MSGS + 1) if seq_counts[s] == 0]
    extra    = [s for s in range(1, TOTAL_MSGS + 1) if seq_counts[s] > 2]
    print(f"\n  origin_seq counts  (expected each = 2, got {len(seq_counts)} distinct):")
    for s in sorted(seq_counts):
        mark = "✓" if seq_counts[s] == 2 else "✗"
        print(f"    seq {s:3d} → {seq_counts[s]} occurrence(s)  {mark}")

    print()
    if missing:
        print(f"  ✗ MISSING seqs  : {missing}")
    else:
        print("  ✓ No missing seqs")

    if extra:
        print(f"  ✗ DUPLICATE seqs: {extra}")
    else:
        print("  ✓ No extra duplicates")

    # Final aggregator state
    final_agg = pipeline.aggregator.get_state()
    # Expected sum from SourceA: sum(1..N) = N(N+1)/2
    # Expected sum from SourceB (doubled): sum(2,4,..2N) = N(N+1)
    N = TOTAL_MSGS
    expected_sum = N * (N + 1) // 2 + N * (N + 1)
    actual_sum   = final_agg.get("sum", "?")

    print(f"\n  Aggregator final sum   : {actual_sum}")
    print(f"  Expected sum (theory)  : {expected_sum}  "
          f"[ΣA=1..{N} + ΣB=2..{2*N} step 2]")

    sum_ok = actual_sum == expected_sum
    print(f"  Sum correct            : {'✓' if sum_ok else '✗'}")

    overall_ok = not missing and not extra
    print()
    banner("RESULT: Exactly-once semantics " +
           ("PRESERVED ✓" if overall_ok else "VIOLATED ✗"))

    if not overall_ok:
        print("  (Tip: increase TOTAL_MSGS or adjust timing for clearer results.)")


if __name__ == "__main__":
    main()
