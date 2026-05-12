# Chandy-Lamport Distributed Snapshot Algorithm

A from-scratch Python implementation of the **Chandy-Lamport distributed snapshot algorithm** — the theoretical core behind Apache Flink's checkpointing — applied to a multi-node streaming pipeline with full **failure recovery** and **exactly-once semantics** proof.

## What This Is

The [Chandy-Lamport algorithm](https://lamport.azurewebsites.net/pubs/chandy.pdf) (1985) captures a *globally consistent state* of a distributed system without stopping it.  Consistency means: for every message M that appears as "received" in node Q's snapshot, M also appears as "sent" in node P's snapshot.  No message is lost; no message is invented.

Apache Flink implements this exact algorithm (calling the markers "barriers") to checkpoint stateful streaming jobs.  This repo builds every piece from primitives: FIFO channels, marker propagation, channel state recording, coordinator assembly, recovery, and an end-to-end exactly-once proof.

---

## Pipeline Topology

```
SourceA ─────────────────────────────────────────► MergeNode ──► Aggregator ──► Sink
SourceB ──► SlowTransform (×2, 120 ms/msg) ───────►╯
```

The **MergeNode** is the critical node: it has two incoming channels.  When the fast-path marker (from SourceA) arrives first, MergeNode records its local state and begins recording the slow-path channel.  Any SourceB messages that have already left SlowTransform but haven't reached MergeNode yet are captured as **in-transit channel state** — the canonical Chandy-Lamport demonstration.

---

## The Algorithm

### Snapshot initiation (source nodes)
1. Record local state.
2. Send `Marker` on every outgoing channel.
3. Local snapshot immediately complete (no incoming channels).

### Marker reception (processing nodes)

**First marker** on channel `C`:
1. Record local state — this is the node's snapshot.
2. Set `state(C) = ∅` (FIFO guarantees all pre-snapshot messages on C arrived before the marker).
3. Start recording every *other* incoming channel.
4. Propagate `Marker` downstream.

**Subsequent markers** on channel `C`:
1. Stop recording `C`.
2. `state(C)` = messages recorded since step 1 above — these are the in-transit messages.

When markers have arrived on **all** incoming channels, the node submits its `NodeSnapshot` to the `SnapshotCoordinator`.

### Global snapshot
The coordinator collects `NodeSnapshot`s from every node.  Once all have reported, it assembles the `GlobalSnapshot`.

---

## Recovery

```
1. Stop all nodes
2. Drain all channels  (discard post-snapshot messages)
3. Restore each node's state from NodeSnapshot
4. Re-inject in-transit messages into their channels
5. Restart all nodes
   └─ Sources resume from checkpointed next_seq
```

### Why exactly-once is preserved

| Property | Mechanism |
|---|---|
| No message processed twice before snapshot | State is restored to *before* in-transit messages were applied |
| No message processed twice after recovery | `Node._seen` set deduplicates by `msg_id` |
| No message skipped | In-transit messages are replayed; sources resume from checkpoint |
| Sources don't re-emit | `SourceNode.state = {"next_seq": k}` — resumes from k, not 1 |

---

## Project Structure

```
chandy-lamport-snapshot/
├── chandy_lamport/
│   ├── message.py      # DataMessage, Marker
│   ├── channel.py      # Thread-safe FIFO channel with recording
│   ├── node.py         # Node base + SourceNode, TransformNode, MergeNode,
│   │                   #   AggregatorNode, SinkNode, SlowTransformNode
│   ├── snapshot.py     # NodeSnapshot, GlobalSnapshot, SnapshotCoordinator
│   └── pipeline.py     # Pipeline assembly + take_snapshot() + recover()
├── tests/
│   ├── test_snapshot.py       # Protocol correctness, coordinator, FIFO invariant
│   ├── test_recovery.py       # State restoration, source offsets, in-transit replay
│   └── test_exactly_once.py   # End-to-end exactly-once proof, idempotency guard
└── demo.py             # Interactive walkthrough with live output
```

---

## Quick Start

```bash
# No external dependencies — pure stdlib
python demo.py

# Run the test suite
pip install pytest pytest-timeout
pytest -v
```

### Demo output (abridged)

```
════════════════════════════════════════════════════════════════
  Chandy-Lamport Distributed Snapshot Demo
════════════════════════════════════════════════════════════════

  Phase 2 · Snapshot
  ──────────────────────────────────────────────────────────────

  GlobalSnapshot [snap-3a7f]:
    NodeSnapshot(SourceA,    state={'next_seq': 9},              in_transit={})
    NodeSnapshot(SourceB,    state={'next_seq': 7},              in_transit={})
    NodeSnapshot(SlowTx,     state={'processed': 4},             in_transit={})
    NodeSnapshot(Merge,      state={'received': 11},             in_transit={'SlowTx->Merge': 2})
    NodeSnapshot(Aggregator, state={'sum': 34, 'count': 11},     in_transit={})
    NodeSnapshot(Sink,       state={'count': 11, ...},           in_transit={})

  ✓ Captured 2 in-transit message(s) in channel states.
    Channel 'SlowTx->Merge': [Data(8, seq=4, ...), Data(10, seq=5, ...)]

  Phase 6 · Exactly-once semantics proof
  ──────────────────────────────────────────────────────────────
  ✓ No missing seqs
  ✓ No extra duplicates
  Aggregator final sum: 330   Expected: 330  ✓

════════════════════════════════════════════════════════════════
  RESULT: Exactly-once semantics PRESERVED ✓
════════════════════════════════════════════════════════════════
```

---

## Key Files by Concept

| Concept | File | Key symbol |
|---|---|---|
| Marker propagation | `node.py` | `Node._on_marker` |
| Channel state recording | `channel.py` | `Channel.start_recording / stop_recording` |
| Global snapshot assembly | `snapshot.py` | `SnapshotCoordinator.receive` |
| Recovery procedure | `pipeline.py` | `Pipeline.recover` |
| Exactly-once guard | `node.py` | `Node._seen` + `Node._handle_data` |
| FIFO invariant test | `tests/test_snapshot.py` | `test_first_channel_state_is_empty` |

---

## Connection to Apache Flink

| This implementation | Flink equivalent |
|---|---|
| `Marker` | Checkpoint barrier |
| `SnapshotCoordinator` | `CheckpointCoordinator` (JobManager) |
| `NodeSnapshot.state` | Operator state backend (RocksDB / heap) |
| `NodeSnapshot.channel_states` | In-flight buffer snapshot |
| `SourceNode.next_seq` | Source offset (Kafka offset, file position) |
| `Pipeline.recover()` | Job recovery from savepoint |

Flink adds aligned/unaligned barrier modes, asynchronous state serialization, and distributed storage (HDFS/S3) — but the core invariant is identical.

---

## References

- Chandy, K. M. & Lamport, L. (1985). *Distributed Snapshots: Determining Global States of Distributed Systems*. ACM TOCS 3(1).
- Carbone et al. (2017). *State Management in Apache Flink*. PVLDB 10(12).
- [Flink Checkpointing docs](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/)
