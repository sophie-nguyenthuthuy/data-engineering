# CRDT Distributed Data Quality Counters

A coordinator-free system where **50 pipeline workers** each track data quality
metrics locally, then merge automatically across the cluster using
**Conflict-free Replicated Data Types (CRDTs)**.

No central coordinator. No locks. Guaranteed eventual consistency.

---

## CRDTs implemented

| Type | Use case | Merge rule |
|---|---|---|
| **G-Counter** | null counts, valid counts, histogram buckets | element-wise max of node vectors |
| **PNCounter** | anomaly count (detected − resolved) | two G-Counters (P and N) |
| **OR-Set** | distinct anomaly type strings seen | union tokens, subtract tombstones |
| **HyperLogLog CRDT** | approx. distinct value count | element-wise max of register arrays |

All four types satisfy the three CRDT merge laws:

- **Commutative** — `merge(a, b) = merge(b, a)`
- **Associative** — `merge(merge(a, b), c) = merge(a, merge(b, c))`
- **Idempotent** — `merge(a, a) = a`

---

## Architecture

```
Worker-000          Worker-001   …   Worker-049
  GCounter(nulls)     GCounter         GCounter
  GCounter(valid)     GCounter         GCounter
  PNCounter(anomaly)  PNCounter        PNCounter
  ORSet(types)        ORSet            ORSet
  HyperLogLog         HyperLogLog      HyperLogLog
       ↑                  ↑                ↑
       └──────────────────┴────── gossip / ring / full merge
```

Workers process independent data partitions. Merges are pushed via three
topology modes — no coordinator ever reads or writes global state.

---

## Merge topologies

| Topology | Messages per round | Rounds to converge | When to use |
|---|---|---|---|
| **Full** | O(n²) | 1 | small clusters, batch reconciliation |
| **Gossip** | O(n·fanout) | ~log_f(n) | large clusters, low overhead |
| **Ring** | O(n) | O(n) | streaming pipelines, strict ordering |

Gossip with `fanout=3` on 50 workers converges in ~5 rounds (theory: log₃50 ≈ 3.6).

---

## Project layout

```
crdt-data-quality/
├── src/
│   ├── crdts/
│   │   ├── g_counter.py        # G-Counter
│   │   ├── pn_counter.py       # PNCounter (two G-Counters)
│   │   ├── or_set.py           # OR-Set with tombstones
│   │   └── hyperloglog.py      # HyperLogLog CRDT (register-max merge)
│   ├── metrics.py              # WorkerMetrics — all CRDTs per column
│   ├── worker.py               # PipelineWorker — processes a partition
│   └── cluster.py              # Cluster — 50 workers + merge strategies
├── tests/
│   ├── test_g_counter.py       # CRDT law proofs for G-Counter
│   ├── test_pn_counter.py      # CRDT law proofs for PNCounter
│   ├── test_or_set.py          # CRDT law proofs for OR-Set
│   ├── test_hyperloglog.py     # CRDT law proofs + accuracy
│   └── test_cluster.py         # End-to-end convergence proofs
├── benchmarks/
│   └── benchmark_merge.py      # Merge overhead across topologies & sizes
└── scripts/
    └── demo.py                 # Live convergence walkthrough
```

---

## Quick start

```bash
# no external dependencies required
python -m pytest tests/ -v          # 46 tests, all CRDT laws proven
python scripts/demo.py              # 50-worker gossip convergence demo
python benchmarks/benchmark_merge.py   # merge timing across cluster sizes
```

Optional dev dependencies:

```bash
pip install pytest pytest-cov
```

---

## Sample demo output

```
Workers: 50  |  Rows: 100,000  |  Null rate: 5%

[1/3] Workers processing partitions locally …
      Pre-merge spread: null=44, valid=44

[2/3] Gossip merging (fanout=3, up to 7 rounds) …
      Round  5: null spread=0  ✓ CONVERGED

[3/3] Global quality report
  Total observed    : 100,000
  Null count        : 4,880  (4.88%)
  Anomaly count     : 2040
  Anomaly types     : duplicate_key, out_of_range, referential_integrity, …
  Distinct values   : ~94,841  (±3.25%)
  Merge rounds      : 5
  Avg merge time    : 13.8 ms
```

---

## HyperLogLog accuracy

| Precision (b) | Registers (2^b) | Theoretical error | Observed error (n=50k) |
|---|---|---|---|
| 8 | 256 | 6.50% | ~6% |
| 10 | 1,024 | 3.25% | ~3% |
| 12 | 4,096 | 1.63% | ~1.5% |
| 14 | 16,384 | 0.81% | ~0.8% |

The HyperLogLog CRDT merge (element-wise max of register arrays) produces the
same estimate as if a single node had seen all values — provably, because each
register tracks the max leading-zero run across all inputs.

---

## CRDT law proofs

Every law is mechanically verified in the test suite:

```
tests/test_g_counter.py::test_merge_commutativity     PASSED
tests/test_g_counter.py::test_merge_associativity     PASSED
tests/test_g_counter.py::test_merge_idempotency       PASSED
tests/test_g_counter.py::test_merge_is_least_upper_bound  PASSED
tests/test_cluster.py::test_convergence_variance_zero_after_full_merge  PASSED
tests/test_cluster.py::test_gossip_merge_converges    PASSED
tests/test_cluster.py::test_ring_merge_eventually_converges  PASSED
```

---

## Key design decisions

**OR-Set tombstone semantics** — tokens are UUIDs scoped to `(node_id, uuid4)`.
A remove tombstones only tokens the removing node has *observed*. A concurrent
add from another node generates a fresh token that survives the tombstone,
giving add-wins semantics without a vector clock.

**HyperLogLog as a CRDT** — standard HyperLogLog is already a join-semilattice:
`merge = max`. The precision/register count must be identical across nodes
(enforced at merge time). The implementation uses SHA-256 truncated to 64 bits
for a uniform hash distribution.

**PNCounter composition** — rather than allowing negative G-Counter increments
(which would violate monotonicity), a PNCounter holds two independent G-Counters.
The value is `P - N`; each component merges independently.
