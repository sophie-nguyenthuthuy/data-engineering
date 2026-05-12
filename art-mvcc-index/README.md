# art-mvcc-index

**Adaptive Radix Tree (ART)** from the Leis et al. paper, extended with **multi-version concurrency control** via epoch-based reclamation. Snapshot-isolated analytical scans run concurrently with point-write transactions, no readers blocking writers and vice-versa.

> **Status:** Design / spec phase.

## Why ART + MVCC

- ART is asymptotically optimal for in-memory indexing of integer keys: O(k) lookups with cache-friendly node layout.
- MVCC eliminates reader-writer blocking — essential for HTAP workloads where long scans coexist with point writes.
- The combination is rare in OSS implementations. Most ART variants are single-writer or use coarse locking.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                       ART (node types)                       │
│   Node4 ─▶ Node16 ─▶ Node48 ─▶ Node256  (adaptive widen)     │
│   each ART node ──▶ version chain ──▶ leaf records           │
└──────────────────────────────────────────────────────────────┘
                              │
            ┌─────────────────┴─────────────────┐
            ▼                                   ▼
   ┌────────────────┐                  ┌────────────────┐
   │ Epoch manager  │                  │ Version chain  │
   │ (reclamation)  │                  │ per leaf       │
   │ - per-thread   │                  │ - newest first │
   │   local epoch  │                  │ - GC at epoch  │
   │ - global epoch │                  │   boundary     │
   └────────────────┘                  └────────────────┘
```

## Components

| Module | Role |
|---|---|
| `src/art/nodes.py` | Node4 / Node16 / Node48 / Node256 with SIMD-friendly layout |
| `src/art/operations.py` | Insert, lookup, delete, range scan |
| `src/art/split.py` | **Lock-free node splitting** (the hard part) |
| `src/mvcc/version_chain.py` | Per-leaf chronologically ordered version list |
| `src/mvcc/snapshot.py` | Snapshot construction at scan start |
| `src/mvcc/epoch.py` | Epoch-based reclamation (no RCU, no GC pauses) |
| `src/tests/jepsen/` | Concurrency tests: no lost updates, no dirty reads |

## The hard parts

1. **Lock-free Node48 → Node256 split.** Path-copy on write would defeat ART's cache locality. Use a CAS-based protocol where the parent's child pointer is swung atomically only after the new node is populated.
2. **Epoch reclamation for variable-sized nodes.** Each thread publishes its epoch; the global epoch advances when all threads have observed it. Retired nodes are reclaimed two epochs later — safe because any reader started before retirement has long since finished.
3. **Snapshot isolation under concurrent inserts.** Scan resolves each leaf's version chain against its snapshot timestamp; abort+retry on first-committer-wins.

## Correctness tests

Modeled after Jepsen:
- **Lost-update test:** 64 writers each increment the same key 10⁶ times. Final value must equal 64×10⁶.
- **Dirty-read test:** writer flips key value through invariants; scanner verifies invariants hold for every snapshot.
- **Phantom test:** scanner counts rows matching a predicate twice; counts must equal under SI.

## Benchmarks (targets)

| Op | Baseline (BTreeMap) | ART | ART + MVCC |
|---|---|---|---|
| Point lookup, 1M keys | 200 ns | 70 ns | 90 ns |
| Insert (single-thread) | 300 ns | 150 ns | 200 ns |
| Range scan, 1k keys | 80 µs | 25 µs | 30 µs |
| Mixed (50% read / 50% write, 16 threads) | locks, ~6 Mops/s | needs eval | target ≥ 25 Mops/s |

## References

- Leis et al., "The Adaptive Radix Tree: ARTful Indexing for Main-Memory Databases" (ICDE 2013)
- Wang et al., "Building a Bw-Tree Takes More Than Just Buzz Words" (SIGMOD 2018)
- Fraser, "Practical Lock-Freedom" (PhD thesis, 2004) — epoch reclamation

## Roadmap

- [ ] Node4 / Node16 / Node48 / Node256 single-threaded
- [ ] Adaptive widening + narrowing
- [ ] Range scan with prefix compression
- [ ] Lock-free split protocol
- [ ] Epoch reclamation
- [ ] MVCC version chains
- [ ] Snapshot isolation read path
- [ ] Jepsen-style concurrency test harness
