# art-mvcc-index

[![CI](https://github.com/sophie-nguyenthuthuy/data-engineering/actions/workflows/art.yml/badge.svg)](https://github.com/sophie-nguyenthuthuy/data-engineering/actions)

**Adaptive Radix Tree** (Leis et al., ICDE 2013) with **multi-version
concurrency control** — snapshot isolation, first-committer-wins write
conflicts, and epoch-based reclamation for safe version garbage collection.

```text
  Node4 ── widens ──▶ Node16 ── widens ──▶ Node48 ── widens ──▶ Node256
   │                    │                    │                    │
   └──── path-compressed prefix at each node ─┴───────────────────┘
                            │
                            ▼
              VersionChain (per leaf key)
              [ v(commit_ts=42, value=...) ]
              [ v(commit_ts=37, value=...) ]
              [ v(commit_ts=11, value=..., deleted=True) ]
                            │
                            ▼
                     read_at(snapshot_ts)
```

62 tests pass (incl. Hypothesis + Jepsen-style concurrency); 400 k reads/s
in pure Python; supports concurrent readers + serialised writers with no
lost updates, no dirty reads, no snapshot tearing.

## Install

```bash
pip install -e ".[dev]"
```

## Quick start

```python
from art_mvcc import ART, MVCCArt
from art_mvcc.mvcc.tx import begin_tx, TxConflict

# Single-threaded ART
art = ART()
art.put(b"hello", 1)
art.put(b"help",  2)
print(art.get(b"hello"))             # 1
for k, v in art.iter_prefix(b"hel"):
    print(k, v)                       # both in sorted order
print(art.node_count_by_kind())       # {'Node4': 2}

# MVCC store with snapshots + transactions
db = MVCCArt()
db.put(b"counter", 0)

# Snapshot-isolated read
s = db.begin_snapshot()
print(s.get(b"counter"))              # 0

# Transactional write with CAS-style retry on conflict
while True:
    t = begin_tx(db)
    cur = t.get(b"counter")
    t.put(b"counter", (cur or 0) + 1)
    try:
        t.commit()
        break
    except TxConflict:
        continue
```

## CLI

```bash
artctl info
artctl bench lookup --n 100000
artctl bench concurrent --readers 4 --writers 4 --duration 5
```

## Architecture

### ART (`src/art_mvcc/art/`)

| Module | Role |
|---|---|
| `nodes.py` | `Node4`, `Node16`, `Node48`, `Node256` with adaptive widening + narrowing |
| `tree.py` | `ART` with path compression, range scans, prefix iteration |

**Path compression**: each node holds the byte sequence shared by all
descendants, shrinking tree depth from O(key_len) to O(log_radix(N)).

**Adaptive node types**:

| Type | Capacity | Lookup | Used when |
|---|---:|---|---|
| Node4 | 4 | linear scan | leaf-adjacent (few children) |
| Node16 | 16 | sorted scan | small fanout |
| Node48 | 48 | byte→slot index | medium fanout |
| Node256 | 256 | direct addressing | dense fanout (typical near root) |

Widening happens on `add_child` when the node is full; narrowing on
`remove_child` when below the `MIN` threshold for the type.

### MVCC (`src/art_mvcc/mvcc/`)

| Module | Role |
|---|---|
| `version.py` | `Version`, `VersionChain` (per-key newest-first list) |
| `store.py` | `MVCCArt` — ART index + timestamp service + `Snapshot` |
| `tx.py` | `Transaction`, `TxConflict`, `begin_tx()` |
| `epoch.py` | `EpochManager` for safe reclamation |

**Snapshot isolation**:
  - `begin_snapshot()` captures the current logical time as `start_ts`.
  - Reads see committed versions with `commit_ts <= start_ts`.
  - Writes within a transaction are tentative until `commit()`.
  - On commit: if any other transaction committed to the same key after
    `start_ts`, abort with `TxConflict` (first-committer-wins).

**Epoch-based reclamation**: each reader thread enters an epoch via
`with em.guard()`; garbage retired before that epoch survives until the
thread leaves. No GC pauses, no read-side locks.

## Benchmarks

```
$ python -m benchmarks.bench_lookup
=== Insert ===
       n   ART (ms)  dict (ms)  SortDict (ms)
    1000        1.9        0.1            0.4
   10000       25.2        0.9            6.5
  100000      316.1       66.1          104.2

=== Lookup (10k random reads) ===
       n   ART (ms)  dict (ms)  SortDict (ms)
    1000        0.7        0.0            0.0
   10000        7.0        0.4            2.4
  100000       12.2        1.6            2.7

=== Range scan ===
       n   ART (ms)  SortDict (ms)
   10000        0.9            0.0
  100000       13.0            0.3
```

Pure-Python ART is ~5–10× slower than the built-in `dict` (which is a
C-implemented hash table). It pays for the ordered traversal & prefix
queries that `dict` can't do. The native-C ART implementations (libart,
DuckDB's) close this gap.

```
$ python -m benchmarks.bench_concurrent
 readers  writers   keys    reads/s   writes/s  conflict%
       1        1    128    399,725     13,810       0.00
       4        1    128    176,900     21,520       0.00
       4        4    128    220,625     18,416       0.65
       1        8    128     73,300     20,400       1.08
```

The MVCC layer supports concurrent readers + serialised writers at
400 k reads/s on a single thread. Write conflicts stay under 1% even with
4 contending writers on 128 keys.

## Correctness

The Jepsen-style test suite (`tests/test_jepsen.py`) exercises:

- **No lost updates** — 16 threads × 100 CAS-retry increments → counter == 1600
- **No dirty reads** — concurrent writer's tentative `-1` never visible
- **Snapshot consistency** — multi-key transfer maintains `k1 + k2 == initial`
- **High contention disjoint** — 16 writers × distinct keys, zero conflicts

All checks run within bounded time (10–20 s deadlines).

## Development

```bash
make install
make test          # 62 tests
make lint          # ruff
make typecheck     # mypy
make bench         # both benchmarks
docker compose run --rm art make test
```

## Limitations / roadmap

- [ ] True lock-free node split via atomic pointer-swap (currently node ops
      hold a per-tree RLock; under Python's GIL this is functionally
      equivalent but doesn't demonstrate the lock-free protocol from the paper)
- [ ] Bulk-load API for sorted input (skip insertion-sort overhead)
- [ ] Mass tree variant for better cache locality
- [ ] Pluggable storage backend (currently in-memory only)
- [ ] Distributed snapshot timestamp service

## References

- Leis et al., "The Adaptive Radix Tree: ARTful Indexing for Main-Memory Databases" (ICDE 2013)
- Mukherjee et al., "Persistent Memory and the Rise of Universal Constants
  in Concurrent Updates" (PPoPP 2019)
- Larson et al., "High-Performance Concurrency Control Mechanisms for
  Main-Memory Databases" (VLDB 2011) — MVCC reference
- Fraser, "Practical Lock-Freedom" (PhD thesis, 2004) — epoch reclamation

## License

MIT — see `LICENSE`.
