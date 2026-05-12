# disaggregated-storage-engine

[![CI](https://github.com/sophie-nguyenthuthuy/data-engineering/actions/workflows/disagg.yml/badge.svg)](https://github.com/sophie-nguyenthuthuy/data-engineering/actions)

A **compute-storage disaggregated** storage engine: the buffer pool lives
on a remote `PageServer`, compute nodes share it via a **sharded coherence
directory** (write-invalidate), and a **Markov-chain prefetcher** hides
network latency on sequential and locality-rich workloads.

```text
  ┌──────────┐  ┌──────────┐  ┌──────────┐
  │ Client 1 │  │ Client 2 │  │ Client 3 │      ← Compute side
  │ ┌──────┐ │  │ ┌──────┐ │  │ ┌──────┐ │
  │ │Cache │ │  │ │Cache │ │  │ │Cache │ │
  │ └──────┘ │  │ └──────┘ │  │ └──────┘ │
  └────┬─────┘  └────┬─────┘  └────┬─────┘
       │ Transport   │             │
       │  (RDMA sim) │             │
       │  + latency  │             │
       └─────────────┼─────────────┘
                     ▼
        ┌───────────────────────────┐
        │       Page Server         │   ← Storage side
        │  ─ canonical pages        │
        │  ─ LRU evictor            │
        │  ─ sharded coherence dir  │   write-invalidate
        │     (16-way default)      │   directory
        └───────────────────────────┘
```

48 tests pass (incl. multi-client coherence stress); ~200k reads/s
in-process; ~13k reads/s at 50µs simulated cross-AZ latency.

## Install

```bash
pip install -e ".[dev]"
```

## Quick start

```python
from disagg import PageServer, ClientCache, MarkovPrefetcher
from disagg.transport.simulated import SimulatedTransport
from disagg.client.cache import InvalidationRegistry
from disagg.core.page import PageId, PAGE_SIZE

# Set up a server + transport + two clients
server = PageServer(capacity_pages=1024)
transport = SimulatedTransport(server=server, latency_us=5.0)
registry = InvalidationRegistry()

c1 = ClientCache(client_id=1, transport=transport, capacity=64,
                 invalidation_registry=registry)
c2 = ClientCache(client_id=2, transport=transport, capacity=64,
                 invalidation_registry=registry)

# Write from c1 — c2 (if holding the page) gets invalidated automatically
c1.write(PageId(0, 0), b"hello" + b"\x00" * (PAGE_SIZE - 5))
print(c2.read(PageId(0, 0)).data[:5])      # b'hello'
```

## CLI

```bash
disaggctl info
disaggctl bench lookup         # read throughput vs network latency
disaggctl bench prefetch       # cache hit rate by workload
```

## Architecture

### Core (`src/disagg/core/`)

| Module | Role |
|---|---|
| `page.py` | `Page` (versioned), `PageId` (multi-tenant), `PAGE_SIZE=4096` |

### Server (`src/disagg/server/`)

| Module | Role |
|---|---|
| `page_server.py` | Remote buffer pool with LRU eviction + dispatch over Transport |
| `coherence.py`   | `CoherenceDirectory` (N-shard, write-invalidate protocol) |
| `eviction.py`    | `LRUEvictor` (thread-safe, evict-when-over-capacity) |

### Transport (`src/disagg/transport/`)

| Module | Role |
|---|---|
| `api.py`       | `Transport` interface |
| `simulated.py` | In-process simulator: `latency_us`, `jitter_us`, `drop_rate` |

Real production transports are RDMA over `ucx-py` or InfiniBand verbs.

### Client (`src/disagg/client/`)

| Module | Role |
|---|---|
| `cache.py` | Per-client cache + miss handler + `InvalidationRegistry` |

### Prefetcher (`src/disagg/prefetch/`)

| Module | Role |
|---|---|
| `markov.py` | Order-1 Markov chain + bounded memory + phase-change detector |

The prefetcher trains online from observed page accesses. When the recent
window's prediction accuracy falls below `1 - phase_threshold`, the chain
resets (CUSUM-style phase detector) and re-learns.

### Workload (`src/disagg/workload/`)

| Module | Role |
|---|---|
| `scan.py` | OLAP-shaped: sequential scan, Zipfian hot-key |
| `tpcc.py` | OLTP-shaped: TPC-C transaction mix with warehouse locality |

## Coherence protocol

Write-invalidate, directory-tracked:

1. **Read**: client requests page; server adds client to `holders` and
   returns the page + version.
2. **Write**: client sends new data; server bumps the page version, sets
   `writer=client`, and returns the **list of other holders** that must
   invalidate their cached copy.
3. **Invalidation**: in production the server pushes via a control plane;
   here we route through an `InvalidationRegistry` in-process.
4. **Release**: client drops the page; directory removes from `holders`.

The directory is **sharded** (16 shards by default) by `hash(page_id) % N`
to spread lock contention. Each shard has its own `threading.Lock`.

## Benchmarks

```
$ disaggctl bench lookup
  latency µs  reads         ms      reads/s   hits   miss
         0.0   1000        5.0      200,195      0   1000
         5.0   1000       15.6       64,238      0   1000
        50.0   1000       75.3       13,279      0   1000
```

At zero simulated latency, the system pushes ~200k reads/s end-to-end (the
limit is Python dispatch + GIL serialisation). At 5µs (intra-rack RDMA)
~64k reads/s; at 50µs (cross-AZ) ~13k reads/s — consistent with the
Amdahl-style ceiling for synchronous lookups.

```
$ disaggctl bench prefetch
workload          ops     hits   miss   hit%  predict acc
scan-1pass        500      499    500   49.9        100.0
scan-3pass        600     1200      0  100.0        100.0
zipf             1000     1865      0  100.0         43.8
tpcc             1938     1974   1665   54.2         61.3
```

Markov prefetcher achieves:
- **100% prediction accuracy + 100% hit rate** on multi-pass sequential
- **44% prediction accuracy** on Zipfian (low — Zipf has no sequential
  pattern; cache still wins due to hot-key locality)
- **61% prediction accuracy + 54% hit rate** on TPC-C (the OLTP locality
  pays off in cache hits even when prediction is moderate)

## Correctness (concurrency)

The `tests/test_concurrent.py` suite exercises multi-client coherence:

- **Concurrent reads** — 4 clients × 100 reads of the same page, no
  corruption
- **Concurrent writes** — 2 clients × 50 writes; server serialises and the
  final page is one of the two written values
- **Mixed read/write** — readers and writers running for 500 ms;
  invalidations honoured, no inconsistent reads
- **High invalidation throughput** — 200 writes drive ≥ 200 invalidations
  to a holder without deadlock

All tests are deadline-bounded (5-10 s).

## Limitations / roadmap

- [ ] **True RDMA via ucx-py** — transport layer is simulated; production
      needs RDMA verbs for real µs-scale latency
- [ ] **Order-2/3 Markov chains** — currently order-1; higher orders capture
      cycles like `(scan_A, lookup_A, scan_B)`
- [ ] **Hedged requests** — duplicate read to 2 replicas, take faster one
- [ ] **Pinned pages** — currently no pin/unpin; evictor can evict
      in-use pages
- [ ] **Persistent storage layer** — the server is in-memory only; real
      systems back the page server with a log + checkpoints

## Development

```bash
make install
make test         # 48 tests
make lint         # ruff
make typecheck    # mypy
make bench        # both benchmarks
docker compose run --rm disagg make test
```

## References

- Cao et al., "Polardb Serverless: A Cloud Native Database for Disaggregated
  Data Centers" (SIGMOD 2021)
- Verbitski et al., "Amazon Aurora: Design Considerations for High Throughput
  Cloud-Native Relational Databases" (SIGMOD 2017)
- Joseph & Grunwald, "Prefetching using Markov Predictors" (ISCA 1997)
- Antonopoulos et al., "Socrates: The New SQL Server in the Cloud" (SIGMOD 2019)

## License

MIT — see `LICENSE`.
