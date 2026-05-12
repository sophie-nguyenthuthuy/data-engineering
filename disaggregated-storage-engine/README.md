# disaggregated-storage-engine

A storage engine where the **buffer pool lives on a remote machine**, accessed over RDMA (or shared-memory + injected latency in dev). Multiple compute nodes share the remote buffer pool, coordinated by a lightweight page-level coherence protocol. Prefetcher uses a Markov chain over page access sequences.

> **Status:** Design / spec phase.

## Why

Disaggregation (separating compute from storage at the page-cache level) is the architecture behind Aurora, Socrates, Snowflake's FDN, and TiKV's TiFlash. The hard problems are:

1. **Latency hiding** — every page miss is a network round-trip. Prefetching has to be aggressive *and* accurate.
2. **Coherence** — N compute nodes sharing one remote buffer pool need a page-level invalidation/lease protocol that doesn't bottleneck.
3. **Workload differentiation** — OLTP wants point reads; OLAP wants sequential scans. The prefetcher must adapt.

## Architecture

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  Compute 1  │  │  Compute 2  │  │  Compute N  │
│  ┌───────┐  │  │  ┌───────┐  │  │  ┌───────┐  │
│  │ Local │  │  │  │ Local │  │  │  │ Local │  │
│  │ cache │  │  │  │ cache │  │  │  │ cache │  │
│  └───┬───┘  │  │  └───┬───┘  │  │  └───┬───┘  │
└──────┼──────┘  └──────┼──────┘  └──────┼──────┘
       │                │                │
       └────── RDMA ────┴── RDMA ────────┘
                        │
                ┌───────▼────────┐
                │ Remote Buffer  │   ← Markov prefetcher
                │     Pool       │   ← Coherence directory
                │ (page server)  │
                └───────┬────────┘
                        │
                ┌───────▼────────┐
                │  Cold storage  │   (S3-like, page-addressable)
                └────────────────┘
```

## Components

| Module | Role |
|---|---|
| `src/page_server/` | Remote buffer pool: pin/unpin, page faults, eviction |
| `src/coherence/` | Page lease + invalidation protocol (write-invalidate, MESI-style) |
| `src/prefetch/markov.py` | Order-k Markov chain over page IDs, online training |
| `src/transport/` | RDMA (`ucx`) or shared-mem+latency sim for dev |
| `src/client/` | Compute-side page cache + fault handler |
| `src/workload/` | TPC-C + TPC-H drivers for benchmarking |

## Hard parts (where the work is)

1. **Coherence directory at line rate.** A naive global lock kills throughput. Use sharded directories keyed by page-id ranges, with per-shard lock-free queues.
2. **Prefetcher accuracy under mixed workloads.** A single Markov model trained on all traffic gets worse than no prefetcher. Solution: per-tenant or per-table-segment chains, with online detection of access pattern phase changes.
3. **Tail latency.** p99.9 network blip → query stall. Two-sided requests with hedging at p95.

## Benchmarks (targets)

| Workload | Local SSD baseline | This engine target |
|---|---|---|
| TPC-C (16 cores, 100 warehouses) | 8000 tpmC | ≥ 6000 tpmC (75% retention) |
| TPC-H Q1 (SF=100) | 12 s | ≤ 18 s (1.5× regression) |
| Cold-cache point-read p99 | 0.5 ms | ≤ 2 ms |

## References

- Aurora: "Amazon Aurora: Design Considerations for High Throughput Cloud-Native Relational Databases" (SIGMOD 2017)
- Socrates: "Socrates: The New SQL Server in the Cloud" (SIGMOD 2019)
- Page-level coherence: VAX-Cluster directory protocol, modernised
- Markov prefetcher: Joseph & Grunwald, "Prefetching using Markov Predictors" (ISCA 1997)

## Roadmap

- [ ] Page-addressable cold store interface
- [ ] Remote buffer pool with sharded directory
- [ ] Compute-side page cache + fault handler over shared-mem+latency
- [ ] Online Markov prefetcher with phase detection
- [ ] RDMA transport (ucx-py)
- [ ] TPC-C / TPC-H harness
- [ ] Coherence stress tests (Jepsen-style)
