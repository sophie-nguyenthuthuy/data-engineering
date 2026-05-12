# Changelog

## [0.1.0] — Initial public release

### Added

- `Page` / `PageId` abstractions (4 KB pages, multi-tenant, versioned)
- Simulated `Transport` with configurable latency, jitter, drop rate
- `PageServer`: remote buffer pool with LRU eviction
- `CoherenceDirectory`: N-shard write-invalidate protocol
- `ClientCache`: per-compute-node LRU + miss handler
- `InvalidationRegistry`: in-process invalidation routing
- `MarkovPrefetcher`: order-1 chain + bounded memory + phase-change detector
- Workload generators: `scan_workload`, `zipf_workload`, `tpcc_workload`
- 48 tests across 8 files (incl. multi-client concurrency stress with
  deadline-bounded runners)
- Benchmarks: read throughput vs latency, prefetcher hit-rate by workload
- CLI: `disaggctl bench lookup`, `disaggctl bench prefetch`, `disaggctl info`
- GitHub Actions CI on Python 3.10 / 3.11 / 3.12
- Mypy strict, ruff lint, Dockerfile, docker-compose

### Limitations

- Transport is simulated (latency injected via `time.sleep`); production
  needs RDMA via `ucx-py` or InfiniBand verbs
- Order-1 Markov chain only; deeper history would capture more patterns
- No persistent storage layer (page server is in-memory)
- No pinned pages (evictor can evict pages in active use)
