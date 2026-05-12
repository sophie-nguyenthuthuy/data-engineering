# Changelog

## [0.1.0] — Initial public release

### Added

- Adaptive Radix Tree (Leis et al., ICDE 2013) with all 4 node types
  (Node4 / Node16 / Node48 / Node256) and adaptive widening + narrowing
- Path compression: each node carries the shared prefix of its descendants
- Range scans, prefix iteration, and ordered traversal
- Multi-version concurrency control:
    - `VersionChain` (per-key, newest-first ordering)
    - `Snapshot` for isolated reads
    - `Transaction` with first-committer-wins on commit
- `EpochManager` for epoch-based reclamation
- 62 tests, including:
    - 42 unit tests (nodes, tree ops, path compression, iteration)
    - Hypothesis property tests comparing against `dict` ground-truth
    - 4 Jepsen-style chaos tests (lost-updates, dirty-reads, snapshot
      consistency, high-contention disjoint)
- Benchmarks vs `dict` and `sortedcontainers.SortedDict`
- CLI: `artctl bench lookup`, `artctl bench concurrent`, `artctl info`
- GitHub Actions CI matrix on Python 3.10 / 3.11 / 3.12
- Mypy strict, ruff lint, Dockerfile, docker-compose

### Limitations

- Pure-Python; the GIL limits write throughput vs C/Rust implementations
- Lock-free node split is implemented as a per-tree RLock (functionally
  equivalent under the GIL; doesn't demonstrate the lock-free protocol
  from the original paper)
- No persistent storage backend (in-memory only)
