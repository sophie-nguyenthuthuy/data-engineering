# Changelog

## [0.1.0] — Initial public release

### Added

- `Message` (PUT/DEL with monotone seq)
- `LeafNode` (newest-wins by seq) and `InternalNode` (with buffer + pivots)
- `BEpsilonTree` with:
    - put / get / delete / `__contains__`
    - items / iter_range
    - cascading buffer flushes processed in **descending** child-index
      order to avoid the split-shifts-indices bug
    - leaf-overwrite guard: only newer seq wins
    - thread-safe via `RLock`
- Leaf and internal-node splits with proper buffer partitioning by the
  new separator
- `WorkloadObserver` + `EpsilonTuner` (linear with hysteresis)
- `WriteAmpStats` for measuring leaf_applies, buffer_inserts,
  flushed_messages, splits, and the resulting write amplification ratio
- Workload generators: `mixed_workload`, `write_heavy`, `read_heavy`
- 55 tests across 8 modules including Hypothesis property test
- Benchmarks: write throughput vs ε (with write amp), read latency vs ε
- CLI: `bepsctl bench {write, read}`, `bepsctl info`
- GitHub Actions CI on Python 3.10 / 3.11 / 3.12
- Dockerfile + docker-compose

### Fixed during development

- Original prototype had a deadlock in `WriteAmpStats`: `snapshot()` held
  `_lock` then called `self.write_amplification` (a property that also
  acquired the lock). Replaced `Lock` with `RLock`.
- Original prototype lost messages during cascading flushes when child
  splits shifted indices. Fixed by processing groups in **descending**
  index order.

### Limitations

- Pure Python; no persistent backing store
- Single ε per tree (no per-subtree adaptation)
- Range query is a filtering scan, not a true seek
- At extreme ε (>0.85 with small `node_size`) the tree degenerates
  because pivot capacity shrinks to ≤ 2 children per internal node
