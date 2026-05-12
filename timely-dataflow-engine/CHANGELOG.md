# Changelog

## [0.1.0] — Initial public release

### Added

- `Timestamp(epoch, iteration)` lattice with partial order, `join`, `meet`,
  `next_iter`, `next_epoch`
- `Antichain` (pairwise-incomparable timestamps) with insert / remove /
  dominates / less_than
- `ProgressTracker`: per-pointstamp counts with `InvariantViolation` on
  negative counts
- `Frontier`: antichain of minimal active timestamps per operator
- `ProgressCoordinator`: multi-worker delta broadcast + listener subscriptions
- `GraphBuilder`: `source` / `map` / `filter` / `reduce` / `iterate` / `sink`
- `Runtime`: single-worker queue-drain executor
- Examples: iterative PageRank, toy belief propagation
- `spec/progress.tla` formalising the non-negative-counts invariant
- 44 tests including Hypothesis lattice-axiom checks + concurrent
  coordinator stress (8 threads × 200 updates → consistent final state)
- Benchmark: timely vs naive PageRank (3 small graphs)
- CLI: `timelyctl pagerank`, `timelyctl bench`, `timelyctl info`
- GitHub Actions CI on Python 3.10/3.11/3.12

### Bugs found while building

- Source operator originally didn't forward records to downstream — initial
  inputs sat in the source's queue and the source's `fn` was a no-op.
  Fixed by adding `downstream` parameter to `source()`.

### Limitations

- Single-worker runtime (multi-worker coordinator exists but no
  per-thread scheduler yet)
- No frontier-triggered emit (operators emit per-record)
- No state checkpointing → no crash recovery
- No built-in n-way join operator
