# Changelog

## [0.1.0] — Initial public release

### Added

- Window-function IVM: `RowNumberIVM`, `RankIVM`, `DenseRankIVM`,
  `LagLeadIVM`, `SlidingSumIVM` (SUM / AVG over ROWS BETWEEN n PRECEDING
  AND CURRENT)
- `PerKeySum`, `PerKeyCount`, `PerKeyAvg`, `PerKeyMax`, `PerKeyMin`
- `CorrelatedSubqueryIVM`: lateral-join rewrite with `qualifying()`
  membership tracking
- Nested aggregates: `MaxOfSum`, `SumOfMax` with held-max bookkeeping
- `StrategyController` with hysteresis + `LinearCostModel`
- 3 workload generators (mixed, burst, sliding-window)
- 50 tests including Hypothesis property tests vs full-recompute ground
  truth
- Benchmarks: delta-vs-full speedup tables
- CLI: `ivmctl bench delta-vs-full`, `ivmctl info`
- GitHub Actions CI matrix Python 3.10/3.11/3.12

### Bugs found while building

- `SlidingSumIVM` initially rebuilt the entire prefix-sum array on every
  insert, making it slower than full recompute. Fixed by doing an
  O(tail-size) shift after insert; for in-order inserts this is O(1).
- `StrategyController` initially didn't apply `history_size` (the deque
  always had maxlen=20 from the field default). Added `__post_init__`
  to honour the constructor parameter.

### Limitations

- No `RANGE` / `GROUPS` frame variants
- `RowNumberIVM` uses Python list insert (O(n)); a balanced BST would
  give true O(log n)
- No SQL frontend; programmatic API only
- Two-level nested aggregates only (MAX(SUM), SUM(MAX)); N-level
  generalisation is roadmap
