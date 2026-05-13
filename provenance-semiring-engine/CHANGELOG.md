# Changelog

## [0.1.0] — Initial public release

### Added

- Abstract `Semiring[T]` base
- 5 instances: `BagSemiring`, `BooleanSemiring`, `WhyProvenance`,
  `HowProvenance` (with `Polynomial` + `Monomial`), `TriCS`
- 5 annotated relational operators: `select`, `project`, `union`,
  `join`, `aggregate`, plus `annotate` for building base relations
- Lineage queries: `lineage`, `witnesses`, `multiplicity`,
  `exact_probability` (truth-table for ≤ 18 vars)
- Polynomial evaluation under variable substitution
- 43 tests across 5 modules:
    - Hypothesis-checked semiring axioms (60 random triples per instance)
    - TriCS quasi-semiring failure regression test
    - Per-operator behavior + end-to-end pipelines
- CLI: `provctl demo`, `provctl info`
- GitHub Actions CI matrix Python 3.10/3.11/3.12

### Documented quasi-semiring

TriCS does NOT satisfy distributivity for correlated events. The
`test_trics_distributivity_known_failure` test pins this as known
behavior; the docstring documents the workaround (truth-table
`exact_probability` on the how-polynomial).

### Limitations

- No lifted probabilistic inference (truth-table is O(2^n))
- No negation/difference (would need m-semirings)
- No outer joins
- No SQL parser
