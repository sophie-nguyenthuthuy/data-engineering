# provenance-semiring-engine

A query engine that **annotates every tuple with a provenance token** and propagates tokens through query operators using **semiring operations** (Green–Karvounarakis–Tannen, PODS 2007). Trace any output back to the exact input tuples that contributed, across arbitrarily deep pipelines.

Three semiring instances ship:

1. **Why-provenance** — sets of witness combinations (the minimal proofs).
2. **How-provenance** — polynomials in N[X] (which combinations, how many times, with which operators).
3. **TriCS** — semiring for probabilistic databases (computes output marginals).

> **Status:** Design / spec phase.

## Why

Lineage tracking that's bolted on after the fact gets the wrong answer in the presence of duplicates, projections, and aggregations. The semiring framework is the *only* compositional theory — it makes provenance a first-class data type that propagates through arbitrary relational algebra.

## The algebra

A commutative semiring `K = (K, ⊕, ⊗, 0, 1)` where:

| Relational op | Semiring op |
|---|---|
| union (∪) | `⊕` |
| join (⋈), projection collapse | `⊗` |
| selection (σ) | identity (no token change) |
| difference, aggregation | depends on semiring |

Instances:

| K | `⊕` | `⊗` | use |
|---|---|---|---|
| `(N, +, *, 0, 1)` | + | * | bag semantics (counting) |
| `(2^X, ∪, ×, ∅, {()})` | union | cartesian | Why-provenance |
| `N[X]` polynomials | + | * | How-provenance |
| `([0,1], ⊕ₚ, *)` | indep-OR | * | probabilistic (TriCS) |

## Architecture

```
SQL → Logical plan → Operator tree with K-annotated tuples
                            │
                            ▼
                  Per-operator ⊕ / ⊗ rules
                            │
                            ▼
                  Output tuple with K-annotation
                            │
              ┌─────────────┴─────────────┐
              ▼                           ▼
          Why-provenance               How-provenance
          (which inputs?)              (how derived?)

       Inversion: given an output tuple,
       recover the set of input tuples that
       contributed (lineage drill-through).
```

## Components

| Module | Role |
|---|---|
| `src/semiring/` | Abstract base + 3 instances (Why, How, TriCS) |
| `src/operators/` | Annotated relational ops: σ, π, ⋈, ∪, γ |
| `src/planner/` | Compiles SQL → annotated plan; instance is a runtime knob |
| `src/lineage/` | Inversion: output tuple → set of input rows |
| `src/storage/` | Sparse polynomial representation for How-provenance |
| `src/eval/` | Validation: lineage results vs. ground truth on TPC-H |

## Example: How-provenance

Inputs:
```
R = {a:t1, b:t2}    S = {a:t3}
```
Query: `R ⋈ S ∪ R`

Output token for tuple `a`:
```
(t1 ⊗ t3) ⊕ t1 = t1*t3 + t1 = t1*(t3 + 1)
```

Output token for tuple `b`:
```
t2
```

The polynomial tells you not just *that* `t1` contributed but *how* — via both the join and the union.

## Example: Lineage drill-through

Given output tuple `(a, ...)` with token `t1*t3 + t1`, the **lineage** is `{t1, t3}` — recover by looking at all variables that appear.

For an aggregation output, this might be hundreds of input rows. The engine returns an iterator (sparse representation) so users can `LIMIT` the drill-through.

## Probabilistic queries (TriCS)

If each input tuple has a probability `p_i`, replace variables with probabilities:

```
P(output = a) = 1 - (1 - p1 * p3)(1 - p1)
```

For non-trivial queries this is #P-hard in general; we implement a *lifted* inference that's polynomial for safe queries and falls back to approximate inference (Monte Carlo) for unsafe ones.

## Benchmarks

- **Why-provenance overhead:** target ≤ 2× baseline TPC-H query time.
- **How-provenance overhead:** target ≤ 5× (polynomial blow-up is the cost of completeness).
- **Lineage query latency:** sub-second for any TPC-H output tuple.

## References

- Green, Karvounarakis, Tannen, "Provenance Semirings" (PODS 2007)
- Buneman et al., "Why and Where: A Characterization of Data Provenance" (ICDT 2001)
- Suciu et al., *Probabilistic Databases* (2011)

## Roadmap

- [ ] Semiring abstract base + 3 instances
- [ ] Annotated σ / π / ⋈ / ∪
- [ ] Annotated aggregation (semiring-aware reductions)
- [ ] SQL parser + plan compiler
- [ ] Sparse polynomial store
- [ ] Inversion / lineage API
- [ ] TriCS probability evaluator
- [ ] TPC-H validation harness
