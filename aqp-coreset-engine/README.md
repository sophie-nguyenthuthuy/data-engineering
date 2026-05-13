# aqp-coreset-engine

**Approximate query processing** via coresets — weighted samples whose
queries provably approximate the same query on the full dataset. Built
on Feldman–Langberg sensitivity sampling for SUM/COUNT, merge-and-reduce
for streaming, KLL for quantiles, and Horvitz–Thompson confidence
intervals for the answers analytics dashboards actually display.

[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Why coresets, not uniform samples

A uniform 1 % sample answers `AVG(amount)` accurately for the bulk of
the data, but is hopeless for `AVG(amount) WHERE category = 'rare'` —
the rare stratum may have zero samples. **Sensitivity sampling** picks
each row with probability proportional to its maximum normalised
contribution to any query in the class, then reweights survivors by the
inverse inclusion probability. The result is an **unbiased estimator**
whose variance is bounded uniformly over the query class — including
the rare-stratum queries that break uniform sampling.

## Guarantee

For a class `Q` with VC-dimension `vc`, sensitivity sampling produces a
coreset of size

```
m = ⌈(1/ε²) · (vc + log(1/δ))⌉           (Feldman & Langberg, STOC 2011)
```

such that for any `q ∈ Q`:

```
| ans(q on coreset) − ans(q on full data) |  ≤  ε · ans(q on full data)
```

with probability ≥ 1 − δ.

## Install

```bash
pip install -e ".[dev]"
```

Python 3.10+. Single runtime dependency: `numpy`.

## CLI

```bash
aqpctl info                                                       # version
aqpctl size --eps 0.05 --delta 0.01                               # required m
aqpctl validate --rows 20000 --queries 200 --eps 0.05 --delta 0.01
aqpctl quantile --rows 50000 --eps 0.01                           # KLL demo
```

Example `aqpctl validate`:

```
n_rows=5000  queries=50  eps=0.05  delta=0.01
sensitivity    m=  2243  coverage= 1.000  mean_rel_err= 0.0219  max_rel_err= 0.0439
uniform        m=  2243  coverage= 1.000  mean_rel_err= 0.0103  max_rel_err= 0.0209
```

## Library

```python
from aqp.coreset.sensitivity import SensitivityCoreset
from aqp.coreset.streaming   import StreamingSumCoreset
from aqp.coreset.kll         import KLLSketch
from aqp.queries.predicates  import range_pred, eq_pred, box_pred, and_
from aqp.bounds.size         import coreset_size_sum

# 1. Offline coreset over a fixed table.
sens = SensitivityCoreset(eps=0.05, delta=0.01)
for value, payload in rows:
    sens.add(value, payload)
cs = sens.finalize()

# 2. Estimate + 95 % CI.
pred = and_(eq_pred(0, 3.0), range_pred(1, 10.0, 20.0))
ci = cs.sum_confidence_interval(pred, level=0.95)
print(ci.estimate, ci.lo, ci.hi)

# 3. Streaming variant: bounded memory, online merge-and-reduce.
stream = StreamingSumCoreset(base_size=512)
for value, payload in unbounded_stream:
    stream.add(value, payload)
report = stream.finalize().query_sum(pred)

# 4. KLL for quantiles.
kll = KLLSketch.for_epsilon(eps=0.01)
for x in values:
    kll.add(x)
median, p95 = kll.quantile(0.5), kll.quantile(0.95)

# 5. Size planning.
m_required = coreset_size_sum(eps=0.05, delta=0.01)  # → 2243
```

## Components

| Module                            | Role                                                                  |
| --------------------------------- | --------------------------------------------------------------------- |
| `aqp.coreset.core`                | `WeightedRow`, `Coreset`, `ConfidenceInterval` + SUM/COUNT/AVG + CIs |
| `aqp.coreset.sensitivity`         | Offline Feldman–Langberg sensitivity sampler                          |
| `aqp.coreset.uniform`             | Reservoir uniform sample (baseline)                                   |
| `aqp.coreset.streaming`           | Merge-and-reduce SUM coreset, ``O(base_size · log(n / base_size))``  |
| `aqp.coreset.kll`                 | KLL quantile sketch with associative `merge`                          |
| `aqp.queries.predicates`          | `eq_pred`, `range_pred`, `box_pred`, `and_`                          |
| `aqp.bounds.size`                 | `coreset_size_sum` (FL), `hoeffding_count_size`                       |
| `aqp.eval`                        | `validate_coverage` — empirical CI coverage on random range queries   |
| `aqp.cli`                         | `aqpctl` entry point                                                  |

## Confidence intervals

For weighted rows `(vᵢ, wᵢ)` selected by predicate `p`:

```
SUM_est  = Σ wᵢ vᵢ          (Horvitz–Thompson, unbiased)
σ̂²       = Σ (wᵢ vᵢ)²        (per-row contribution variance upper bound)
CI       = SUM_est  ±  z(level) · σ̂
```

`level ∈ {0.90, 0.95, 0.99}` use cached z-scores; other levels go
through a Beasley–Springer–Moro inverse-Φ approximation (tested
against the known values 1.96, 2.5758).

## Streaming merge-and-reduce

Maintains a binary stack of fixed-size coresets. Adding `n` rows yields
`⌈log₂(n / base_size)⌉` levels, each holding `base_size` rows. Errors
compound by a factor per level, so picking `base_size ≈ √n` keeps the
relative error within a small constant of the offline bound.

## KLL quantiles

`KLLSketch.for_epsilon(eps)` picks `k = ⌈1 / eps⌉`, giving a sketch
with O(k · log(n/k)) memory and ε-additive rank error. `merge` is
associative when both sketches share the same `k`.

## Quality

```bash
make lint        # ruff   (E, W, F, I, B, UP, SIM, RUF, TC)
make format      # ruff format
make type        # mypy --strict
make test        # 72 tests
make validate    # CLI coverage report
make quantile    # CLI KLL demo
make docker      # production image
```

- **72 tests**, 0 failing; includes 2 Hypothesis property tests
  (`query_count == total_weight`, KLL quantile monotonicity).
- `mypy --strict` clean over 13 source files.
- Python 3.10 / 3.11 / 3.12 CI matrix + Docker build smoke step.
- Multi-stage slim Docker image, non-root `aqp` user.

## References

- Feldman & Langberg. *A unified framework for approximating and
  clustering data.* STOC 2011.
- Karnin, Lang, Liberty. *Optimal Quantile Approximation in Streams.*
  FOCS 2016. (KLL sketch.)
- Bagchi, Chaudhuri, Indyk, Mitzenmacher. *Streaming algorithms for
  geometric problems.* PODS 2006. (Merge-and-reduce skeleton.)
- Horvitz & Thompson. *A generalization of sampling without replacement
  from a finite universe.* JASA 1952.
- Beasley & Springer / Moro. *Inverse normal CDF.* Algorithm AS 111,
  1977.

## License

MIT — see [LICENSE](LICENSE).
