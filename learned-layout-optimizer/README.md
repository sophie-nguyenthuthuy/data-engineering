# learned-layout-optimizer

A **closed-loop background agent** that continuously retunes the physical
data layout (Z-order, Hilbert curve, sort key) to match a drifting query
workload. Cast as a contextual bandit: actions are layout rewrites,
context is the workload profile, reward is **pages saved per query**
under shadow replay minus an amortised I/O cost.

[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Why

Static Z-ordering picks a fixed column priority once and lives with it
forever. Real workloads drift — a column hot today is cold next month —
so static layouts pay the worst-case of every workload they've ever
seen. An online agent can re-converge to the current optimum, amortising
the I/O cost of layout rewrites across the queries they accelerate.

## Architecture

```
   Query stream
        │
        ▼
   ┌─────────────┐
   │  Profile    │  per-column freq, range fraction, co-occurrence
   └──────┬──────┘
          │
          ▼
   ┌─────────────┐    drift?
   │   Policy    │ ◄──────── DriftDetector  (total-variation distance)
   │ (heuristic /│
   │  UCB1 /     │
   │  ε-greedy / │
   │  Thompson)  │
   └──────┬──────┘
          │ action ∈ {noop, sortkey, zorder, hilbert}
          ▼
   ┌─────────────┐
   │ Replay /    │   page-model cost of last `window` queries
   │ Reward      │   under the proposed permutation
   └──────┬──────┘
          │ reward
          └──────────► back to policy.update(action, reward)
```

## Install

```bash
pip install -e ".[dev]"
```

Python 3.10+. Single runtime dependency: `numpy`.

## CLI

```bash
llo info                                                       # version
llo bench    --rows 4096 --queries 800 --shift-every 200       # static layouts
llo simulate --rows 4096 --queries 800 --act-every 50          # UCB closed-loop
```

Example output of `llo bench`:

```
rows=4096  queries=800  shift_every=200
layout               mean pages
noop                      48.21
sortkey:a                 24.32
zorder:a,b                10.07
hilbert:a,b                9.94
zorder:a,b,c,d            12.31
```

## Library

```python
import numpy as np
from llo.workload.profile import Query, WorkloadProfile
from llo.workload.drift import DriftDetector
from llo.policy.bandit import Action, UCBPolicy, ThompsonPolicy, heuristic_action
from llo.replay.pages import apply_layout, expected_pages, reward
from llo.curves.spacefill import z_order_index, hilbert_index, hilbert_index_nd
from llo.agent.loop import LayoutAgent

# 1. Build a workload profile.
prof = WorkloadProfile(columns=["a", "b", "c"])
prof.set_domain("a", 0.0, 100.0)
for q in stream_of_queries:
    prof.observe(q)

# 2. Pick a layout — either rule-based or learned.
action = heuristic_action(prof)
# … or train a bandit:
policy = UCBPolicy(actions=[
    Action("noop", ()),
    Action("zorder", ("a", "b")),
    Action("hilbert", ("a", "b")),
])

# 3. Closed-loop agent: observes queries, periodically acts.
agent = LayoutAgent(data=data, columns=["a", "b", "c"], policy=policy, profile=prof)
agent.run(queries, act_every=50)
```

## Components

| Module                       | Role                                                                  |
| ---------------------------- | --------------------------------------------------------------------- |
| `llo.curves.spacefill`       | Z-order (N-D), Hilbert (2-D and N-D Skilling)                         |
| `llo.workload.profile`       | `WorkloadProfile` — per-column freq, range fraction, selectivity      |
| `llo.workload.drift`         | `DriftDetector` — TV distance on column-frequency vectors             |
| `llo.policy.bandit`          | `Action`, `HeuristicPolicy`, `UCBPolicy`, `EpsilonGreedyPolicy`, `ThompsonPolicy` |
| `llo.replay.pages`           | Page-model shadow replay: pages-scanned, expected-pages, reward       |
| `llo.agent.loop`             | `LayoutAgent` — closed loop: observe → choose → replay → update       |
| `llo.bench`                  | Shifted-workload generator + static-layout evaluator                  |
| `llo.cli`                    | `llo` entry point                                                     |

## Curves

- **Z-order** (Morton 1966): bit-interleave the per-column coordinates;
  N-D, O(d·bits) per row.
- **Hilbert 2-D** (Hilbert 1891): iterative bit-by-bit rotate/reflect.
  Consecutive 1-D keys are guaranteed grid-adjacent in 2-D (tested as a
  property: max Manhattan distance between successive points = 1).
- **Hilbert N-D** (Skilling 2004): transposed-axes Gray-coded algorithm
  for arbitrary `d`. `d * bits ≤ 63` so keys fit in `uint64`.

All three reject negative coordinates, out-of-range values, and bad bit
counts up-front.

## Policies

Bandits share the `choose() / update(action, reward)` interface.

- **HeuristicPolicy** — top-2 frequent columns, pick `hilbert` if both
  are heavily range-queried, else `zorder`.
- **UCBPolicy** — UCB1, exploration coefficient `c = √2` by default.
- **EpsilonGreedyPolicy** — ε-greedy with injectable `random.Random`.
- **ThompsonPolicy** — Gaussian Thompson with closed-form posterior
  under known observation variance.

`Action` is `(kind, cols)` with strict arity checks in `__post_init__`:
`sortkey` takes 1 column, `zorder`/`hilbert` take ≥ 2, `noop` takes 0.

## Replay reward

```
reward = expected_pages(noop) − expected_pages(action) − io_cost / |window|
```

A *page* is a contiguous block of `PAGE_ROWS = 64` rows; a query reads
a page iff any row matches every predicate. Layouts that cluster
matching rows reduce pages-scanned, which is what an analytics engine
actually cares about.

## Drift detection

```
TV(p, q) = ½ Σ_c |p(c) − q(c)|       (column-freq vectors)
```

When TV exceeds `threshold`, the agent re-baselines and the bandit gets
to re-explore. No numpy required, O(|columns|) per check.

## Quality

```bash
make lint        # ruff   (E, W, F, I, B, UP, SIM, RUF, TC)
make format      # ruff format
make type        # mypy --strict
make test        # 73 tests
make bench       # CLI static-layout comparison
make simulate    # CLI closed-loop UCB run
make docker      # production image
```

- **73 tests** (~70 deterministic + 2 Hypothesis property), 0 failing.
- `mypy --strict` clean over 14 source files.
- Python 3.10 / 3.11 / 3.12 CI matrix + Docker build smoke step.
- Multi-stage slim Docker image, non-root `llo` user.

## References

- Morton, G. M. *A Computer Oriented Geodetic Data Base and a New
  Technique in File Sequencing.* 1966.
- Hilbert, D. *Über die stetige Abbildung einer Linie auf ein
  Flächenstück.* Math. Ann. 38, 1891.
- Skilling, J. *Programming the Hilbert Curve.* AIP Conf. Proc. 707,
  381–387, 2004.
- Auer, P., Cesa-Bianchi, N., Fischer, P. *Finite-time analysis of the
  multiarmed bandit problem.* Machine Learning 47(2-3), 2002. (UCB1)
- Russo, D., Van Roy, B. *An Information-Theoretic Analysis of
  Thompson Sampling.* JMLR 17(68), 2016.

## License

MIT — see [LICENSE](LICENSE).
