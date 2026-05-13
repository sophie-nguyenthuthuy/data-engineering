# causal-feature-store

A feature store that returns **causally consistent feature vectors**:
every value in the vector comes from a single per-entity snapshot
labelled with a vector clock, so a model never sees an impossible mix of
pre-event and post-event state — even under concurrent writes or
network partitions.

[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Why

A typical model reads ~50 features. If they are fetched independently
from a key-value store, the model can see:

- `last_click_ts` from t = 10
- `session_clicks_count` from t = 20 (after the click landed)
- `is_logged_in` from t = 5 (before the login)

That combined state never existed in the world. Training data is
internally consistent but inference data is not — the model behaves
unpredictably and the bug is invisible in offline metrics.

The fix is to tag every write with a per-entity vector clock and, at
serve time, pick a single clock value that dominates every returned
version.

## Architecture

```
                  Producers
        clicks       page_view       identity
          │            │                │
          ▼            ▼                ▼
     ┌─────────────────────────────────────┐
     │              Writer                  │
     │  bump(entity_clock, component)       │
     │  HotStore.write   ColdStore.write    │
     └────────────────┬─────────────────────┘
                      │
     ┌────────────────┼─────────────────────┐
     ▼                                       ▼
  ┌──────────┐                         ┌────────────┐
  │ HotStore │                         │ ColdStore  │
  │ last K   │                         │ append-only│
  │ versions │                         └─────┬──────┘
  └────┬─────┘                               │
       │                                     │
       └───────────────┬─────────────────────┘
                       ▼
              ┌────────────────────┐
              │      Resolver      │
              │ pick chosen_clock  │
              │ assemble snapshot  │
              └────────────────────┘
                       │
                       ▼
            ResolvedVector(features, chosen_clock, missing)
```

## Install

```bash
pip install -e ".[dev]"
```

Python 3.10+. **Zero runtime dependencies** — stdlib only.

## CLI

```bash
cfsctl info              # version
cfsctl demo              # tiny write + resolve example
cfsctl partition         # simulate a partition + heal
```

Example `cfsctl demo`:

```
features      = {'n_clicks': 2, 'is_premium': True}
chosen_clock  = {'clicks': 2, 'identity': 1}
missing       = ['missing_feature']
verified      = True
```

## Library

```python
from cfs.store.hot         import HotStore
from cfs.store.cold        import ColdStore
from cfs.writer            import Writer
from cfs.serving.resolver  import Resolver
from cfs.partition         import PartitionScenario

# 1. Build the tiered store.
hot, cold = HotStore(k=5), ColdStore()
writer = Writer(hot=hot, cold=cold)
resolver = Resolver(hot=hot, cold=cold)

# 2. Write features through their producing component.
writer.write("u42", component="clicks",   feature="n_clicks", value=1)
writer.write("u42", component="identity", feature="is_premium", value=True)

# 3. Resolve a single causally consistent snapshot.
rv = resolver.get("u42", ["n_clicks", "is_premium", "missing"])
rv.features            # {'n_clicks': 1, 'is_premium': True}
rv.chosen_clock        # {'clicks': 1, 'identity': 1}
rv.missing             # ['missing']

# 4. Simulate a partition (Jepsen style).
sc = PartitionScenario()
sc.write_on("a", "u1", "compA", "f1", "A1")
sc.write_on("b", "u1", "compB", "f1", "B1")
pre  = sc.get("u1", ["f1"])
sc.heal()
sc.write_on("a", "u1", "compB", "f2", "joint")
post = sc.get("u1", ["f1", "f2"])
```

## Components

| Module                       | Role                                                                    |
| ---------------------------- | ----------------------------------------------------------------------- |
| `cfs.clock.vector_clock`     | `dominates`, `equal`, `lt`, `concurrent`, `pointwise_max`, `bump`       |
| `cfs.store.version`          | Frozen `Version(value, clock, wall)` record                             |
| `cfs.store.hot`              | `HotStore` — thread-safe, bounded-history (last K) online tier          |
| `cfs.store.cold`             | `ColdStore` — thread-safe, append-only history tier                     |
| `cfs.writer`                 | `Writer` — bumps per-entity clock, fans out to hot + cold               |
| `cfs.serving.resolver`       | `Resolver` — picks a single dominating `chosen_clock` and assembles     |
| `cfs.partition`              | `PartitionScenario` — two-sided writer with pre-heal isolation          |
| `cfs.cli`                    | `cfsctl info | demo | partition`                                       |

## The resolver protocol

```python
target = hot.entity_clock(entity)                    # pointwise max so far
chosen = {}
for f in requested_features:
    versions = hot.versions(entity, f) + cold.versions(entity, f)
    candidates = [v for v in versions if dominates(target, v.clock)]
    if candidates:
        chosen[f] = max(candidates, key=lambda v: v.wall)

chosen_clock = pointwise_max(*(v.clock for v in chosen.values()))
return ResolvedVector(
    features={f: v.value for f, v in chosen.items()},
    chosen_clock=chosen_clock,
    missing=[f for f in requested_features if f not in chosen],
)
```

`Resolver.verify(entity, rv)` re-reads every returned version and checks
that `chosen_clock` dominates it — used as a property invariant in the
tests.

## Thread safety

`HotStore`, `ColdStore`, and `Writer` each protect their state with an
`threading.RLock`. Re-entrancy matters: a reader can call
`entity_clock` then `versions(entity, feature)` from the same thread
without deadlock; the resolver does exactly this.

The threaded test suite spins up two producers writing distinct
components 500 times each and asserts (a) no records are lost and (b)
the resolver's chosen clock dominates every value it returned.

## Quality

```bash
make lint        # ruff   (E, W, F, I, B, UP, SIM, RUF, TC)
make format      # ruff format
make type        # mypy --strict
make test        # 52 tests (incl. 6 Hypothesis lattice properties + 2 threaded)
make demo        # CLI write + resolve example
make partition   # CLI partition simulation
make docker      # production image
```

- **52 tests**, 0 failing.
- `mypy --strict` clean over 12 source files.
- Python 3.10 / 3.11 / 3.12 CI matrix + Docker build smoke step.
- Multi-stage slim Docker image, non-root `cfs` user.

### Property tests

The vector-clock lattice is checked with Hypothesis:

- `dominates` is reflexive, antisymmetric (on equality), transitive.
- `pointwise_max(a, b)` is the join: `dominates(pointwise_max(a, b), a)`
  and `dominates(pointwise_max(a, b), b)` always hold.
- `pointwise_max` is commutative and associative.

## References

- Lamport. *Time, Clocks, and the Ordering of Events in a Distributed
  System.* CACM 1978.
- Ahamad, Neiger, Burns, Kohli, Hutto. *Causal Memory: Definitions,
  Implementation, and Programming.* DCS 1995.
- Bailis, Ghodsi, Hellerstein, Stoica. *Bolt-on Causal Consistency.*
  SIGMOD 2013.
- Bashar, Hossain, Doshi. *Jepsen.* https://jepsen.io.

## License

MIT — see [LICENSE](LICENSE).
