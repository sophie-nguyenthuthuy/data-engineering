# adversarial-chaos-engine

Targeted adversarial fuzzing for data pipelines. Instead of throwing
random inputs and waiting for crashes, the engine:

1. Lets you declare **invariants** (`row_count_preserved`, `sum_invariant`,
   `no_nulls`, `value_range`, `monotone`, `distinct_count_preserved`) on
   each transform via a tiny decorator DSL.
2. Generates inputs biased toward an **edge-case library** (IEEE corner
   floats, int overflow boundaries, Unicode oddities, DST/Y2K38
   timestamps).
3. Runs each transform, **shrinks** any failing input down to a minimal
   counterexample.
4. Emits a **valid pytest regression case** (verified to `ast.parse`)
   per discovered violation.

[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Why

Random fuzzing wastes 99 % of cycles on inputs the system already
handles. Real production bugs hide at boundaries that random sampling
almost never produces in reasonable time:

- the row count exactly one past a spill-to-disk threshold;
- the timestamp landing on a DST jump;
- the string with a zero-width joiner or RTL override;
- the float that's `1 + epsilon` away from the test fixture;
- the `Decimal` exactly one digit past max precision.

Each of those is a real bug class. They all live in
`ace.edges.{numeric,strings,timestamps}` so the generator can sample
them directly.

## Architecture

```
        Pipeline + invariant decorators
                     │
                     ▼
        ┌─────────────────────────┐
        │  Catalog (per-test)     │  isolates registry — no global state
        └────────────┬────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │  AdversarialGenerator   │  edge_fraction-biased sampling
        │  (numeric/string/time)  │  on invariant-referenced columns
        └────────────┬────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │  Runner                 │  run pipeline, dedupe violations
        │   + shrink_rows()       │  one-row delta debugging shrinker
        └────────────┬────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │  Report                 │  list[Violation], grouping helpers
        └────────────┬────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │  emit_pytest()          │  valid Python (ast.parse verified)
        └─────────────────────────┘
```

## Install

```bash
pip install -e ".[dev]"
```

Python 3.10+. **Zero runtime dependencies** — stdlib only.

## CLI

```bash
acectl info                              # version
acectl edges                             # edge-case library sizes
acectl run    --trials 100               # run a demo buggy pipeline
acectl bench  --trials 200               # targeted vs. random on the bug zoo
```

Example `acectl edges`:

```
numeric_edges   : 40 values
string_edges    : 21 values
timestamp_edges : 12 values
```

Example `acectl run`:

```
trials=60  pipelines=1
failing=1  exceptions=0
  buggy_abs → sum_invariant(amount)  (input rows: 1)
```

The shrinker reduced the failing input to a single row.

## Library

```python
from ace.invariants.catalog import Catalog
from ace.runner            import Runner
from ace.regression        import emit_pytest

cat = Catalog()

@cat.invariant(sum_invariant=["amount"], no_nulls=["name"])
def clean_transactions(frame):
    # BUG: abs() changes the sum if any input is negative.
    return [{**r, "amount": abs(r["amount"])} for r in frame]

report = Runner(catalog=cat, seed=0).run(trials=100)
for v in report.failing():
    print(emit_pytest(v, module="my_pipeline"))
```

The emitted pytest module:

```python
# Auto-discovered 2026-05-13 by adversarial-chaos-engine.
# Invariant: sum_invariant(amount)
# Observed output: [{'amount': 5}]

from my_pipeline import clean_transactions


def test_clean_transactions_violates_sum_invariant_amount() -> None:
    df_in = [
        {'amount': -5},
    ]
    df_out = clean_transactions(df_in)
    assert sum(r.get('amount', 0) for r in df_out) == sum(r.get('amount', 0) for r in df_in)
```

## Components

| Module                              | Role                                                                |
| ----------------------------------- | ------------------------------------------------------------------- |
| `ace.edges.numeric`                 | `INT_EDGES`, `FLOAT_EDGES`, `numeric_edges()`                       |
| `ace.edges.strings`                 | `STRING_EDGES`, `string_edges()`                                    |
| `ace.edges.timestamps`              | `TIMESTAMP_EDGES`, `timestamp_edges()`                              |
| `ace.invariants.catalog`            | `Catalog` (per-test registry), `invariant` decorator, `InvariantSpec` |
| `ace.invariants.checks`             | `row_count_preserved`, `sum_invariant`, `column_no_nulls`, `column_value_range`, `monotone_increasing`, `distinct_count_preserved` |
| `ace.generator`                     | `AdversarialGenerator` — targeted (edge-biased) + random baseline   |
| `ace.shrinker`                      | `shrink_rows` — row-deletion delta debugger                         |
| `ace.runner`                        | `Runner`, `Report`, `Violation`                                     |
| `ace.regression`                    | `emit_pytest` — `ast.parse`-verified regression source              |
| `ace.bench`                         | `run_benchmark` — targeted vs. random fuzzing on a bug zoo          |
| `ace.cli`                           | `acectl info | edges | run | bench`                                |

## Invariant DSL

```python
@cat.invariant(
    row_count_preserved=True,
    sum_invariant=["amount"],
    no_nulls=["name"],
    value_range={"score": (0.0, 1.0)},
    monotone=["ts"],
    distinct_count_preserved=["user_id"],
)
def my_transform(frame):
    ...
```

Each invariant is translated to an :class:`InvariantSpec` carrying
``(name, check, description, columns)``. The generator reads
``catalog.referenced_columns()`` and ensures every invariant-referenced
column is populated on every generated row.

## Catalog isolation

The original prototype kept a module-level registry that leaked
between tests. The rebuild scopes the registry to a :class:`Catalog`
instance — tests can run in parallel and create their own catalogs
without bumping into each other. A module-level
``ace.invariants.catalog.default_catalog()`` is still available for
ergonomic use in `__main__`-style scripts.

## Shrinker

`shrink_rows(fn, rows, check)` runs Zeller-style delta debugging:
repeatedly attempt to remove one row; keep any subset still failing
the check. Termination is bounded by `max_passes`. The runner uses
this to reduce a 16-row counterexample down to (typically) 1 row, so
the emitted pytest is human-readable.

## Quality

```bash
make lint        # ruff   (E, W, F, I, B, UP, SIM, RUF, TC)
make format      # ruff format
make type        # mypy --strict
make test        # 49 tests
make run         # CLI run on a demo buggy pipeline
make bench       # CLI targeted-vs-random benchmark
make docker      # production image
```

- **49 tests**, 0 failing; includes 1 Hypothesis property test (the
  generator always returns a list of dicts on any seed).
- `mypy --strict` clean over 14 source files.
- Python 3.10 / 3.11 / 3.12 CI matrix + Docker build smoke step.
- Multi-stage slim Docker image, non-root `ace` user.
- **Zero runtime dependencies.**

## References

- Cadar, Dunbar, Engler. *KLEE: Unassisted and Automatic Generation
  of High-Coverage Tests.* OSDI 2008.
- MacIver. *Hypothesis: Property-Based Testing in Python.* 2015.
- Godefroid, Klarlund, Sen. *DART: Directed Automated Random Testing.*
  PLDI 2005.
- Zeller. *Why Programs Fail: A Guide to Systematic Debugging.* 2009.
  (Delta debugging shrinker.)
- Page. *Continuous inspection schemes.* Biometrika 1954. (Reference
  for the underlying philosophy of "targeted vs. random" testing.)

## License

MIT — see [LICENSE](LICENSE).
