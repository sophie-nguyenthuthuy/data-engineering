# postgres-vs-clickhouse-benchmark

A cross-engine query benchmark harness. Ten TPC-H queries + five
NY-taxi queries; pluggable engines (SQLite reference, an injectable
adapter that wraps any driver); deterministic timing with warmup,
repeat, trim, and p50/p95/p99 stats; cross-engine speedup report.

[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Why

If you only need a "Postgres is faster on Q3, ClickHouse on Q5" table,
you don't need a benchmark *framework* — but if you intend to iterate
on configs, tier choices, schemas, or indexes, you do. This harness
makes the noisy parts (warmup, clock skew, outlier suppression) a
configuration setting and the comparative output a stable artefact you
can store next to the change you made.

## Components

| Module                          | Role                                                         |
| ------------------------------- | ------------------------------------------------------------ |
| `pvc.workloads.base`            | `Query`, `Workload` (id/name/SQL with validation)            |
| `pvc.workloads.tpch`            | `TPCH_QUERIES` — 10 representative TPC-H queries              |
| `pvc.workloads.nytaxi`          | `NY_TAXI_QUERIES` — 5 typical NY-taxi queries                |
| `pvc.engines.base`              | `Engine` ABC + `EngineError`                                 |
| `pvc.engines.sqlite`            | `SQLiteEngine` (stdlib reference)                            |
| `pvc.engines.injectable`        | `InjectableEngine` wrapping any `(sql) → rows` callable      |
| `pvc.stats`                     | `LatencyStats` + `summarise` (nearest-rank percentiles)      |
| `pvc.benchmark`                 | `BenchmarkRunner` — warmup / repeat / trim                   |
| `pvc.report`                    | `build_comparison` + `ComparisonReport.winners()`            |
| `pvc.cli`                       | `pvcctl info | list-queries | demo`                         |

## Install

```bash
pip install -e ".[dev]"
```

Python 3.10+. **Zero runtime dependencies.**

## CLI

```bash
pvcctl info
pvcctl list-queries --workload tpch
pvcctl demo --rows 50000
```

## Library — wire up a real engine

```python
from pvc.benchmark           import BenchmarkRunner
from pvc.engines.sqlite      import SQLiteEngine
from pvc.engines.injectable  import InjectableEngine
from pvc.report              import build_comparison
from pvc.workloads.tpch      import TPCH_QUERIES

# Reference engine — always available.
sqlite = SQLiteEngine(path=":memory:")
sqlite.setup(ddl=[...], inserts=[...])

# Production engine — wrap your driver of choice.
import psycopg2
conn = psycopg2.connect("postgresql://...")
def pg_exec(sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        try:
            return list(cur.fetchall())
        except psycopg2.ProgrammingError:
            return []
postgres = InjectableEngine(execute_fn=pg_exec, name="postgres", closer=conn.close)

# Run the bench.
runner = BenchmarkRunner(
    engines=[sqlite, postgres],
    workload=TPCH_QUERIES,
    warmup=2, repeat=20, trim=2,    # drop the slowest + fastest 2 of every 20
)
results = runner.run()
report = build_comparison(results, baseline="sqlite")
print(report.winners())   # {"Q1": "postgres", "Q3": "postgres", ...}
```

## Timing methodology

- **Warmup**: first `warmup` runs are discarded to let caches and
  connection pools settle.
- **Repeat**: next `repeat` runs are timed with `time.perf_counter`
  (the monotonic clock guaranteed by CPython for this purpose).
- **Trim**: the slowest and fastest `trim` samples are dropped from
  the timed set — a conservative way to suppress GC spikes without
  throwing away signal. The constraint `2 * trim < repeat` is checked
  at construction time.
- **Percentiles**: nearest-rank (`percentile_disc`) so every reported
  number is one that *actually appeared* in the sample.

## Quality

```bash
make test       # 40+ tests, 1 Hypothesis property
make type       # mypy --strict
make lint
```

- **44 tests**, 0 failing; 1 Hypothesis property
  (p50 ≤ p95 ≤ p99 ≤ max across random samples).
- mypy `--strict` clean over 12 source files; ruff clean.
- Multi-stage slim Docker image, non-root `pvc` user.
- Python 3.10 / 3.11 / 3.12 CI matrix.
- **Zero runtime dependencies** — uses stdlib `sqlite3` for the
  reference engine, no other engine is bundled.

## License

MIT — see [LICENSE](LICENSE).
