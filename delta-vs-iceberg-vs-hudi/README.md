# delta-vs-iceberg-vs-hudi

Three mini-implementations of the dominant table formats, plus a
common CDC workload harness that drives all of them through the same
events and reports commits, write amplification, and read-time file
count. The point is to make the trade-offs *visible* — the formats
are real, the metrics are real.

[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## What's inside

| Format       | Key mechanic                                                       |
| ------------ | ------------------------------------------------------------------ |
| **Delta**    | Transaction log: each commit appends a JSON entry to `_delta_log/<NNNN>.json`. Optimistic-concurrency races resolve on filename. |
| **Iceberg**  | Snapshot tree: each commit is a new immutable snapshot pointing at a parent + a list of (file, ADDED/DELETED) entries. |
| **Hudi CoW** | Copy-on-Write: every update **rewrites** the whole base file of the affected file group. Reads are cheap, writes amplify. |
| **Hudi MoR** | Merge-on-Read: updates write small delta-log files; reads merge base + logs; a compaction step periodically folds logs back into a new base. |

## Why

The three formats agree on the high level (immutable data files +
versioned metadata that points at them) but diverge sharply on:

- **Concurrency**: Delta races on filename; Iceberg uses an atomic
  metadata pointer; Hudi uses a per-file-group write lock.
- **Update cost**: Iceberg + Delta both rewrite at file granularity;
  Hudi CoW *always* does; Hudi MoR amortises with delta logs.
- **Read cost**: CoW = 1 file per group; MoR = 1 base + logs, merged
  on the fly until compaction.

This package implements just enough of each to surface those
differences in a deterministic test.

## Components

| Module                       | Role                                                                |
| ---------------------------- | ------------------------------------------------------------------- |
| `tfl.delta.action`           | `Action`, `ActionType`, `FileEntry` (commit-log records)            |
| `tfl.delta.table`            | `DeltaTable` — optimistic-concurrency log + replay-based state     |
| `tfl.iceberg.table`          | `IcebergTable` — snapshot tree + time-travel                       |
| `tfl.hudi.table`             | `HudiCoWTable`, `HudiMoRTable` + Hudi timeline                      |
| `tfl.bench.workload`         | `CDCEvent`, `CDCOp`, `Workload`                                     |
| `tfl.bench.compare`          | `run_workload(wl) → CompareReport` (commits / write-amp / read-files) |
| `tfl.cli`                    | `tflctl info | compare`                                            |

## Install

```bash
pip install -e ".[dev]"
```

Python 3.10+. **Zero runtime dependencies.**

## CLI

```bash
tflctl info
tflctl compare --events 500 --insert-pct 20 --update-pct 70 --delete-pct 10
```

Example output:

```
workload    = synthetic-cdc
update_pct  = 0.69
format     commits  write_amp  read_files
delta          347        253          39
iceberg        347        253          39
hudi_cow       347        347          50
hudi_mor       547        547         103
lowest_write_amp = delta
```

The numbers shift with the update ratio — at low update rates (an
ETL pipeline), Delta and Iceberg are equally cheap; at high update
rates (CDC into a slowly-changing dimension), Hudi MoR's write cost
explodes unless you compact often.

## Library

```python
from tfl.bench.workload import CDCEvent, CDCOp, Workload
from tfl.bench.compare  import run_workload

wl = Workload(
    name="cdc-sample",
    events=tuple([
        CDCEvent(op=CDCOp.INSERT, key="u1", payload_size=128),
        CDCEvent(op=CDCOp.UPDATE, key="u1", payload_size=128),
        CDCEvent(op=CDCOp.UPDATE, key="u1", payload_size=128),
        CDCEvent(op=CDCOp.DELETE, key="u1", payload_size=0),
    ]),
)
report = run_workload(wl)
for m in report.metrics:
    print(m.name, m.commits, m.write_amplification, m.read_files_at_end)
```

## Concurrency model

```python
from tfl.delta.table  import DeltaTable, DeltaConflict
from tfl.delta.action import Action, ActionType, FileEntry

t = DeltaTable()
v = t.commit([Action(ActionType.METADATA, schema_id=0)], expected_version=-1)

# Two writers race for the next version:
try:
    t.commit([Action(ActionType.ADD, file=FileEntry("a", 100, 10))], expected_version=v)
    t.commit([Action(ActionType.ADD, file=FileEntry("b", 100, 10))], expected_version=v)
except DeltaConflict:
    print("loser retries with t.version()")
```

The threaded test (`test_concurrent_writers_one_wins`) proves the
guarantee under real threads: of two simultaneous commits at the same
``expected_version``, exactly one wins; the other gets `DeltaConflict`.

## Quality

```bash
make test       # 41 tests
make type       # mypy --strict
make lint
```

- **41 tests**, 0 failing.
- mypy `--strict` clean over 12 source files; ruff clean.
- Multi-stage slim Docker image, non-root `tfl` user.
- Python 3.10 / 3.11 / 3.12 CI matrix.
- **Zero runtime dependencies.**

## License

MIT — see [LICENSE](LICENSE).
