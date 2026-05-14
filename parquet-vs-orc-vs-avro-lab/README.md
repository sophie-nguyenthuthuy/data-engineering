# parquet-vs-orc-vs-avro-lab

From-scratch implementations of the three dominant analytical-storage
formats — Parquet-like (row groups + per-chunk stats footer + encoding
heuristics), ORC-like (stripes + leading stats index), Avro-like (row-
oriented, schema-once header) — plus a benchmark harness that
measures compression ratio + write speed + read speed for the same
data on all three.

[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Why

The Parquet/ORC/Avro spec PDFs are dense, and the production libraries
(arrow, orc, fastavro) hide the trade-offs under thousands of lines of
metadata code. This lab implements the *shape* of each format in ~150
lines per file so you can see, by eye, **what each format actually
optimises for**:

- **Parquet** — row groups + per-chunk stats at the footer + smart
  encoding (dictionary for low-cardinality, RLE for runs, plain
  otherwise). Best at "scan a subset of columns".
- **ORC** — stripes with a *leading* stats index so a reader can skip
  whole stripes without seeking to the end of the file. Best when
  the file lives behind a slow seek (object storage).
- **Avro** — row-by-row with a one-shot schema header. Best when the
  query reads every column of a few records.

The encodings (RLE, dictionary, delta, plain) are standalone modules
and tested in isolation so you can swap them between formats.

## Install

```bash
pip install -e ".[dev]"
```

Python 3.10+. **Zero runtime dependencies** — gzip is in the stdlib.

## CLI

```bash
povactl info
povactl bench --rows 2000 --seed 0
```

Example output:

```
rows=2000
format     bytes    write_s    read_s
parquet     6234     0.0084    0.0091
orc         8731     0.0061    0.0058
avro        9420     0.0034    0.0040
smallest = parquet
fast_read = avro
fast_write = avro
```

## Library

```python
from pova.columnar.column   import Column, ColumnType
from pova.columnar.schema   import Schema
from pova.formats.parquet_like import parquet_write, parquet_read
from pova.formats.orc_like     import orc_write, orc_read
from pova.formats.avro_like    import avro_write, avro_read
from pova.bench             import run_benchmark

schema = Schema(fields=(
    ("id", ColumnType.INT64),
    ("category", ColumnType.STRING),
))
columns = [
    Column("id", ColumnType.INT64, tuple(range(1_000))),
    Column("category", ColumnType.STRING,
           tuple(["A", "B", "C", "A"][i % 4] for i in range(1_000))),
]

# Round-trip through any format:
buf = parquet_write(schema, columns, row_group_size=128)
schema_back, columns_back = parquet_read(buf)

# Compare all three on the same input:
result = run_benchmark(schema, columns)
print(result.best_compression())  # "parquet"
```

## Predicate pushdown

```python
from pova.pushdown   import Predicate, Op, can_skip_row_group
from pova.stats.column import ColumnStats

stats = ColumnStats.from_values([10, 12, 13, 16, 100])
assert can_skip_row_group(Predicate("amount", Op.LT, 5), stats)   # 5 < 10 → skip
assert not can_skip_row_group(Predicate("amount", Op.EQ, 12), stats)
```

Real Parquet/ORC readers wire the same logic against the per-chunk
footer: load the metadata, evaluate the predicate against each chunk's
stats, skip the chunks the predicate proves are unsatisfiable.

## Components

| Module                          | Role                                                                |
| ------------------------------- | ------------------------------------------------------------------- |
| `pova.columnar.column`          | `Column` + `ColumnType` (int64/float64/string/bool, nulls allowed)  |
| `pova.columnar.schema`          | `Schema(fields=…).validate(columns)`                                 |
| `pova.encoding.plain`           | Length-prefixed plain encoder (baseline)                            |
| `pova.encoding.rle`             | Run-length encoder                                                  |
| `pova.encoding.dictionary`      | Low-cardinality dictionary encoder                                   |
| `pova.encoding.delta`           | Monotone-integer delta encoder                                       |
| `pova.stats.column`             | `ColumnStats.from_values` (min/max/null_count/n_rows)               |
| `pova.pushdown`                 | `Predicate` + `can_skip_row_group`                                  |
| `pova.formats.parquet_like`     | Row-group writer/reader with per-chunk encoding heuristics          |
| `pova.formats.orc_like`         | Stripe writer/reader with leading per-stripe stats index            |
| `pova.formats.avro_like`        | Row-oriented writer/reader with schema-once header                  |
| `pova.bench`                    | `run_benchmark(schema, columns)` → `BenchmarkResult`                |
| `pova.cli`                      | `povactl info | bench`                                              |

## Quality

```bash
make test       # 54 tests, 2 Hypothesis properties
make type       # mypy --strict
make lint
```

- **54 tests**, 0 failing; 2 Hypothesis properties
  (plain encode/decode round-trip on int64, dictionary round-trip on
  low-cardinality strings).
- mypy `--strict` clean over 18 source files; ruff clean.
- Multi-stage slim Docker image, non-root `pova` user.
- Python 3.10 / 3.11 / 3.12 CI matrix.
- **Zero runtime dependencies** — gzip from stdlib.

## License

MIT — see [LICENSE](LICENSE).
