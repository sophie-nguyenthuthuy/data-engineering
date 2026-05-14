# partitioning-strategy-advisor

Profile a SQL query log → recommend a partition column and a bucket
column + count, with cardinality and skew penalties baked in.

[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue.svg)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## What it does

Given a workload of SELECT queries, the advisor:

1. Extracts each query's **filter / join / group-by** columns with a
   lightweight tokeniser (no sqlglot dependency).
2. Aggregates per-column usage counts across the whole log.
3. Scores each column as a **partition candidate** —
   `filter_count − cardinality_penalty − skew_penalty`.
4. Picks the best **bucket column** from join keys and rounds the
   bucket count to a power-of-two ≈ `√estimated_distinct`, capped at
   1024.
5. Returns explicit recommendations with the reasoning trail.

## Components

| Module               | Role                                                              |
| -------------------- | ----------------------------------------------------------------- |
| `psa.parser`         | `parse_query(sql) → ParsedQuery(filter, join, group columns)`     |
| `psa.profile`        | `Profiler` aggregates usage counters into a `QueryProfile`        |
| `psa.cardinality`    | `estimate_cardinality(name, sample)` → Chao1 distinct estimate   |
| `psa.skew`           | `detect_skew(name, values)` → coefficient-of-variation + top-3 share |
| `psa.recommender`    | `recommend(profile, cardinalities, skews) → (Partition, Bucket)` |
| `psa.cli`            | `psactl info | profile | recommend`                              |

## Install

```bash
pip install -e ".[dev]"
```

Python 3.10+. **Zero runtime dependencies.**

## CLI

```bash
psactl info
psactl profile   --file query_log.sql
psactl recommend --file query_log.sql --target-partitions 200
```

Sample output:

```
partition_by = country   (filter_count=42, cardinality=200, no skew)
bucket_by    = customer_id   buckets=128   (join_count=18, est_distinct=12000)
```

## Library

```python
from psa.cardinality   import CardinalityEstimate
from psa.profile       import Profiler
from psa.recommender   import recommend
from psa.skew          import SkewReport

prof = Profiler()
prof.consume([
    "SELECT * FROM orders WHERE country = 'US' AND status IN ('shipped')",
    "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.country = 'CA'",
    "SELECT country, SUM(amount) FROM orders WHERE country IN ('US', 'CA') GROUP BY country",
])
profile = prof.build()

part, bucket = recommend(
    profile,
    cardinalities={
        "country": CardinalityEstimate("country", 1000, 50, 50),
        "customer_id": CardinalityEstimate("customer_id", 1000, 800, 1_000_000),
    },
    skews={
        "country": SkewReport("country", n=1000, distinct=50,
                              coefficient_of_variation=0.4, top_3_share=0.3),
    },
    target_partitions=200,
)
print(part.column, "—", part.reason)
print(bucket.column, "buckets=", bucket.bucket_count)
```

## Heuristics

**Partition pick** — choose the column maximising:

```
score = filter_count
        − 50 if estimated_distinct > 10 × target_partitions    (too granular)
        − 25 if column is skewed (CV ≥ 1 or top-3 share ≥ 0.5)
```

A column that's filtered often, has moderate cardinality, and isn't
skewed wins.

**Bucket pick** — most-joined column, with `next_pow2(√est_distinct)`
buckets, floor 8, cap 1024. Bucketing on a column you never join on
is useless, so the advisor refuses if `join_count == 0`.

## Quality

```bash
make test       # 35 tests, 1 Hypothesis property
make type       # mypy --strict
make lint
```

- **35 tests**, 0 failing; 1 Hypothesis property (parser is
  deterministic on the same input).
- mypy `--strict` clean over 7 source files; ruff clean.
- Multi-stage slim Docker image, non-root `psa` user.
- Python 3.10 / 3.11 / 3.12 CI matrix.
- **Zero runtime dependencies.**

## License

MIT — see [LICENSE](LICENSE).
