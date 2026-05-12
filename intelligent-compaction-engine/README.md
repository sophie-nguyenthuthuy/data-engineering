# Intelligent Compaction & Partition Pruning Engine

A background service that analyzes query patterns, recommends and executes optimal Z-ordering/clustering, merges small files, and prunes obsolete partitions for **Delta Lake** and **Apache Iceberg** tables — with **zero query disruption**.

## Features

| Feature | Delta Lake | Iceberg |
|---|---|---|
| Small file compaction | `OPTIMIZE` per partition batch | `rewrite_data_files` (bin-pack) |
| Z-ordering / clustering | `OPTIMIZE ZORDER BY` | `rewrite_data_files` (sort) |
| Partition pruning | `DROP PARTITION` | `expire_snapshots` |
| Storage cleanup | `VACUUM` | `delete_orphan_files` |
| Query pattern analysis | SQL log parsing → column scores | same |
| Before/after benchmarks | median latency, file count | same |
| Prometheus metrics export | ✓ | ✓ |

## Architecture

```
Query Log (SQL strings / files)
         │
         ▼
 QueryPatternAnalyzer          ← scores columns by filter/join/group frequency
         │
         ▼
   TableAnalyzer               ← reads Delta/Iceberg metadata (file sizes, partition ages)
         │
         ▼
  CompactionPlanner            ← orders: compact → prune → zorder → vacuum
         │
    ┌────┼────────────┐
    ▼    ▼            ▼
FileCompactor  PartitionPruner  ZOrderOptimizer
    │              │                │
    └──────────────┴────────────────┘
                   │
            PerformanceMetrics     ← before/after query benchmarks + SQLite history
                   │
            CompactionScheduler    ← background daemon, cron-based, non-blocking
```

## Quick Start

### 1. Install

```bash
pip install -r requirements.txt
# For Iceberg support:
pip install pyiceberg
```

### 2. Generate test data (creates a fragmented Delta table)

```bash
python examples/generate_test_data.py
```

### 3. Run the full example

```bash
python examples/delta_lake_example.py
```

Sample output:
```
============================================================
  Intelligent Compaction & Partition Pruning Engine
  Delta Lake Example
============================================================

[1/5] Ingesting query patterns...
  Top Z-order candidates: ['event_date', 'region', 'user_id', 'event_type']

[2/5] Analyzing table health...
  Files:              4 382
  Small files:        3 847  (87.8% fragmentation)
  Avg file size:      3.1 MB
  Total size:         13.24 GB
  Stale partitions:   365
  Needs compaction:   True
  Needs pruning:      True

[3/5] Generating compaction plan...
  [events] Planned: compact, prune_partitions, zorder, vacuum
    [1] compact: 3847/4382 files below target size (87.8% fragmentation)
    [2] prune_partitions: 365 stale partitions detected
    [3] zorder: Columns ['event_date', 'region', 'user_id'] drive most filters
    [4] vacuum: Routine storage cleanup

[4/5] Running benchmark (before)...
  Avg query time (before): 8.412s

[5/5] Executing compaction plan...
  → Compacting small files...
  → Z-ordering on ['event_date', 'region', 'user_id']...
  → Pruning stale partitions...
  → Running VACUUM...

============================================================
  COMPACTION IMPACT REPORT: events
============================================================
  Query latency:  8.412s → 0.931s  (+88.9%)
  File count:     4382 → 104  (-97.6%)
  Avg file size:  3.1 MB → 130.2 MB
  Data size:      13.24 GB → 13.24 GB

  Per-query speedup:
    daily_revenue_by_region            +91.2%
    user_purchase_history              +87.4%
    recent_events_us_east              +88.1%
============================================================
```

### 4. Run as a background service

```bash
python scripts/run_service.py \
  --config config/default_config.yaml \
  --tables db.events:delta:spark-warehouse/events

# Trigger immediate run and exit:
python scripts/run_service.py \
  --config config/default_config.yaml \
  --tables db.events:delta:spark-warehouse/events \
  --run-now

# Dry-run (plan only, no writes):
python scripts/run_service.py ... --run-now --dry-run
```

### 5. Run the benchmark tool

```bash
python scripts/benchmark.py \
  --table-path spark-warehouse/events \
  --table-format delta \
  --query "SELECT COUNT(*) FROM delta.\`spark-warehouse/events\` WHERE region='us-east'" \
  --runs 5
```

## Configuration

Edit `config/default_config.yaml`:

```yaml
engine:
  target_file_size_mb: 128     # target file size after compaction
  small_file_size_mb: 32       # files smaller than this are candidates
  max_zorder_columns: 4        # max columns in ZORDER BY clause
  min_column_query_frequency: 3 # min score to include a column

scheduler:
  compaction_cron: "0 2 * * *"  # 2 AM daily
  pruning_cron: "0 3 * * 0"     # 3 AM Sunday
  max_concurrent_jobs: 2

pruning:
  stale_partition_days: 365     # archive partitions older than 1 year
  auto_archive_days: 730        # drop partitions older than 2 years
  dry_run: false

metrics:
  prometheus_port: 8000
  benchmark_runs: 3
  db_path: "compaction_metrics.db"
```

## Programmatic API

```python
from pyspark.sql import SparkSession
from compaction_engine import (
    QueryPatternAnalyzer, TableAnalyzer,
    CompactionPlanner, PerformanceMetrics,
)
from compaction_engine.metrics import BenchmarkQuery
from compaction_engine.scheduler import CompactionScheduler, TableRegistration

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 1. Feed query patterns
analyzer = QueryPatternAnalyzer()
analyzer.ingest_query("SELECT * FROM events WHERE region='us-east' AND event_date > '2024-01-01'", "events")

# 2. Schedule as a background service
scheduler = CompactionScheduler(spark, config={
    "target_file_size_mb": 128,
    "stale_partition_days": 365,
    "dry_run": False,
})
scheduler.register_table(TableRegistration(
    table_name="events",
    table_format="delta",
    table_path="spark-warehouse/events",
    benchmark_queries=[
        BenchmarkQuery("count_by_region", "SELECT region, COUNT(*) FROM events GROUP BY region"),
    ],
))
scheduler.start()  # runs in background thread
```

## How It Works

### Query Pattern Analysis

`QueryPatternAnalyzer` parses SQL using **sqlglot** and scores each column by role:

| Role | Weight | Rationale |
|---|---|---|
| `WHERE` filter | 3.0× | Data skipping has the largest impact |
| `JOIN` predicate | 2.0× | Co-location reduces shuffle |
| `GROUP BY` | 1.5× | Locality improves aggregation |
| `ORDER BY` | 1.0× | Sorting already benefits from Z-order |

### Z-Order Selection

Top-scored columns (up to `max_zorder_columns`) are passed to `OPTIMIZE ZORDER BY`. Because Z-order effectiveness degrades beyond ~4 columns (curse of dimensionality), the engine enforces the cap.

### Compaction Strategy

- **Batch by partition** — only rewrites partitions with the worst fragmentation first, minimising write amplification
- **Non-blocking** — Delta OPTIMIZE uses a copy-on-write protocol; readers see either the old or new files, never a partial state
- **Target file size** — configured via `spark.databricks.delta.optimize.maxFileSize`

### Partition Pruning

Partitions are identified as stale by parsing date literals from partition spec strings (e.g. `dt=2022-01-15`). Two thresholds:
- `stale_partition_days` → archive flag (logged, moved to cold storage in prod)
- `auto_archive_days` → physical drop via `ALTER TABLE DROP PARTITION`

### Performance Measurement

Each benchmark query is run `benchmark_runs` times with Spark cache cleared between runs. The **median** is used (not the mean) to avoid outliers from JIT warm-up or GC pauses. Results are stored in SQLite for trend analysis.

## Tests

```bash
pytest tests/ -v
```

The test suite uses mocked Spark sessions, so no cluster is required:

```
tests/test_analyzer.py     — SQL parsing, column scoring, log ingestion
tests/test_compactor.py    — compaction decisions, Delta/Iceberg SQL generation
tests/test_optimizer.py    — Z-order plan generation and execution
tests/test_pruner.py       — partition age detection, vacuum SQL, dry-run
tests/test_metrics.py      — benchmark runs, impact calculation, SQLite persistence
```

## Prometheus Metrics

When `prometheus_port` is configured, the engine exposes:

| Metric | Labels | Description |
|---|---|---|
| `compaction_query_latency_seconds` | `table`, `phase` | Benchmark query latency |
| `compaction_query_speedup_pct` | `table` | % improvement after compaction |
| `compaction_file_reduction_pct` | `table` | % reduction in file count |

## Requirements

- Python 3.9+
- PySpark 3.4+
- `delta-spark` 2.4+ (for Delta Lake)
- `pyiceberg` 0.5+ (for Iceberg, optional)
- Java 11 or 17 (for Spark)

## License

MIT
