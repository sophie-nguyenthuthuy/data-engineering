# Copy-on-Write vs Merge-on-Read Benchmark Engine

A self-contained benchmark framework that implements both **Copy-on-Write (CoW)** and **Merge-on-Read (MoR)** storage strategies from scratch over Parquet/Iceberg-style data, then automatically recommends the better strategy per table based on your workload.

## What it does

| Component | Description |
|---|---|
| **CoW engine** | Rewrites affected Parquet files on every mutation; reads scan the current file set with no merging |
| **MoR engine** | Appends mutations to a lightweight delta log; reads merge base files + deltas on the fly |
| **Workload generator** | Drives both engines with identical operation sequences (inserts, updates, deletes, scans) |
| **Query classifier** | Infers workload class (OLAP, OLTP, CDC, streaming, batch) from observed operation traces |
| **Compaction model** | Analytically estimates I/O cost, read amplification, and ROI of compaction |
| **Recommender** | Combines benchmark results + classification → per-table strategy recommendation |

## Architecture

```
src/cow_mor_bench/
├── engines/
│   ├── base.py          # StorageEngine ABC
│   ├── cow.py           # Copy-on-Write implementation
│   └── mor.py           # Merge-on-Read implementation
├── data/
│   ├── generator.py     # Synthetic data generation (orders / events / inventory)
│   └── schemas.py       # PyArrow schemas, TableMetadata, Snapshot, DataFile, DeltaFile
├── workload/
│   ├── patterns.py      # Workload profile definitions
│   ├── generator.py     # Operation sequence executor + WorkloadTrace
│   └── classifier.py    # Rule-based workload classifier
├── benchmark/
│   ├── runner.py        # Runs CoW + MoR in parallel, returns BenchmarkResult
│   └── metrics.py       # Metric comparison table builder
├── compaction/
│   └── model.py         # I/O cost model, amplification curve, break-even analysis
├── recommender/
│   └── engine.py        # Strategy recommendation engine
└── cli.py               # Click CLI
```

## Quick start

```bash
pip install -e ".[dev]"

# Run all 6 workload profiles on the orders schema
cow-mor-bench run

# Run specific profiles
cow-mor-bench run -p olap -p oltp -p cdc --schema orders --table-size 50000 --ops 80

# Get a recommendation without running a full benchmark
cow-mor-bench recommend \
  --write-ratio 0.4 \
  --update-fraction 0.15 \
  --avg-batch-rows 500 \
  --full-scan-ratio 0.2 \
  --point-read-ratio 0.6 \
  --data-gb 50 \
  --table-name my_table

# Show compaction amplification curve
cow-mor-bench compaction-model --data-gb 20 --bytes-per-delta-mb 10 --max-delta-files 40

# List available workload profiles
cow-mor-bench list-profiles
```

## Workload profiles

| Profile | Class | Writes | Reads | Typical use case |
|---|---|---|---|---|
| `olap` | OLAP heavy | 8% | 92% | Data warehouse fact tables |
| `oltp` | OLTP heavy | 80% | 20% | Transactional order tables |
| `mixed` | Mixed | 40% | 60% | General-purpose operational tables |
| `streaming` | Streaming ingest | 91% | 9% | Event streams, logs |
| `batch_update` | Batch update | 85% | 15% | Daily ETL, CDC reconciliation |
| `cdc` | CDC | 95% | 5% | Change-data-capture pipelines |

## How the recommendation works

```
WorkloadTrace → Classifier → WorkloadClass
                                  ↓
                     BenchmarkResult (CoW vs MoR timings)
                                  ↓
                     CompactionCostModel (ROI, amplification)
                                  ↓
                         StrategyRecommendation
                    (CoW | MoR | MoR+compaction)
```

1. The **classifier** extracts features from the operation trace (read/write ratio, batch sizes, scan selectivity) and maps them to a workload class.
2. The **benchmark result** provides measured latency and throughput for both strategies under that workload.
3. The **compaction model** analytically estimates whether triggering compaction is cost-effective given current delta file accumulation and read frequency.
4. The **recommender** combines all three signals into a final recommendation with confidence, reasoning, and compaction guidance.

## Iceberg-style metadata

Both engines maintain a lightweight Iceberg-inspired metadata layer:

- **Snapshots** — each write creates a new snapshot referencing the current set of data and delta files
- **DataFile** — tracks path, row count, byte size, and PK min/max for partition pruning
- **DeltaFile** — tracks path, byte size, commit timestamp, and operation counts
- **TableMetadata** — persists the snapshot chain to `<table>/metadata/table.json`

## Running tests

```bash
pytest tests/ -v
```

## Extending

- **Add a schema**: define a new `pa.Schema` in `data/schemas.py` and a generator in `data/generator.py`
- **Add a workload profile**: add a `WorkloadProfile` entry in `workload/patterns.py`
- **Tune the classifier**: edit the scoring rules in `workload/classifier.py`
- **Custom cluster config**: pass a `ClusterConfig` to `estimate_compaction_cost` or `recommend`
