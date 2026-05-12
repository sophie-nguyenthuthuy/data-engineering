# physical-plan-compiler

A cross-engine **physical-plan compiler**: takes a logical pipeline DAG and a catalogue of physical operators (Spark, dbt, Flink, DuckDB), then searches the physical-plan space with a **cost-based Cascades-style planner** guided by a **learned cost model**. Output: an executable plan with cross-engine data conversions inserted automatically. Essentially a query optimizer whose operators are entire frameworks.

> **Status:** Design / spec phase. Generalises [`pipeline-topology-compiler`](../pipeline-topology-compiler/) (which targets one engine at a time) into a multi-engine cost-based planner.

## The problem

Today, choosing an engine per step is a human guess: "this aggregation is small — dbt; this join is huge — Spark; that windowed thing — Flink". The wrong choice costs 5–10× in runtime + dollar. And the *combinations* (e.g., Spark stage → dbt stage requires materialising to a warehouse table) carry hidden costs no one models.

This compiler:

1. Treats each engine as a set of typed physical operators with cost annotations.
2. Inserts data-format conversion operators (Parquet ↔ warehouse table ↔ Kafka topic ↔ DuckDB local file) automatically when crossing engines.
3. Searches plans with **memoization (Cascades)** + **dominance pruning**.
4. Picks the cost-minimal plan.

## Architecture

```
Logical DAG
   │
   ▼
┌────────────────────────────────────────────┐
│  Plan space generator                      │
│  (per-node: candidate physical operators   │
│   from each engine)                        │
└────────────┬───────────────────────────────┘
             │
             ▼
┌────────────────────────────────────────────┐
│  Cascades search                           │
│  - memo table (group, expr, props)         │
│  - branch on operator choice + interesting │
│    physical properties (partitioning,      │
│    sort order, ...)                        │
│  - prune via dominance                     │
└────────────┬───────────────────────────────┘
             │
             ▼
┌────────────────────────────────────────────┐
│  Learned cost model                        │
│  (per-op-shape NN: features →              │
│   {runtime, data_scanned_bytes, $cost})    │
└────────────┬───────────────────────────────┘
             │
             ▼
┌────────────────────────────────────────────┐
│  Plan emitter                              │
│  - emits: Spark jobs, dbt models, Flink    │
│    jobs, DuckDB scripts                    │
│  - inserts data-conversion ops             │
│  - emits orchestration manifest (Dagster)  │
└────────────────────────────────────────────┘
```

## Components

| Module | Role |
|---|---|
| `src/logical/` | Logical operators: filter, project, aggregate, join, window, sort, union |
| `src/physical/spark/` | Physical operators implementable in Spark |
| `src/physical/dbt/` | Physical operators implementable in dbt (warehouse SQL) |
| `src/physical/flink/` | Physical operators implementable in Flink |
| `src/physical/duckdb/` | Physical operators implementable in DuckDB |
| `src/physical/convert/` | Cross-engine conversion ops (with cost) |
| `src/cascades/` | Memo, group, expression, rule-firing engine |
| `src/cost_model/` | NN-based cost predictor + training pipeline |
| `src/emitter/` | Generates engine-native code + Dagster manifest |

## Cross-engine conversions

| From | To | Conversion op | Approx cost |
|---|---|---|---|
| Spark DataFrame | dbt source | `.write.parquet(s3) + register_external_table` | seconds + storage |
| dbt model | Flink source | `kafka_connect_jdbc + flink_table_source` | minutes (initial) |
| DuckDB local | Spark | `export_parquet → spark.read.parquet` | fast |
| Flink stream | dbt source | `kafka → S3 sink → external table` | ongoing $$ |

Conversion costs are explicit nodes in the plan space; the planner sometimes picks an "inferior" engine *because* it avoids an expensive conversion.

## Learned cost model

Features per physical-op instance:
- input cardinality (estimated)
- input bytes
- predicate selectivities
- join key cardinality
- output cardinality
- engine + op type

Predicts: runtime + bytes scanned + $ cost.

Trained offline on logged production runs. Periodically retrained.

## Cascades search

Each logical operator maps to a *group* in the memo table. Each group has multiple physical implementations. Search applies transformation rules:

- `LogicalJoin → PhysicalHashJoin(Spark) | PhysicalSortMergeJoin(Spark) | PhysicalJoin(dbt) | PhysicalKeyedStreamJoin(Flink)`
- `LogicalAggregate → PhysicalHashAggregate(Spark) | PhysicalAggregate(dbt) | PhysicalKeyedAggregate(Flink) | PhysicalAggregate(DuckDB)`

Each candidate has *interesting properties* (output partitioning, output engine, output format). Properties propagate up; conversions inserted when child's output property doesn't match parent's required input property.

Pruning: a plan is *dominated* if there exists another plan with same logical output, all interesting properties ≥, and cost ≤. Dominated plans deleted.

## Example output

```yaml
# Dagster manifest emitted by the compiler
pipeline: customer_360_daily
ops:
  - id: raw_events_kafka_to_s3
    engine: kafka_connect
    config: {sink_format: parquet, partition_by: [date]}

  - id: enrich_events
    engine: spark
    depends_on: [raw_events_kafka_to_s3]
    sql: SELECT e.*, c.tier FROM events e JOIN customers c ...

  - id: daily_rollup
    engine: dbt
    depends_on: [enrich_events]
    materialization: incremental
    sql: SELECT date, customer_id, SUM(amount) FROM enriched_events ...

  - id: rev_etl_push
    engine: duckdb
    depends_on: [daily_rollup]
    source: warehouse://rollup
    sink: https://salesforce.api/...
```

## References

- Graefe, "The Cascades Framework for Query Optimization" (Data Eng. Bulletin 1995)
- Calcite (Apache) — open Cascades implementation
- Marcus et al., "Bao: Making Learned Query Optimization Practical" (SIGMOD 2021)

## Roadmap

- [ ] Logical operator IR
- [ ] Physical operators per engine + properties
- [ ] Conversion ops + cost
- [ ] Cascades memo + rule engine
- [ ] Learned cost model (trained on synthetic + logged plans)
- [ ] Code emitters (Spark, dbt, Flink, DuckDB)
- [ ] Dagster manifest emitter
- [ ] Bench: vs. handwritten plans on TPC-DS portions
