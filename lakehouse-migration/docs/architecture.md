# Architecture Decisions

## Format Choice: Delta Lake vs. Apache Iceberg

| Concern | Delta Lake | Apache Iceberg |
|---|---|---|
| Compute | Best with Spark / Databricks | Multi-engine (Spark, Flink, Trino, Athena) |
| Catalog | Unity Catalog / metastore | Nessie, Glue, REST catalog |
| Time travel API | `VERSION AS OF` / `TIMESTAMP AS OF` | `FOR SYSTEM_VERSION AS OF` |
| Schema evolution | Column mapping (name mode) | Hidden partitioning + schema evolution |
| CDF / CDC | Change Data Feed built-in | Equality delete files |
| Maturity | GA | GA (Apache top-level) |

**Decision**: default to **Delta Lake** (easier MERGE semantics, better Spark integration).  
Use **Iceberg** when multi-engine reads are required (e.g. Athena + Spark).

## Medallion Layers

### Bronze
- Append-only raw copies of source tables.
- No transformations except metadata columns (`_ingested_at`, `_source_table`).
- Partitioned by ingestion date to enable efficient pruning.
- Retained for 90 days (configurable).

### Silver
- Deduplicated, typed, SCD Type-2 versioned rows.
- ACID MERGE from Bronze on each incremental run.
- Change Data Feed enabled for downstream CDF consumers.

### Gold
- Business-facing aggregates and fact/dim tables.
- Full overwrite on each run (idempotent).
- Z-ORDER clustered for BI query patterns.

## Incremental Strategy

1. **Watermark-based**: reads rows where `updated_at > last_max(updated_at)`.
   - Simple, no source-side changes needed.
   - Risk: misses deletes. Mitigated by CDC (step 2).

2. **CDC with Debezium** (recommended for prod):
   - Debezium → Kafka → Spark Structured Streaming → Bronze (append) → Silver (MERGE).
   - Handles inserts, updates, and hard deletes.

## Compaction Policy

| Layer | OPTIMIZE frequency | VACUUM retention |
|---|---|---|
| Bronze | Weekly | 7 days |
| Silver | Daily | 7 days |
| Gold | After each Gold run | 2 days |
