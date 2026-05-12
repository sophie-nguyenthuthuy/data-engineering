"""
End-to-end example: Intelligent Compaction Engine on a Delta Lake table.

This example:
1. Builds a Spark session with Delta Lake support
2. Ingests sample query patterns
3. Runs a before-benchmark
4. Executes compaction + Z-ordering
5. Runs an after-benchmark and prints the impact report

Prerequisites:
    python examples/generate_test_data.py   # creates spark-warehouse/events
    pip install -r requirements.txt
"""

from pyspark.sql import SparkSession
from compaction_engine import (
    QueryPatternAnalyzer,
    TableAnalyzer,
    CompactionPlanner,
    FileCompactor,
    ZOrderOptimizer,
    PartitionPruner,
    PerformanceMetrics,
)
from compaction_engine.metrics import BenchmarkQuery
from compaction_engine.scheduler import CompactionScheduler, TableRegistration


TABLE_PATH = "spark-warehouse/events"
TABLE_NAME = "events"

BENCHMARK_QUERIES = [
    BenchmarkQuery(
        name="daily_revenue_by_region",
        sql=f"""
            SELECT region, SUM(revenue) as total_revenue
            FROM delta.`{TABLE_PATH}`
            WHERE event_date BETWEEN '2024-01-01' AND '2024-03-31'
            GROUP BY region
            ORDER BY total_revenue DESC
        """,
    ),
    BenchmarkQuery(
        name="user_purchase_history",
        sql=f"""
            SELECT event_id, event_date, event_type, revenue
            FROM delta.`{TABLE_PATH}`
            WHERE user_id = 12345 AND event_type = 'purchase'
            ORDER BY event_date DESC
            LIMIT 100
        """,
    ),
    BenchmarkQuery(
        name="recent_events_us_east",
        sql=f"""
            SELECT COUNT(*), event_type
            FROM delta.`{TABLE_PATH}`
            WHERE region = 'us-east' AND event_date >= '2024-10-01'
            GROUP BY event_type
        """,
    ),
]

SAMPLE_QUERIES = [
    f"SELECT * FROM events WHERE event_date = '2024-01-01'",
    f"SELECT * FROM events WHERE region = 'us-east' AND event_date > '2024-01-01'",
    f"SELECT region, SUM(revenue) FROM events WHERE event_date BETWEEN '2024-01-01' AND '2024-03-31' GROUP BY region",
    f"SELECT * FROM events WHERE user_id = 1234 AND event_type = 'purchase'",
    f"SELECT * FROM events WHERE event_date >= '2024-06-01' AND region = 'eu-central'",
    f"SELECT user_id, COUNT(*) FROM events WHERE event_date = '2024-05-15' GROUP BY user_id",
    f"SELECT * FROM events WHERE region = 'us-west' ORDER BY event_date DESC",
    f"SELECT * FROM events WHERE event_type = 'purchase' AND revenue > 100 AND event_date > '2023-01-01'",
]


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("IntelligentCompactionExample")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        # Allow VACUUM without the 7-day retention check (for demo purposes only)
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .getOrCreate()
    )


def main():
    print("=" * 60)
    print("  Intelligent Compaction & Partition Pruning Engine")
    print("  Delta Lake Example")
    print("=" * 60)

    spark = build_spark()

    # ---- 1. Ingest query patterns ----
    print("\n[1/5] Ingesting query patterns...")
    analyzer = QueryPatternAnalyzer(db_path="compaction_metrics.db")
    for sql in SAMPLE_QUERIES:
        analyzer.ingest_query(sql, table_name=TABLE_NAME)

    top_cols = analyzer.top_zorder_columns(TABLE_NAME, max_cols=4, min_frequency=1)
    print(f"  Top Z-order candidates: {top_cols}")

    # ---- 2. Analyze table health ----
    print("\n[2/5] Analyzing table health...")
    table_analyzer = TableAnalyzer(spark, config={"small_file_size_mb": 32})
    health = table_analyzer.analyze_delta_table(TABLE_PATH, query_analyzer=analyzer)
    print(f"  Files:              {health.total_files}")
    print(f"  Small files:        {health.small_files} ({health.fragmentation_ratio*100:.1f}%)")
    print(f"  Avg file size:      {health.avg_file_size_mb:.1f} MB")
    print(f"  Total size:         {health.total_size_gb:.2f} GB")
    print(f"  Stale partitions:   {health.stale_partition_count}")
    print(f"  Needs compaction:   {health.needs_compaction}")
    print(f"  Needs pruning:      {health.needs_pruning}")

    # ---- 3. Generate compaction plan ----
    print("\n[3/5] Generating compaction plan...")
    config = {
        "target_file_size_mb": 128,
        "small_file_size_mb": 32,
        "max_zorder_columns": 4,
        "min_column_query_frequency": 1,
    }
    planner = CompactionPlanner(spark, analyzer, config)
    plan = planner.plan(health)
    print(f"  {plan.summary()}")
    for action in plan.ordered_actions:
        print(f"    [{action.priority}] {action.action_type.value}: {action.reason}")

    # ---- 4. Run benchmark before ----
    print("\n[4/5] Running benchmark (before)...")
    metrics = PerformanceMetrics(
        spark, db_path="compaction_metrics.db", benchmark_runs=2
    )
    before = metrics.run_benchmark(
        TABLE_NAME,
        BENCHMARK_QUERIES,
        phase="before",
        file_count=health.total_files,
        avg_file_size_mb=health.avg_file_size_mb,
        total_size_gb=health.total_size_gb,
    )
    print(f"  Avg query time (before): {before.avg_query_time:.3f}s")

    # ---- 5. Execute compaction plan ----
    print("\n[5/5] Executing compaction plan...")
    from compaction_engine.planner import ActionType
    from compaction_engine.compactor import FileCompactor
    from compaction_engine.optimizer import ZOrderOptimizer
    from compaction_engine.pruner import PartitionPruner

    compactor = FileCompactor(spark, **{k: config[k] for k in ["target_file_size_mb", "small_file_size_mb"]})
    zorder_opt = ZOrderOptimizer(spark, analyzer, max_zorder_columns=4, min_column_frequency=1)
    pruner = PartitionPruner(spark, dry_run=False)

    for action in plan.ordered_actions:
        if action.action_type == ActionType.COMPACT:
            print("  → Compacting small files...")
            result = compactor.compact(health, dry_run=False)
            print(f"     {result.summary()}")

        elif action.action_type == ActionType.ZORDER and plan.zorder_plan:
            print(f"  → Z-ordering on {plan.zorder_plan.recommended_columns}...")
            zorder_result = zorder_opt.execute(plan.zorder_plan, dry_run=False)
            print(f"     Elapsed: {zorder_result.get('elapsed_seconds', '?')}s")

        elif action.action_type == ActionType.PRUNE_PARTITIONS:
            print("  → Pruning stale partitions...")
            prune_result = pruner.prune(health)
            print(f"     {prune_result.summary()}")

        elif action.action_type == ActionType.VACUUM:
            print("  → Running VACUUM...")
            pruner.vacuum(TABLE_NAME, "delta", TABLE_PATH)

    # ---- Post-compaction benchmark ----
    print("\n  Running benchmark (after)...")
    after_health = table_analyzer.analyze_delta_table(TABLE_PATH)
    after = metrics.run_benchmark(
        TABLE_NAME,
        BENCHMARK_QUERIES,
        phase="after",
        file_count=after_health.total_files,
        avg_file_size_mb=after_health.avg_file_size_mb,
        total_size_gb=after_health.total_size_gb,
    )

    impact = metrics.compare(before, after)
    print(impact.report())

    spark.stop()


if __name__ == "__main__":
    main()
