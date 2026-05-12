"""
Standalone benchmark tool: measure query performance before/after compaction.

Usage:
    python scripts/benchmark.py \
        --table-path spark-warehouse/events \
        --table-format delta \
        --query "SELECT COUNT(*) FROM delta.\`spark-warehouse/events\` WHERE region='us-east'"
        --runs 3
"""

import argparse
import logging
import time
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("benchmark")


def build_spark(table_format: str):
    from pyspark.sql import SparkSession
    builder = SparkSession.builder.appName("CompactionBenchmark")
    if table_format == "delta":
        builder = builder \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    elif table_format == "iceberg":
        builder = builder \
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.iceberg.spark.SparkSessionCatalog")
    return builder.getOrCreate()


def run_query(spark, sql: str, runs: int) -> dict:
    times = []
    rows = 0
    for i in range(runs):
        spark.catalog.clearCache()
        t0 = time.perf_counter()
        df = spark.sql(sql)
        rows = df.count()
        elapsed = time.perf_counter() - t0
        times.append(elapsed)
        logger.info("  Run %d/%d: %.3fs (%d rows)", i + 1, runs, elapsed, rows)
    times.sort()
    return {
        "min": round(times[0], 4),
        "median": round(times[len(times) // 2], 4),
        "max": round(times[-1], 4),
        "avg": round(sum(times) / len(times), 4),
        "rows": rows,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="Delta/Iceberg query benchmark")
    parser.add_argument("--table-path", required=True)
    parser.add_argument("--table-format", default="delta", choices=["delta", "iceberg"])
    parser.add_argument("--query", required=True, help="SQL to benchmark")
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args(argv)

    spark = build_spark(args.table_format)

    print(f"\nBenchmarking query ({args.runs} runs):")
    print(f"  {args.query}\n")

    stats = run_query(spark, args.query, args.runs)

    print("\nResults:")
    print(f"  Min:    {stats['min']:.3f}s")
    print(f"  Median: {stats['median']:.3f}s")
    print(f"  Max:    {stats['max']:.3f}s")
    print(f"  Avg:    {stats['avg']:.3f}s")
    print(f"  Rows:   {stats['rows']}")

    spark.stop()


if __name__ == "__main__":
    main()
