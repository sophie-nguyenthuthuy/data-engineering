"""Deequ-style data quality metrics on Spark — Lab 14 Part B.

Real Deequ is JVM/Scala. This script reproduces the four metric families
(completeness, uniqueness, distribution, size) on PySpark so the demo
runs in the same Spark container as the rest of the bootcamp.

Usage from host:
    docker exec -i spark python3 /opt/bitnami/spark/apps/dq_metrics.py

Or as part of a spark-submit if you need s3a credentials baked in:
    docker exec -u root spark bash -c \\
      "spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 \\
                    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \\
                    --conf spark.hadoop.fs.s3a.access.key=minio \\
                    --conf spark.hadoop.fs.s3a.secret.key=minio12345 \\
                    --conf spark.hadoop.fs.s3a.path.style.access=true \\
                    /opt/bitnami/spark/apps/dq_metrics.py"
"""

import sys
from pyspark.sql import SparkSession, functions as F


SILVER_PATH = "s3a://lakehouse/silver/orders/"


def banner(title: str) -> None:
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)


def main() -> int:
    spark = (
        SparkSession.builder
        .appName("dq-metrics-deequ-style")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio12345")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(SILVER_PATH)
    total = df.count()
    if total == 0:
        print(f"[ERROR] {SILVER_PATH} is empty. Run Lab 08 first.", file=sys.stderr)
        spark.stop()
        return 1

    banner(f"Loaded silver/orders — {total} rows")
    df.printSchema()

    # 1. COMPLETENESS — fraction of non-null per column.
    banner("1. COMPLETENESS (1.0 = no missing values)")
    completeness_cols = ["order_id", "order_timestamp", "quantity",
                         "unit_price", "order_status", "payment_method"]
    df.select([
        F.avg(F.when(F.col(c).isNotNull(), 1.0).otherwise(0.0)).alias(c)
        for c in completeness_cols
    ]).show(truncate=False)

    # 2. UNIQUENESS — distinct ratio + duplicate detection.
    banner("2. UNIQUENESS (rows == distinct_order_id ⇒ no duplicates)")
    df.select(
        F.count("*").alias("rows"),
        F.countDistinct("order_id").alias("distinct_order_id"),
        (F.countDistinct("order_id") / F.count("*")).alias("uniqueness_ratio"),
    ).show()

    # 3. DISTRIBUTION — central tendency + spread + percentiles for numerics.
    banner("3. DISTRIBUTION — quantity")
    df.select(
        F.min("quantity").alias("min"),
        F.max("quantity").alias("max"),
        F.avg("quantity").alias("avg"),
        F.stddev("quantity").alias("stddev"),
        F.expr("percentile_approx(quantity, 0.5)").alias("p50"),
        F.expr("percentile_approx(quantity, 0.95)").alias("p95"),
    ).show()

    banner("3. DISTRIBUTION — unit_price")
    df.select(
        F.min("unit_price").alias("min"),
        F.max("unit_price").alias("max"),
        F.avg("unit_price").alias("avg"),
        F.stddev("unit_price").alias("stddev"),
        F.expr("percentile_approx(unit_price, 0.5)").alias("p50"),
        F.expr("percentile_approx(unit_price, 0.95)").alias("p95"),
    ).show()

    # 4. CATEGORICAL DISTRIBUTION — value counts for low-cardinality fields.
    banner("4a. CATEGORICAL DISTRIBUTION — order_status")
    df.groupBy("order_status").count().orderBy(F.desc("count")).show(truncate=False)

    banner("4b. CATEGORICAL DISTRIBUTION — payment_method")
    df.groupBy("payment_method").count().orderBy(F.desc("count")).show(truncate=False)

    # 5. SIZE — overall row count for anomaly detection.
    banner("5. SIZE")
    print(f"row_count = {total}")
    print("(In production: compare to 7-day rolling avg; alert if drop > 30%.)")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
