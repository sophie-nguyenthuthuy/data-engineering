"""
Spark job — heavy rollups that aren't cheap in SQL.

Reads the raw events from BigQuery (via the BigQuery connector) or directly
from GCS parquet (fallback), computes per-language-guess / per-hour rollups
with windowed aggregations, and writes back to BigQuery marts.

Submit:
  spark-submit \
    --master spark://spark-master:7077 \
    --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.41.0 \
    /opt/jobs/daily_rollups.py --start 2024-01-01 --end 2024-01-07
"""

from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession, functions as F, Window


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("gh_daily_rollups")
        .config("spark.sql.session.timeZone", "UTC")
        .config("credentialsFile", "/opt/gcp-key.json")
        .getOrCreate()
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    args = p.parse_args()

    project = os.environ["GCP_PROJECT_ID"]
    raw_ds = os.environ.get("BQ_DATASET_RAW", "gh_raw")
    marts_ds = os.environ.get("BQ_DATASET_MARTS", "gh_marts")

    spark = build_spark()

    events = (
        spark.read.format("bigquery")
        .option("table", f"{project}.{raw_ds}.events")
        .option("filter", f"event_hour >= TIMESTAMP('{args.start}') AND event_hour < TIMESTAMP_ADD(TIMESTAMP('{args.end}'), INTERVAL 1 DAY)")
        .load()
    )

    # Hourly rollup per repo — total events + a running 24h sum.
    hourly = (
        events
        .withColumn("hour_ts", F.date_trunc("hour", "event_hour"))
        .groupBy("hour_ts", F.col("repo.id").alias("repo_id"), F.col("repo.name").alias("repo_name"))
        .agg(
            F.count("*").alias("events"),
            F.sum(F.when(F.col("type") == "PushEvent", 1).otherwise(0)).alias("pushes"),
            F.sum(F.when(F.col("type") == "WatchEvent", 1).otherwise(0)).alias("stars"),
            F.approx_count_distinct("actor.id").alias("approx_unique_actors"),
        )
    )

    w24 = Window.partitionBy("repo_id").orderBy(F.col("hour_ts").cast("long")).rangeBetween(-86400, 0)
    rolling = hourly.withColumn("events_24h", F.sum("events").over(w24))

    (
        rolling.write.format("bigquery")
        .option("table", f"{project}.{marts_ds}.repo_hourly_rollup")
        .option("partitionField", "hour_ts")
        .option("partitionType", "HOUR")
        .option("clusteredFields", "repo_id")
        .option("writeMethod", "direct")
        .mode("overwrite")
        .save()
    )

    spark.stop()


if __name__ == "__main__":
    main()
