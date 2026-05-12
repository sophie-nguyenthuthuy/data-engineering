"""PySpark analysis asset: aggregate the Parquet slice per cohort."""

from dagster import AssetExecutionContext, asset
from pyspark.sql import functions as F

from orchestrator.assets.raw_events import hourly_partitions
from orchestrator.obs import track_asset
from orchestrator.resources import S3Resource
from orchestrator.spark import build_spark_session


@asset(
    partitions_def=hourly_partitions,
    group_name="analyze",
    description=(
        "Aggregate the hourly Parquet slice (event_type x status x country x "
        "device) with count, error count, and p95 / average latency."
    ),
)
def event_analysis(
    context: AssetExecutionContext,
    raw_events_parquet: dict,
    s3: S3Resource,
) -> list[dict]:
    with track_asset(context):
        return _analyze(context, raw_events_parquet, s3)


def _analyze(
    context: AssetExecutionContext,
    raw_events_parquet: dict,
    s3: S3Resource,
) -> list[dict]:
    key = raw_events_parquet["key"]
    uri = f"s3a://{raw_events_parquet['bucket']}/{key}"
    context.log.info("reading %s", uri)

    if raw_events_parquet["rows"] == 0:
        context.log.info("empty partition, skipping Spark job")
        return []

    spark = build_spark_session(s3, app_name=f"analysis-{context.partition_key}")
    try:
        df = spark.read.parquet(uri)
        agg = (
            df.groupBy("event_type", "status", "country", "device")
            .agg(
                F.count(F.lit(1)).alias("events"),
                F.sum(F.when(F.col("status") == "error", 1).otherwise(0)).alias("errors"),
                F.percentile_approx("latency_ms", 0.95).alias("p95_latency_ms"),
                F.avg("latency_ms").alias("avg_latency_ms"),
            )
        )
        out = [row.asDict(recursive=True) for row in agg.collect()]
    finally:
        spark.stop()

    context.add_output_metadata({"cohorts": len(out)})
    return out
