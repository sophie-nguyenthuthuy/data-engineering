"""Watermark-based incremental ingestion: Bronze append, Silver upsert (MERGE)."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta import DeltaTable
import yaml
import logging

logger = logging.getLogger(__name__)


def _last_watermark(spark: SparkSession, bronze_path: str, table_name: str, wm_col: str) -> str:
    """Return the max watermark value already in bronze, or epoch if table is empty."""
    try:
        return (
            spark.read.format("delta")
            .load(f"{bronze_path}/{table_name}")
            .agg(F.max(wm_col).cast("string"))
            .collect()[0][0]
            or "1970-01-01 00:00:00"
        )
    except Exception:
        return "1970-01-01 00:00:00"


def read_incremental_jdbc(
    spark: SparkSession, cfg: dict, table_cfg: dict, since: str
) -> DataFrame:
    src = cfg["source"]
    wm_col = cfg["ingestion"]["watermark_column"]
    query = f"(SELECT * FROM {table_cfg['source_table']} WHERE {wm_col} > '{since}') t"

    return (
        spark.read.format("jdbc")
        .option("url", src["url"])
        .option("dbtable", query)
        .option("user", src["user"])
        .option("password", src["password"])
        .option("fetchsize", src.get("fetch_size", 50000))
        .load()
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_table", F.lit(table_cfg["source_table"]))
    )


def append_bronze(df: DataFrame, bronze_path: str, table_name: str, partition_cols: list[str]) -> int:
    if df.isEmpty():
        logger.info("No new rows for %s — skipping bronze append", table_name)
        return 0

    writer = df.write.format("delta").mode("append")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(f"{bronze_path}/{table_name}")

    count = df.count()
    logger.info("Appended %d rows to bronze/%s", count, table_name)
    return count


def upsert_silver(
    spark: SparkSession,
    df: DataFrame,
    silver_path: str,
    table_name: str,
    primary_keys: list[str],
    scd_type: int = 1,
) -> None:
    """ACID MERGE into Silver. Supports SCD Type-1 (overwrite) and Type-2 (versioned rows)."""
    target_path = f"{silver_path}/{table_name}"

    if not DeltaTable.isDeltaTable(spark, target_path):
        df.write.format("delta").mode("overwrite").save(target_path)
        logger.info("Silver table created: %s", target_path)
        return

    target = DeltaTable.forPath(spark, target_path)
    merge_condition = " AND ".join(f"target.{k} = source.{k}" for k in primary_keys)

    if scd_type == 1:
        (
            target.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # SCD Type-2: soft-close existing row, insert new version
        updates_df = df.withColumn("_is_current", F.lit(True)).withColumn(
            "_valid_from", F.current_timestamp()
        ).withColumn("_valid_to", F.lit(None).cast("timestamp"))

        (
            target.alias("target")
            .merge(
                updates_df.alias("source"),
                f"{merge_condition} AND target._is_current = true",
            )
            .whenMatchedUpdate(set={"_is_current": "false", "_valid_to": F.current_timestamp()})
            .whenNotMatchedInsertAll()
            .execute()
        )
        updates_df.write.format("delta").mode("append").save(target_path)

    logger.info("Upserted silver/%s (SCD Type-%d)", table_name, scd_type)


def run_incremental(spark: SparkSession, config_path: str = "config/env.yaml") -> None:
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    wm_col = cfg["ingestion"]["watermark_column"]

    for table_cfg in cfg["tables"]:
        name = table_cfg["name"]
        logger.info("Incremental run: %s", name)

        since = _last_watermark(spark, cfg["layers"]["bronze"], name, wm_col)
        df = read_incremental_jdbc(spark, cfg, table_cfg, since)

        append_bronze(df, cfg["layers"]["bronze"], name, table_cfg.get("partition_by", []))
        upsert_silver(
            spark, df, cfg["layers"]["silver"], name,
            table_cfg["primary_keys"], table_cfg.get("scd_type", 1),
        )
