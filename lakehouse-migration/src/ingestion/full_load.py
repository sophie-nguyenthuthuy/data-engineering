"""Full historical load from a JDBC source into the Bronze Delta layer."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta import DeltaTable
import yaml
import logging

logger = logging.getLogger(__name__)


def read_jdbc_table(spark: SparkSession, cfg: dict, table_cfg: dict) -> DataFrame:
    src = cfg["source"]
    return (
        spark.read.format("jdbc")
        .option("url", src["url"])
        .option("dbtable", table_cfg["source_table"])
        .option("user", src["user"])
        .option("password", src["password"])
        .option("fetchsize", src.get("fetch_size", 50000))
        .option("numPartitions", cfg["ingestion"]["parallelism"])
        .load()
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_table", F.lit(table_cfg["source_table"]))
    )


def write_bronze(df: DataFrame, bronze_path: str, table_name: str, partition_cols: list[str]) -> None:
    target_path = f"{bronze_path}/{table_name}"
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.save(target_path)
    logger.info("Full load written to bronze: %s  rows=%d", target_path, df.count())


def run_full_load(spark: SparkSession, config_path: str = "config/env.yaml") -> None:
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    for table_cfg in cfg["tables"]:
        logger.info("Full load: %s", table_cfg["name"])
        df = read_jdbc_table(spark, cfg, table_cfg)
        write_bronze(df, cfg["layers"]["bronze"], table_cfg["name"], table_cfg.get("partition_by", []))
