"""Silver → Gold aggregation layer: fact and dimension materialisation."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import yaml
import logging

logger = logging.getLogger(__name__)


def build_fact_transactions(spark: SparkSession, silver_path: str) -> DataFrame:
    txn = spark.read.format("delta").load(f"{silver_path}/transactions").filter("_is_current = true")
    cust = spark.read.format("delta").load(f"{silver_path}/customers").filter("_is_current = true")
    prod = spark.read.format("delta").load(f"{silver_path}/products")

    return (
        txn.alias("t")
        .join(cust.alias("c"), "customer_id", "left")
        .join(prod.alias("p"), "product_id", "left")
        .select(
            "t.transaction_id",
            "t.event_date",
            "t.customer_id",
            "c.country_code",
            "t.product_id",
            "p.category",
            "t.amount",
            "t.currency",
            F.current_timestamp().alias("_updated_at"),
        )
    )


def build_daily_revenue(fact_df: DataFrame) -> DataFrame:
    return (
        fact_df.groupBy("event_date", "country_code", "category")
        .agg(
            F.sum("amount").alias("total_revenue"),
            F.count("transaction_id").alias("transaction_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .orderBy("event_date")
    )


def write_gold(df: DataFrame, gold_path: str, table_name: str, partition_cols: list[str] = None) -> None:
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(f"{gold_path}/{table_name}")
    logger.info("Gold table written: %s/%s", gold_path, table_name)


def run_gold(spark: SparkSession, config_path: str = "config/env.yaml") -> None:
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    silver = cfg["layers"]["silver"]
    gold = cfg["layers"]["gold"]

    fact_df = build_fact_transactions(spark, silver)
    write_gold(fact_df, gold, "fact_transactions", partition_cols=["event_date"])

    daily_df = build_daily_revenue(fact_df)
    write_gold(daily_df, gold, "daily_revenue", partition_cols=["event_date"])
