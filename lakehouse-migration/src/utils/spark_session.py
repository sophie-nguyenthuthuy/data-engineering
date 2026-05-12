from pyspark.sql import SparkSession
from typing import Optional
import yaml


def get_spark(config_path: str = "config/env.yaml", app_name: Optional[str] = None) -> SparkSession:
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    sc = cfg["spark"]
    catalog_cfg = cfg["catalog"]

    builder = (
        SparkSession.builder
        .appName(app_name or sc["app_name"])
        .master(sc.get("master", "local[*]"))
        .config("spark.sql.shuffle.partitions", sc.get("shuffle_partitions", 200))
        .config("spark.driver.memory", sc.get("driver_memory", "2g"))
        .config("spark.executor.memory", sc.get("executor_memory", "4g"))
        # Delta Lake extensions
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Enable schema evolution
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )

    if catalog_cfg["type"] == "glue":
        builder = (
            builder
            .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config("spark.sql.catalog.glue_catalog.warehouse", catalog_cfg["warehouse_path"])
        )

    return builder.getOrCreate()
