"""Bronze layer — Auto Loader streaming ingestion from a UC volume.

One streaming table per source entity. Schema inference is on with hints for
the fields we care about being typed correctly; everything else is captured
in a `_rescued_data` column so schema drift never drops a byte.
"""

from __future__ import annotations

import dlt
from pyspark.sql import functions as F


LANDING = spark.conf.get("landing_volume")


def _autoload(entity: str, schema_hints: str):
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{LANDING}/_schemas/{entity}")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", schema_hints)
        .option("header", "true")
        .load(f"{LANDING}/{entity}")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingest_ts", F.current_timestamp())
    )


@dlt.table(
    name="bronze_customers",
    comment="Raw customer records, one row per source event. Append-only.",
    table_properties={"quality": "bronze", "pipelines.reset.allowed": "false"},
)
def bronze_customers():
    return _autoload(
        "customers",
        schema_hints="customer_id STRING, email STRING, created_at TIMESTAMP",
    )


@dlt.table(
    name="bronze_products",
    comment="Raw product records, append-only.",
    table_properties={"quality": "bronze", "pipelines.reset.allowed": "false"},
)
def bronze_products():
    return _autoload(
        "products",
        schema_hints="product_id STRING, unit_price DECIMAL(10,2), category STRING",
    )


@dlt.table(
    name="bronze_orders",
    comment="Raw order headers, append-only.",
    table_properties={"quality": "bronze", "pipelines.reset.allowed": "false"},
)
def bronze_orders():
    return _autoload(
        "orders",
        schema_hints="order_id STRING, customer_id STRING, order_date DATE, status STRING",
    )


@dlt.table(
    name="bronze_order_items",
    comment="Raw order line items, append-only.",
    table_properties={"quality": "bronze", "pipelines.reset.allowed": "false"},
)
def bronze_order_items():
    return _autoload(
        "order_items",
        schema_hints=(
            "order_id STRING, product_id STRING, quantity INT, "
            "unit_price DECIMAL(10,2), line_total DECIMAL(12,2)"
        ),
    )
