"""Silver layer — type-cast, dedupe, enforce business expectations.

Each entity is a streaming table fed by `APPLY CHANGES INTO` keyed on the
business key so late-arriving updates mutate the row rather than duplicate it.
Bad rows are dropped and counted via `expect_or_drop`.
"""

from __future__ import annotations

import dlt
from pyspark.sql import functions as F

from common.expectations import (
    CUSTOMER_SILVER,
    ORDER_ITEM_SILVER,
    ORDER_SILVER,
    PRODUCT_SILVER,
)


@dlt.view
def _customers_staged():
    return (
        dlt.read_stream("bronze_customers")
        .withColumn("email", F.lower(F.trim("email")))
        .withColumn("customer_id", F.trim("customer_id"))
    )


dlt.create_streaming_table(
    name="silver_customers",
    comment="Cleansed customers, one row per active customer_id.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
    expect_all_or_drop=CUSTOMER_SILVER,
)

dlt.apply_changes(
    target="silver_customers",
    source="_customers_staged",
    keys=["customer_id"],
    sequence_by=F.col("_ingest_ts"),
    stored_as_scd_type=1,
    except_column_list=["_source_file", "_rescued_data"],
)


@dlt.view
def _products_staged():
    return (
        dlt.read_stream("bronze_products")
        .withColumn("product_id", F.trim("product_id"))
        .withColumn("category", F.lower(F.trim("category")))
    )


dlt.create_streaming_table(
    name="silver_products",
    comment="Cleansed products.",
    table_properties={"quality": "silver"},
    expect_all_or_drop=PRODUCT_SILVER,
)

dlt.apply_changes(
    target="silver_products",
    source="_products_staged",
    keys=["product_id"],
    sequence_by=F.col("_ingest_ts"),
    stored_as_scd_type=1,
    except_column_list=["_source_file", "_rescued_data"],
)


@dlt.view
def _orders_staged():
    return (
        dlt.read_stream("bronze_orders")
        .withColumn("status", F.lower(F.trim("status")))
    )


dlt.create_streaming_table(
    name="silver_orders",
    comment="Cleansed order headers.",
    table_properties={"quality": "silver"},
    expect_all_or_drop=ORDER_SILVER,
)

dlt.apply_changes(
    target="silver_orders",
    source="_orders_staged",
    keys=["order_id"],
    sequence_by=F.col("_ingest_ts"),
    stored_as_scd_type=1,
    except_column_list=["_source_file", "_rescued_data"],
)


@dlt.table(
    name="silver_order_items",
    comment="Cleansed line items. Append-only; de-dup via primary key.",
    table_properties={"quality": "silver"},
)
@dlt.expect_all_or_drop(ORDER_ITEM_SILVER)
def silver_order_items():
    return (
        dlt.read_stream("bronze_order_items")
        .dropDuplicates(["order_id", "product_id"])
        .drop("_source_file", "_rescued_data")
    )
