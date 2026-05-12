"""Gold — fct_sales.

Grain: one row per order line. Surrogate keys joined from dims; facts are
additive (quantity, net_amount). Liquid clustering on the common filter keys.
"""

from __future__ import annotations

import dlt
from pyspark.sql import functions as F

from common.expectations import FCT_SALES_GOLD


@dlt.table(
    name="fct_sales",
    comment="Order-line sales fact.",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
    },
    cluster_by=["date_sk", "customer_sk"],
)
@dlt.expect_all_or_drop(FCT_SALES_GOLD)
def fct_sales():
    orders = dlt.read("silver_orders").alias("o")
    items = dlt.read("silver_order_items").alias("i")
    customers = dlt.read("dim_customer_current").alias("c")
    products = dlt.read("dim_product").alias("p")
    dates = dlt.read("dim_date").alias("d")

    return (
        items.join(orders, F.col("i.order_id") == F.col("o.order_id"), "inner")
        .join(customers, F.col("o.customer_id") == F.col("c.customer_id"), "left")
        .join(products, F.col("i.product_id") == F.col("p.product_id"), "left")
        .join(dates, F.date_format(F.col("o.order_date"), "yyyyMMdd").cast("int") == F.col("d.date_sk"), "left")
        .select(
            F.col("o.order_id"),
            F.col("i.product_id"),
            F.col("c.customer_sk"),
            F.col("p.product_sk"),
            F.col("d.date_sk"),
            F.col("o.order_date"),
            F.col("o.status").alias("order_status"),
            F.col("i.quantity"),
            F.col("i.unit_price"),
            F.col("i.line_total").alias("net_amount"),
        )
    )
