"""Gold — dim_product (SCD1).

Product attributes overwrite in place. No history retained here; raw history
is in silver/bronze if anyone ever needs it.
"""

from __future__ import annotations

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="dim_product",
    comment="SCD1 product dimension with surrogate key.",
    table_properties={"quality": "gold"},
)
def dim_product():
    return (
        dlt.read("silver_products")
        .select(
            F.sha2(F.col("product_id"), 256).alias("product_sk"),
            "product_id",
            "category",
            "unit_price",
        )
    )
