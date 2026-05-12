"""Gold — dim_date.

Pre-generated 2000-01-01 through 2050-12-31. Materialized once on first run
and left alone; no streaming source.
"""

from __future__ import annotations

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="dim_date",
    comment="Calendar dimension spanning 2000-2050.",
    table_properties={"quality": "gold"},
)
def dim_date():
    days = spark.range(0, 365 * 51).withColumnRenamed("id", "day_offset")
    return (
        days.select(
            F.date_add(F.lit("2000-01-01"), F.col("day_offset").cast("int")).alias("date")
        )
        .select(
            F.date_format("date", "yyyyMMdd").cast("int").alias("date_sk"),
            F.col("date"),
            F.year("date").alias("year"),
            F.quarter("date").alias("quarter"),
            F.month("date").alias("month"),
            F.date_format("date", "MMMM").alias("month_name"),
            F.dayofmonth("date").alias("day_of_month"),
            F.dayofweek("date").alias("day_of_week"),
            F.date_format("date", "EEEE").alias("day_name"),
            F.weekofyear("date").alias("iso_week"),
            (F.dayofweek("date").isin(1, 7)).alias("is_weekend"),
        )
    )
