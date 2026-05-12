"""Gold: BI-ready aggregates powering the dashboard."""
from __future__ import annotations

import duckdb
from dagster import MaterializeResult, asset

from ..iceberg_utils import overwrite_table, scan


@asset(
    group_name="gold",
    compute_kind="duckdb",
    deps=["silver_listings"],
    description="Per-city listing counts, median price, median CHF/m².",
)
def gold_city_stats(context) -> MaterializeResult:
    df = scan("silver_listings")
    con = duckdb.connect()
    con.register("s", df)
    out = con.execute(
        """
        SELECT
          city,
          listing_type,
          COUNT(*) AS n_listings,
          MEDIAN(price) AS median_price,
          MEDIAN(price_per_m2) AS median_price_per_m2,
          AVG(rooms) AS avg_rooms
        FROM s
        GROUP BY 1, 2
        ORDER BY listing_type, median_price DESC
        """
    ).arrow()
    full = overwrite_table("gold_city_stats", out)
    return MaterializeResult(metadata={"rows": out.num_rows, "iceberg_table": full})


@asset(
    group_name="gold",
    compute_kind="duckdb",
    deps=["silver_listings"],
    description="Price-band distribution by city — drives the dashboard histogram.",
)
def gold_price_bands(context) -> MaterializeResult:
    df = scan("silver_listings")
    con = duckdb.connect()
    con.register("s", df)
    out = con.execute(
        """
        SELECT
          city,
          CASE
            WHEN price < 500000 THEN '<500k'
            WHEN price < 1000000 THEN '500k-1M'
            WHEN price < 2000000 THEN '1M-2M'
            WHEN price < 5000000 THEN '2M-5M'
            ELSE '5M+'
          END AS price_band,
          COUNT(*) AS n
        FROM s
        WHERE listing_type = 'buy'
        GROUP BY 1, 2
        ORDER BY 1, 2
        """
    ).arrow()
    full = overwrite_table("gold_price_bands", out)
    return MaterializeResult(metadata={"rows": out.num_rows, "iceberg_table": full})
