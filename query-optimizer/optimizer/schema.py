"""
10-table star schema: one fact table + nine dimension tables.

Schema
------
  fact_sales          – central fact table  (10 M rows)
  dim_customer        – 500 K customers
  dim_product         – 50 K products
  dim_date            – 3 650 days (10 years)
  dim_store           – 1 000 stores
  dim_employee        – 20 K employees
  dim_supplier        – 5 000 suppliers
  dim_region          – 200 regions
  dim_category        – 500 categories
  dim_promotion       – 2 000 promotions

Foreign key join predicates connect each dimension to the fact table.
All joins are equi-joins on surrogate keys.
"""
from __future__ import annotations
from optimizer.histogram import StatsCatalog, TableStats, ColumnStats
from optimizer.expressions import Predicate


def build_star_schema() -> tuple[StatsCatalog, list[str], list[Predicate]]:
    """Return (catalog, table_names, join_predicates) for the star schema."""
    catalog = StatsCatalog()

    def _key_col(name: str, ndv: int) -> ColumnStats:
        return ColumnStats(name=name, num_distinct=ndv, min_val=1, max_val=ndv)

    # ---- Fact table ----
    fact = TableStats("fact_sales", row_count=10_000_000, avg_row_bytes=120)
    fact.add_column(_key_col("customer_id",  500_000))
    fact.add_column(_key_col("product_id",    50_000))
    fact.add_column(_key_col("date_id",        3_650))
    fact.add_column(_key_col("store_id",       1_000))
    fact.add_column(_key_col("employee_id",   20_000))
    fact.add_column(_key_col("supplier_id",    5_000))
    fact.add_column(_key_col("region_id",        200))
    fact.add_column(_key_col("category_id",      500))
    fact.add_column(_key_col("promotion_id",   2_000))
    catalog.register(fact)

    # ---- Dimension tables ----
    dims = [
        ("dim_customer",  500_000,  80,  "customer_id",  500_000),
        ("dim_product",    50_000,  60,  "product_id",    50_000),
        ("dim_date",        3_650,  40,  "date_id",        3_650),
        ("dim_store",       1_000,  70,  "store_id",       1_000),
        ("dim_employee",   20_000,  90,  "employee_id",   20_000),
        ("dim_supplier",    5_000,  75,  "supplier_id",    5_000),
        ("dim_region",        200,  50,  "region_id",        200),
        ("dim_category",      500,  45,  "category_id",      500),
        ("dim_promotion",   2_000,  55,  "promotion_id",   2_000),
    ]

    predicates: list[Predicate] = []
    tables: list[str] = ["fact_sales"]

    for table_name, nrows, row_bytes, pk_col, ndv in dims:
        ts = TableStats(table_name, row_count=nrows, avg_row_bytes=row_bytes)
        ts.add_column(_key_col(pk_col, ndv))
        catalog.register(ts)
        tables.append(table_name)

        # FK join: fact_sales.X_id = dim_X.X_id
        predicates.append(Predicate(
            left_table="fact_sales",
            left_col=pk_col,
            right_table=table_name,
            right_col=pk_col,
        ))

    return catalog, tables, predicates
