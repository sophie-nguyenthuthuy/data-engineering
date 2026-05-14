"""TPC-H mini query catalog.

Ten queries selected from the canonical TPC-H benchmark (Q1, Q3, Q4,
Q5, Q6, Q10, Q11, Q12, Q14, Q19) covering the common analytical
shapes: aggregation, multi-join, top-N, anti-join, correlated
subquery. SQL is the ANSI/ISO form so it runs unmodified on Postgres,
SQLite, DuckDB; the per-engine adapter swaps in dialect-specific
function names where needed.
"""

from __future__ import annotations

from pvc.workloads.base import Query, Workload


def _q(qid: str, desc: str, sql: str) -> Query:
    return Query(id=qid, description=desc, sql=sql.strip())


TPCH_QUERIES: Workload = Workload(
    name="tpch-mini",
    queries=(
        _q(
            "Q1",
            "Pricing summary report (group by status, aggregate)",
            """
            SELECT l_returnflag, l_linestatus,
                   SUM(l_quantity) AS sum_qty,
                   SUM(l_extendedprice) AS sum_base,
                   SUM(l_extendedprice * (1 - l_discount)) AS sum_disc,
                   AVG(l_quantity) AS avg_qty,
                   COUNT(*) AS count_order
              FROM lineitem
             GROUP BY l_returnflag, l_linestatus
             ORDER BY l_returnflag, l_linestatus
            """,
        ),
        _q(
            "Q3",
            "Shipping priority — 3-table join + top-10",
            """
            SELECT l_orderkey,
                   SUM(l_extendedprice * (1 - l_discount)) AS revenue,
                   o_orderdate, o_shippriority
              FROM customer, orders, lineitem
             WHERE c_mktsegment = 'BUILDING'
               AND c_custkey = o_custkey
               AND l_orderkey = o_orderkey
             GROUP BY l_orderkey, o_orderdate, o_shippriority
             ORDER BY revenue DESC, o_orderdate
             LIMIT 10
            """,
        ),
        _q(
            "Q4",
            "Order priority checking",
            """
            SELECT o_orderpriority, COUNT(*) AS order_count
              FROM orders
             WHERE EXISTS (SELECT 1 FROM lineitem
                            WHERE l_orderkey = o_orderkey)
             GROUP BY o_orderpriority
             ORDER BY o_orderpriority
            """,
        ),
        _q(
            "Q5",
            "Local supplier volume — 6-table join",
            """
            SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue
              FROM customer, orders, lineitem, supplier, nation, region
             WHERE c_custkey = o_custkey
               AND l_orderkey = o_orderkey
               AND l_suppkey = s_suppkey
               AND c_nationkey = s_nationkey
               AND s_nationkey = n_nationkey
               AND n_regionkey = r_regionkey
               AND r_name = 'ASIA'
             GROUP BY n_name
             ORDER BY revenue DESC
            """,
        ),
        _q(
            "Q6",
            "Forecasting revenue change — single-table scan + filter",
            """
            SELECT SUM(l_extendedprice * l_discount) AS revenue
              FROM lineitem
             WHERE l_discount BETWEEN 0.05 AND 0.07
               AND l_quantity < 24
            """,
        ),
        _q(
            "Q10",
            "Returned items reporting",
            """
            SELECT c_custkey, c_name,
                   SUM(l_extendedprice * (1 - l_discount)) AS revenue
              FROM customer, orders, lineitem, nation
             WHERE c_custkey = o_custkey
               AND l_orderkey = o_orderkey
               AND l_returnflag = 'R'
               AND c_nationkey = n_nationkey
             GROUP BY c_custkey, c_name
             ORDER BY revenue DESC
             LIMIT 20
            """,
        ),
        _q(
            "Q11",
            "Important stock identification — correlated subquery",
            """
            SELECT ps_partkey,
                   SUM(ps_supplycost * ps_availqty) AS value
              FROM partsupp, supplier, nation
             WHERE ps_suppkey = s_suppkey
               AND s_nationkey = n_nationkey
               AND n_name = 'GERMANY'
             GROUP BY ps_partkey
             HAVING SUM(ps_supplycost * ps_availqty) > 0
             ORDER BY value DESC
            """,
        ),
        _q(
            "Q12",
            "Shipping modes and order priority",
            """
            SELECT l_shipmode, COUNT(*) AS high_line_count
              FROM orders, lineitem
             WHERE o_orderkey = l_orderkey
               AND l_shipmode IN ('MAIL', 'SHIP')
             GROUP BY l_shipmode
             ORDER BY l_shipmode
            """,
        ),
        _q(
            "Q14",
            "Promotion effect — single-table conditional aggregate",
            """
            SELECT 100.0 * SUM(CASE WHEN p_type LIKE 'PROMO%'
                                    THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
                         / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
              FROM lineitem, part
             WHERE l_partkey = p_partkey
            """,
        ),
        _q(
            "Q19",
            "Discounted-revenue with deep predicate",
            """
            SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue
              FROM lineitem, part
             WHERE p_partkey = l_partkey
               AND (
                    (p_brand = 'Brand#12' AND l_quantity BETWEEN 1 AND 11)
                 OR (p_brand = 'Brand#23' AND l_quantity BETWEEN 10 AND 20)
                 OR (p_brand = 'Brand#34' AND l_quantity BETWEEN 20 AND 30)
               )
            """,
        ),
    ),
)


__all__ = ["TPCH_QUERIES"]
