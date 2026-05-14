"""NY-taxi query catalog.

Five representative queries against the classic public NY-taxi trip
dataset — point lookups, time-window aggregates, top-N, joins. All
ANSI-SQL so the same text runs on every engine in the harness.
"""

from __future__ import annotations

from pvc.workloads.base import Query, Workload


def _q(qid: str, desc: str, sql: str) -> Query:
    return Query(id=qid, description=desc, sql=sql.strip())


NY_TAXI_QUERIES: Workload = Workload(
    name="ny-taxi",
    queries=(
        _q(
            "NYT-1",
            "Total ride count (single-table scan)",
            "SELECT COUNT(*) FROM trips",
        ),
        _q(
            "NYT-2",
            "Average fare by month of year",
            """
            SELECT EXTRACT(MONTH FROM pickup_ts) AS month,
                   AVG(fare_amount) AS avg_fare
              FROM trips
             WHERE pickup_ts >= '2023-01-01'
               AND pickup_ts <  '2024-01-01'
             GROUP BY EXTRACT(MONTH FROM pickup_ts)
             ORDER BY month
            """,
        ),
        _q(
            "NYT-3",
            "Top-10 pickup zones by ride count",
            """
            SELECT pickup_zone, COUNT(*) AS rides
              FROM trips
             GROUP BY pickup_zone
             ORDER BY rides DESC
             LIMIT 10
            """,
        ),
        _q(
            "NYT-4",
            "Average tip percentage on weekends",
            """
            SELECT AVG(tip_amount / NULLIF(fare_amount, 0)) AS avg_tip_pct
              FROM trips
             WHERE EXTRACT(DOW FROM pickup_ts) IN (0, 6)
            """,
        ),
        _q(
            "NYT-5",
            "Per-vendor share of revenue (join + group)",
            """
            SELECT v.vendor_name,
                   SUM(t.total_amount) AS revenue,
                   COUNT(*) AS rides
              FROM trips t
              JOIN vendors v ON v.vendor_id = t.vendor_id
             GROUP BY v.vendor_name
             ORDER BY revenue DESC
            """,
        ),
    ),
)


__all__ = ["NY_TAXI_QUERIES"]
