#pragma once
#include <string>
#include <vector>
#include <utility>

namespace qc::tpch {

// TPC-H query definitions with expected column names and result shape
struct TpchQuery {
    std::string name;
    std::string sql;
};

// Q1: Pricing summary report — heavy scan+filter+agg on lineitem (~60% selectivity)
inline const TpchQuery Q1 = {"Q1 Pricing Summary",
R"(
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity)                                     AS sum_qty,
    SUM(l_extendedprice)                                AS sum_base_price,
    SUM(l_extendedprice * (1 - l_discount))             AS sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    AVG(l_quantity)                                     AS avg_qty,
    AVG(l_extendedprice)                                AS avg_price,
    AVG(l_discount)                                     AS avg_disc,
    COUNT(*)                                            AS count_order
FROM lineitem
WHERE l_shipdate <= date '1998-09-02'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
)"};

// Q6: Revenue change — single-table scan+filter+agg, no join, maximizes vectorization
inline const TpchQuery Q6 = {"Q6 Revenue Change",
R"(
SELECT
    SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= date '1994-01-01'
  AND l_shipdate < date '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 24.0
)"};

// Q12: Shipping mode impact — scan lineitem, filter, aggregate
inline const TpchQuery Q12 = {"Q12 Shipping Mode",
R"(
SELECT
    l_shipmode,
    SUM(l_extendedprice) AS total_price
FROM lineitem
WHERE l_shipmode = 'MAIL'
  AND l_shipdate >= date '1994-01-01'
  AND l_shipdate < date '1995-01-01'
GROUP BY l_shipmode
)"};

// Q_SCAN: Pure scan + count (baseline — measures scan throughput)
inline const TpchQuery Q_SCAN = {"Scan Throughput",
R"(
SELECT COUNT(*) AS cnt
FROM lineitem
WHERE l_discount > 0.0
)"};

// Queries used in the benchmark suite
inline const std::vector<TpchQuery> BENCHMARK_QUERIES = {Q6, Q12, Q_SCAN};

} // namespace qc::tpch
