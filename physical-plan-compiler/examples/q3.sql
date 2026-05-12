SELECT
    l_orderkey,
    SUM(l_extendedprice) AS revenue
FROM customer c
JOIN orders o ON c.c_custkey = o.o_custkey
JOIN lineitem l ON l.l_orderkey = o.o_orderkey
WHERE c.c_mktsegment = 'BUILDING'
GROUP BY l_orderkey
