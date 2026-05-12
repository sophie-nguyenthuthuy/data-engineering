SELECT
    o_orderstatus,
    COUNT(*) AS cnt,
    SUM(o_totalprice) AS total_revenue,
    AVG(o_totalprice) AS avg_price
FROM orders
WHERE o_totalprice > 100
GROUP BY o_orderstatus
