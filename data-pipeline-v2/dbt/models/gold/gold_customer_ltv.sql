select
    customer_id,
    customer_email,
    customer_country,
    min(ordered_at)                       as first_order_at,
    max(ordered_at)                       as last_order_at,
    count(*)                              as lifetime_orders,
    round(sum(amount_usd)::numeric, 2)    as lifetime_revenue_usd,
    round(avg(amount_usd)::numeric, 2)    as avg_order_value_usd
from {{ ref('silver_orders') }}
where status <> 'cancelled'
group by 1, 2, 3
