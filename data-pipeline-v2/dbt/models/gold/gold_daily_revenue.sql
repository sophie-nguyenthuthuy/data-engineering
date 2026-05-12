select
    date_trunc('day', ordered_at)::date       as order_date,
    product_category,
    count(*)                                   as order_count,
    count(distinct customer_id)                as unique_customers,
    sum(quantity)                              as units_sold,
    round(sum(amount_usd)::numeric, 2)         as revenue_usd
from {{ ref('silver_orders') }}
where status <> 'cancelled'
group by 1, 2
order by 1 desc, 2
