with recent as (
    select *
    from {{ ref('silver_orders') }}
    where status <> 'cancelled'
      and ordered_at >= (select max(ordered_at) - interval '30 days' from {{ ref('silver_orders') }})
)

select
    product_id,
    product_name,
    product_category,
    count(*)                                as orders_30d,
    sum(quantity)                           as units_30d,
    round(sum(amount_usd)::numeric, 2)      as revenue_30d_usd,
    round(avg(amount_usd)::numeric, 2)      as avg_order_value_usd
from recent
group by 1, 2, 3
order by revenue_30d_usd desc
