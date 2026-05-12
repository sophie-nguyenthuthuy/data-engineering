with orders as (
    select * from {{ ref('stg_orders') }}
    where not is_cancelled
),

customer_stats as (
    select
        customer_id,

        -- Order counts
        count(*)                                            as total_orders,
        count(case when not is_returned then 1 end)         as completed_orders,
        count(case when is_returned then 1 end)             as returned_orders,

        -- Revenue
        sum(order_amount_usd)                               as gross_revenue_usd,
        sum(case when not is_returned
            then order_amount_usd else 0 end)               as net_revenue_usd,
        avg(order_amount_usd)                               as avg_order_value_usd,
        max(order_amount_usd)                               as max_order_value_usd,

        -- Discounts
        sum(discount_amount_usd)                            as total_discounts_usd,

        -- Dates
        min(ordered_at)                                     as first_order_at,
        max(ordered_at)                                     as last_order_at,
        min(order_date)                                     as first_order_date,
        max(order_date)                                     as last_order_date,

        -- Recency (days since last order — populated at query time via current_date)
        extract(day from now() - max(ordered_at))::int      as days_since_last_order
    from orders
    group by customer_id
),

with_cohorts as (
    select
        *,
        date_trunc('month', first_order_date)   as acquisition_cohort,
        case
            when total_orders = 1               then 'one_time'
            when total_orders between 2 and 4   then 'repeat'
            when total_orders >= 5              then 'loyal'
        end                                     as customer_segment,
        case
            when days_since_last_order <= 30    then 'active'
            when days_since_last_order <= 90    then 'at_risk'
            else 'churned'
        end                                     as recency_segment
    from customer_stats
)

select * from with_cohorts
