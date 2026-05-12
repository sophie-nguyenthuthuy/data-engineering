with customers as (
    select * from {{ ref('stg_customers') }}
),

order_summary as (
    select * from {{ ref('int_customer_order_summary') }}
),

joined as (
    select
        c.customer_id,
        c.email,
        c.first_name,
        c.last_name,
        c.full_name,
        c.country_code,
        c.acquisition_channel,
        c.is_email_verified,
        c.is_marketing_subscribed,
        c.first_seen_at,

        -- Order behaviour
        coalesce(os.total_orders, 0)            as total_orders,
        coalesce(os.completed_orders, 0)        as completed_orders,
        coalesce(os.returned_orders, 0)         as returned_orders,
        coalesce(os.gross_revenue_usd, 0)       as gross_revenue_usd,
        coalesce(os.net_revenue_usd, 0)         as net_revenue_usd,
        os.avg_order_value_usd,
        os.first_order_at,
        os.last_order_at,
        os.first_order_date,
        os.last_order_date,
        os.days_since_last_order,
        os.acquisition_cohort,
        os.customer_segment,
        os.recency_segment,

        -- LTV tier
        case
            when coalesce(os.net_revenue_usd, 0) >= 5000   then 'platinum'
            when coalesce(os.net_revenue_usd, 0) >= 1000   then 'gold'
            when coalesce(os.net_revenue_usd, 0) >= 250    then 'silver'
            else 'bronze'
        end                                     as ltv_tier,

        -- Flag: ever purchased
        os.total_orders is not null             as has_purchased
    from customers c
    left join order_summary os using (customer_id)
)

select * from joined
