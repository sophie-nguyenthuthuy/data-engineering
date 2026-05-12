with orders as (
    select * from {{ ref('fct_orders') }}
    where not is_cancelled
),

items as (
    select * from {{ ref('fct_order_items') }}
    where not is_cancelled
),

order_daily as (
    select
        order_date                          as date_day,
        count(distinct order_id)            as total_orders,
        count(distinct customer_id)         as unique_customers,
        sum(gross_revenue_usd)              as gross_revenue_usd,
        sum(net_revenue_usd)                as net_revenue_usd,
        sum(discount_amount_usd)            as total_discounts_usd,
        sum(shipping_amount_usd)            as total_shipping_usd,
        sum(tax_amount_usd)                 as total_tax_usd,
        avg(net_revenue_usd)                as avg_order_value_usd,
        count(case when is_returned then 1 end) as returned_orders
    from orders
    group by order_date
),

items_daily as (
    select
        order_date                          as date_day,
        sum(cogs_usd)                       as total_cogs_usd,
        sum(gross_profit_usd)               as total_gross_profit_usd,
        sum(quantity)                       as total_units_sold
    from items
    group by order_date
),

joined as (
    select
        d.date_day,
        d.year_month,
        d.day_name_short,
        d.is_weekend,
        d.is_weekday,

        coalesce(od.total_orders, 0)            as total_orders,
        coalesce(od.unique_customers, 0)        as unique_customers,
        coalesce(od.gross_revenue_usd, 0)       as gross_revenue_usd,
        coalesce(od.net_revenue_usd, 0)         as net_revenue_usd,
        coalesce(od.total_discounts_usd, 0)     as total_discounts_usd,
        coalesce(od.total_shipping_usd, 0)      as total_shipping_usd,
        coalesce(od.total_tax_usd, 0)           as total_tax_usd,
        od.avg_order_value_usd,
        coalesce(od.returned_orders, 0)         as returned_orders,

        coalesce(id.total_cogs_usd, 0)          as total_cogs_usd,
        coalesce(id.total_gross_profit_usd, 0)  as total_gross_profit_usd,
        coalesce(id.total_units_sold, 0)        as total_units_sold,

        -- Margin %
        case
            when coalesce(od.net_revenue_usd, 0) > 0
            then round(
                id.total_gross_profit_usd / od.net_revenue_usd * 100,
                2
            )
        end                                     as gross_margin_pct
    from {{ ref('dim_dates') }} d
    left join order_daily od using (date_day)
    left join items_daily id using (date_day)
    where d.is_historical
)

select * from joined
