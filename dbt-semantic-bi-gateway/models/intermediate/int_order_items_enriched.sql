with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select
        order_id,
        customer_id,
        order_status,
        order_date,
        order_month,
        is_cancelled,
        is_returned
    from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

enriched as (
    select
        oi.order_item_id,
        oi.order_id,
        oi.product_id,
        oi.quantity,
        oi.unit_price_usd,
        oi.discount_amount_usd,
        oi.gross_line_total_usd,
        oi.line_total_usd,

        -- Product attributes (denormalised for BI performance)
        p.sku,
        p.product_name,
        p.category,
        p.subcategory,
        p.brand,
        p.cost_price_usd,
        p.list_price_usd,
        p.is_digital,

        -- Derived financials
        (oi.quantity * p.cost_price_usd)::numeric(18, 4)           as cogs_usd,
        (oi.line_total_usd
            - (oi.quantity * p.cost_price_usd))::numeric(18, 4)    as gross_profit_usd,
        case
            when oi.line_total_usd > 0
            then round(
                (oi.line_total_usd - oi.quantity * p.cost_price_usd)
                / oi.line_total_usd * 100,
                2
            )
            else null
        end                                                         as gross_margin_pct,

        -- Order context
        o.customer_id,
        o.order_status,
        o.order_date,
        o.order_month,
        o.is_cancelled,
        o.is_returned
    from order_items oi
    left join orders o using (order_id)
    left join products p using (product_id)
)

select * from enriched
