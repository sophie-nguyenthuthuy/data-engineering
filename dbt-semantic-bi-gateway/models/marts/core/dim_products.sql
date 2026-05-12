with products as (
    select * from {{ ref('stg_products') }}
),

-- Pre-aggregate sold quantities for each product to enrich dimension
item_stats as (
    select
        product_id,
        sum(quantity)           as total_units_sold,
        sum(line_total_usd)     as total_revenue_usd,
        sum(gross_profit_usd)   as total_gross_profit_usd,
        count(distinct order_id) as total_orders
    from {{ ref('int_order_items_enriched') }}
    where not is_cancelled
    group by product_id
),

final as (
    select
        p.product_id,
        p.sku,
        p.product_name,
        p.category,
        p.subcategory,
        p.brand,
        p.cost_price_usd,
        p.list_price_usd,
        p.gross_margin_usd,
        p.gross_margin_pct,
        p.is_active,
        p.is_digital,
        p.created_at,

        -- Popularity stats
        coalesce(s.total_units_sold, 0)         as total_units_sold,
        coalesce(s.total_revenue_usd, 0)        as total_revenue_usd,
        coalesce(s.total_gross_profit_usd, 0)   as total_gross_profit_usd,
        coalesce(s.total_orders, 0)             as total_orders,

        -- Tier
        case
            when coalesce(s.total_revenue_usd, 0) >= 50000  then 'hero'
            when coalesce(s.total_revenue_usd, 0) >= 10000  then 'core'
            when coalesce(s.total_revenue_usd, 0) >= 1000   then 'niche'
            else 'tail'
        end                                     as revenue_tier
    from products p
    left join item_stats s using (product_id)
)

select * from final
