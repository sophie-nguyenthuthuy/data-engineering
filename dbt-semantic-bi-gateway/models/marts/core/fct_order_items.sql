with items as (
    select * from {{ ref('int_order_items_enriched') }}
)

select
    -- ── Keys ─────────────────────────────────────────────────────────────────
    order_item_id,
    order_id,
    product_id,
    customer_id,
    order_date,

    -- ── Product attributes (denormalised for BI query performance) ───────────
    sku,
    product_name,
    category,
    subcategory,
    brand,
    is_digital,

    -- ── Order context ────────────────────────────────────────────────────────
    order_status,
    order_month,
    is_cancelled,
    is_returned,

    -- ── Measures ─────────────────────────────────────────────────────────────
    quantity,
    unit_price_usd,
    discount_amount_usd,
    gross_line_total_usd,
    line_total_usd          as net_line_total_usd,
    cogs_usd,
    gross_profit_usd,
    gross_margin_pct
from items
