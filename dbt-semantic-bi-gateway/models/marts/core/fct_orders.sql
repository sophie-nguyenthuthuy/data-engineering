with orders as (
    select * from {{ ref('int_orders_enriched') }}
),

final as (
    select
        -- ── Keys ─────────────────────────────────────────────────────────────
        order_id,
        customer_id,
        campaign_id,
        order_date,

        -- ── Descriptors ──────────────────────────────────────────────────────
        order_status,
        currency,
        country_code,
        acquisition_channel,
        campaign_channel,
        campaign_name,

        -- ── Measures ─────────────────────────────────────────────────────────
        order_amount_usd        as gross_revenue_usd,
        shipping_amount_usd,
        tax_amount_usd,
        discount_amount_usd,

        -- Net revenue = gross - discounts (tax/shipping excluded from revenue)
        (order_amount_usd - coalesce(discount_amount_usd, 0))
                                as net_revenue_usd,

        -- ── Flags ─────────────────────────────────────────────────────────────
        is_cancelled,
        is_returned,
        is_delivered,

        -- ── Timestamps ───────────────────────────────────────────────────────
        ordered_at,
        shipped_at,
        delivered_at,
        updated_at,
        order_week,
        order_month,
        order_year,

        -- ── Derived delivery SLA ──────────────────────────────────────────────
        case
            when delivered_at is not null and shipped_at is not null
            then extract(day from delivered_at - shipped_at)::int
        end                     as days_to_deliver
    from orders
)

select * from final
