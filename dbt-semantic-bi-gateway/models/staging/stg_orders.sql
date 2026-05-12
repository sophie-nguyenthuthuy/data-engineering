with source as (
    select * from {{ source('ecommerce_raw', 'orders') }}
),

renamed as (
    select
        id::varchar                              as order_id,
        customer_id::varchar                    as customer_id,
        lower(trim(status))                     as order_status,
        upper(trim(currency))                   as currency,
        amount::numeric(18, 4)                  as order_amount,

        -- Normalise to USD using a static multiplier seed (swap for live FX in prod)
        case upper(trim(currency))
            when 'USD' then amount::numeric(18, 4)
            when 'EUR' then amount::numeric(18, 4) * 1.08
            when 'GBP' then amount::numeric(18, 4) * 1.27
            when 'CAD' then amount::numeric(18, 4) * 0.74
            else amount::numeric(18, 4)
        end                                     as order_amount_usd,

        shipping_amount::numeric(18, 4)         as shipping_amount_usd,
        tax_amount::numeric(18, 4)              as tax_amount_usd,
        discount_amount::numeric(18, 4)         as discount_amount_usd,

        -- Derived flags
        lower(status) in ('cancelled')          as is_cancelled,
        lower(status) in ('returned', 'refunded') as is_returned,
        lower(status) = 'delivered'             as is_delivered,

        -- Timestamps
        created_at::timestamp                   as ordered_at,
        updated_at::timestamp                   as updated_at,
        shipped_at::timestamp                   as shipped_at,
        delivered_at::timestamp                 as delivered_at,

        -- Metadata
        _loaded_at::timestamp                   as _loaded_at
    from source
),

final as (
    select
        *,
        date_trunc('day', ordered_at)::date     as order_date,
        date_trunc('week', ordered_at)::date    as order_week,
        date_trunc('month', ordered_at)::date   as order_month,
        extract(year from ordered_at)::int      as order_year
    from renamed
)

select * from final
