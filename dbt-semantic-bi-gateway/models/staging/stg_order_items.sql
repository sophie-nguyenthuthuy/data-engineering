with source as (
    select * from {{ source('ecommerce_raw', 'order_items') }}
),

renamed as (
    select
        id::varchar                                 as order_item_id,
        order_id::varchar                           as order_id,
        product_id::varchar                         as product_id,

        quantity::int                               as quantity,
        unit_price::numeric(18, 4)                  as unit_price_usd,
        coalesce(discount_amount, 0)::numeric(18, 4) as discount_amount_usd,

        -- Derived line totals
        (quantity * unit_price)::numeric(18, 4)     as gross_line_total_usd,
        (quantity * unit_price
            - coalesce(discount_amount, 0))::numeric(18, 4) as line_total_usd,

        created_at::timestamp                       as created_at,
        _loaded_at::timestamp                       as _loaded_at
    from source
)

select * from renamed
