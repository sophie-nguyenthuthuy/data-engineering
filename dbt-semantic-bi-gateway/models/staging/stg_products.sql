with source as (
    select * from {{ source('ecommerce_raw', 'products') }}
),

renamed as (
    select
        id::varchar                             as product_id,
        trim(sku)                               as sku,
        trim(name)                              as product_name,
        lower(trim(category))                   as category,
        lower(trim(subcategory))                as subcategory,
        lower(trim(brand))                      as brand,

        cost_price::numeric(18, 4)              as cost_price_usd,
        list_price::numeric(18, 4)              as list_price_usd,

        -- Derived
        list_price::numeric(18, 4)
            - cost_price::numeric(18, 4)        as gross_margin_usd,
        case
            when list_price > 0
            then round(
                (list_price - cost_price) / list_price * 100,
                2
            )
            else null
        end                                     as gross_margin_pct,

        coalesce(is_active, true)               as is_active,
        coalesce(is_digital, false)             as is_digital,

        created_at::timestamp                   as created_at,
        updated_at::timestamp                   as updated_at,
        _loaded_at::timestamp                   as _loaded_at
    from source
)

select * from renamed
