with source as (
    select * from {{ source('ecommerce_raw', 'events') }}
),

renamed as (
    select
        id::varchar                                 as event_id,
        session_id::varchar                         as session_id,
        customer_id::varchar                        as customer_id,
        lower(trim(event_type))                     as event_type,
        trim(page_url)                              as page_url,

        -- Parse JSON properties into typed columns
        properties::json ->> 'product_id'           as event_product_id,
        (properties::json ->> 'revenue')::numeric   as event_revenue_usd,
        properties::json ->> 'search_query'         as search_query,
        properties::json ->> 'referrer'             as referrer,

        -- Funnel stage classification
        case lower(trim(event_type))
            when 'page_view'        then 'awareness'
            when 'product_view'     then 'consideration'
            when 'add_to_cart'      then 'intent'
            when 'checkout_start'   then 'purchase'
            when 'order_complete'   then 'conversion'
            else 'other'
        end                                         as funnel_stage,

        occurred_at::timestamp                      as occurred_at,
        date_trunc('day', occurred_at)::date        as event_date,
        _loaded_at::timestamp                       as _loaded_at
    from source
)

select * from renamed
