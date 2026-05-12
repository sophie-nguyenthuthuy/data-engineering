with source as (
    select * from {{ source('ecommerce_raw', 'customers') }}
),

renamed as (
    select
        id::varchar                             as customer_id,
        lower(trim(email))                      as email,
        trim(first_name)                        as first_name,
        trim(last_name)                         as last_name,
        trim(first_name) || ' ' || trim(last_name) as full_name,
        upper(trim(country_code))               as country_code,
        lower(trim(acquisition_channel))        as acquisition_channel,

        -- Flags
        coalesce(is_verified, false)            as is_email_verified,
        coalesce(is_subscribed_to_marketing, false) as is_marketing_subscribed,

        -- Timestamps
        created_at::timestamp                   as first_seen_at,
        updated_at::timestamp                   as updated_at,

        _loaded_at::timestamp                   as _loaded_at
    from source
)

select * from renamed
