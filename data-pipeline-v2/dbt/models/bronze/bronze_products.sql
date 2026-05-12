select
    id           as product_id,
    sku,
    name         as product_name,
    category,
    price_cents,
    price_cents / 100.0 as price_usd,
    updated_at,
    _ingested_at
from {{ source('raw', 'products') }}
