select
    id              as order_id,
    customer_id,
    product_id,
    quantity,
    amount_cents,
    amount_cents / 100.0 as amount_usd,
    status,
    ordered_at,
    updated_at,
    _ingested_at
from {{ source('raw', 'orders') }}
