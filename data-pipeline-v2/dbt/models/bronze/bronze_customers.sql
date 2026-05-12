select
    id           as customer_id,
    lower(email) as email,
    full_name,
    country,
    created_at,
    updated_at,
    _ingested_at
from {{ source('raw', 'customers') }}
