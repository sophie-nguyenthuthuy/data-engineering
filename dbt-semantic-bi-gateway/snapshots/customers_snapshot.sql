{% snapshot customers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

select
    customer_id,
    email,
    first_name,
    last_name,
    country_code,
    acquisition_channel,
    is_email_verified,
    is_marketing_subscribed,
    first_seen_at,
    updated_at
from {{ ref('stg_customers') }}

{% endsnapshot %}
