{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='append_new_columns'
) }}

with src as (
    select *
    from {{ ref('bronze_orders') }}
    {% if is_incremental() %}
    where updated_at > (select coalesce(max(updated_at), '1970-01-01') from {{ this }})
    {% endif %}
),

dedup as (
    -- Guard against duplicate ingestion: keep most-recently-updated row per order_id.
    select *
    from (
        select
            src.*,
            row_number() over (partition by order_id order by updated_at desc) as rn
        from src
    ) t
    where rn = 1
),

joined as (
    select
        o.order_id,
        o.customer_id,
        o.product_id,
        c.email           as customer_email,
        c.country         as customer_country,
        p.product_name,
        p.category        as product_category,
        o.quantity,
        o.amount_usd,
        o.status,
        o.ordered_at,
        o.updated_at
    from dedup o
    left join {{ ref('silver_customers') }} c using (customer_id)
    left join {{ ref('silver_products')  }} p using (product_id)
)

select * from joined
