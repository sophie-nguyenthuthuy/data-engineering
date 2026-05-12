{{ config(materialized='incremental', unique_key='product_id') }}

select *
from {{ ref('bronze_products') }}

{% if is_incremental() %}
where updated_at > (select coalesce(max(updated_at), '1970-01-01') from {{ this }})
{% endif %}
