-- Fails if any order has ordered_at in the future (data-quality guardrail).
select order_id, ordered_at
from {{ ref('silver_orders') }}
where ordered_at > current_timestamp + interval '1 hour'
