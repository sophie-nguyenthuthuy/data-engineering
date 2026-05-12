"""Reusable DLT expectation dictionaries.

Every silver/gold table references these so expectations stay consistent and
show up under the same name in the pipeline event log.
"""

from __future__ import annotations


CUSTOMER_SILVER = {
    "customer_id_not_null": "customer_id IS NOT NULL",
    "email_format_valid": "email RLIKE '^[^@]+@[^@]+\\\\.[^@]+$'",
    "created_at_reasonable": "created_at >= '2000-01-01' AND created_at <= current_timestamp()",
}

PRODUCT_SILVER = {
    "product_id_not_null": "product_id IS NOT NULL",
    "price_non_negative": "unit_price >= 0",
    "category_known": "category IN ('apparel','home','electronics','grocery','other')",
}

ORDER_SILVER = {
    "order_id_not_null": "order_id IS NOT NULL",
    "customer_id_not_null": "customer_id IS NOT NULL",
    "order_date_not_future": "order_date <= current_date()",
    "status_known": "status IN ('placed','shipped','delivered','returned','cancelled')",
}

ORDER_ITEM_SILVER = {
    "order_id_not_null": "order_id IS NOT NULL",
    "product_id_not_null": "product_id IS NOT NULL",
    "quantity_positive": "quantity > 0",
    "line_total_non_negative": "line_total >= 0",
}

FCT_SALES_GOLD = {
    "has_surrogate_keys": "customer_sk IS NOT NULL AND product_sk IS NOT NULL AND date_sk IS NOT NULL",
    "amount_non_negative": "net_amount >= 0",
}
