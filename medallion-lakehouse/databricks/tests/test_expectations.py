"""Unit tests for the expectation dictionaries.

These assert the SQL strings are well-formed and cover the right columns.
Full integration tests against a live DLT pipeline live in a separate job
that seeds a fixture Volume and runs the bundle against it.
"""

from __future__ import annotations

import pytest

from src.common.expectations import (
    CUSTOMER_SILVER,
    FCT_SALES_GOLD,
    ORDER_ITEM_SILVER,
    ORDER_SILVER,
    PRODUCT_SILVER,
)


@pytest.mark.parametrize(
    "rules,required_refs",
    [
        (CUSTOMER_SILVER, ["customer_id", "email", "created_at"]),
        (PRODUCT_SILVER, ["product_id", "unit_price", "category"]),
        (ORDER_SILVER, ["order_id", "customer_id", "order_date", "status"]),
        (ORDER_ITEM_SILVER, ["order_id", "product_id", "quantity", "line_total"]),
        (FCT_SALES_GOLD, ["customer_sk", "product_sk", "date_sk", "net_amount"]),
    ],
)
def test_expectations_reference_every_required_column(rules, required_refs):
    combined = " ".join(rules.values()).lower()
    for col in required_refs:
        assert col.lower() in combined, f"no rule references {col}"


@pytest.mark.parametrize(
    "rules",
    [CUSTOMER_SILVER, PRODUCT_SILVER, ORDER_SILVER, ORDER_ITEM_SILVER, FCT_SALES_GOLD],
)
def test_expectation_names_are_snake_case(rules):
    for name in rules:
        assert name.islower() and "-" not in name and " " not in name


def test_order_status_enum_matches_bronze_hint():
    expected = {"placed", "shipped", "delivered", "returned", "cancelled"}
    rule = ORDER_SILVER["status_known"]
    for status in expected:
        assert f"'{status}'" in rule
