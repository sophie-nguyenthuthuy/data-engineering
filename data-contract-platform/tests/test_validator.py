"""Tests for the validation engine."""

import pytest
import pandas as pd
from pathlib import Path

from dce.contract import load_contract
from dce.validator import ContractValidator

CONTRACTS_DIR = Path(__file__).parent.parent / "contracts" / "examples"


@pytest.fixture()
def orders_contract():
    return load_contract(CONTRACTS_DIR / "orders" / "v1.0.0.yaml")


@pytest.fixture()
def good_df():
    return pd.DataFrame({
        "order_id":    [f"ORD-{i:04d}" for i in range(200)],
        "customer_id": [f"CUST-{i % 50:04d}" for i in range(200)],
        "order_date":  ["2026-05-01T10:00:00Z"] * 200,
        "status":      ["confirmed"] * 150 + ["shipped"] * 50,
        "total_amount": [float(i * 5 + 10) for i in range(200)],
        "item_count":   [i % 9 + 1 for i in range(200)],
        "discount_pct": [float("nan")] * 200,
    })


def test_valid_dataframe_passes(orders_contract, good_df):
    result = ContractValidator(orders_contract).validate(good_df)
    assert result.passed
    assert result.errors() == []


def test_missing_required_field(orders_contract, good_df):
    df = good_df.drop(columns=["order_id"])
    result = ContractValidator(orders_contract).validate(df)
    assert not result.passed
    rules = [i.rule for i in result.errors()]
    assert "schema.required_field" in rules


def test_null_violation(orders_contract, good_df):
    good_df.loc[0, "customer_id"] = None
    result = ContractValidator(orders_contract).validate(good_df)
    assert not result.passed
    assert any(i.rule == "schema.null_violation" for i in result.errors())


def test_invalid_status_value(orders_contract, good_df):
    good_df.loc[0, "status"] = "refunded"  # not in allowed_values
    result = ContractValidator(orders_contract).validate(good_df)
    assert not result.passed
    assert any(i.rule == "schema.constraint.allowed_values" for i in result.errors())


def test_negative_total_fails_semantic(orders_contract, good_df):
    good_df.loc[0, "total_amount"] = -5.0
    result = ContractValidator(orders_contract).validate(good_df)
    assert not result.passed
    assert any("semantic" in i.rule for i in result.errors())


def test_row_count_sla(orders_contract):
    tiny_df = pd.DataFrame({
        "order_id":    ["ORD-0001"],
        "customer_id": ["CUST-0001"],
        "order_date":  ["2026-05-01T10:00:00Z"],
        "status":      ["confirmed"],
        "total_amount": [50.0],
        "item_count":   [1],
        "discount_pct": [None],
    })
    result = ContractValidator(orders_contract).validate(tiny_df)
    assert not result.passed
    assert any("sla.minimum_rows" in i.rule for i in result.errors())


def test_freshness_sla_failure(orders_contract, good_df):
    # 25 hours → exceeds 86400s threshold
    result = ContractValidator(orders_contract).validate(
        good_df, freshness_seconds=90000
    )
    assert not result.passed
    assert any("freshness" in i.rule for i in result.errors())


def test_freshness_sla_pass(orders_contract, good_df):
    result = ContractValidator(orders_contract).validate(
        good_df, freshness_seconds=3600
    )
    assert result.passed


def test_unexpected_field_is_warning(orders_contract, good_df):
    good_df["ghost_column"] = "x"
    result = ContractValidator(orders_contract).validate(good_df)
    assert result.passed  # warnings don't fail
    assert any(i.rule == "schema.unexpected_field" for i in result.warnings())


def test_to_json_serialisable(orders_contract, good_df):
    import json
    result = ContractValidator(orders_contract).validate(good_df)
    parsed = json.loads(result.to_json())
    assert "passed" in parsed
