"""Tests for the reliability scorer."""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from dce.contract import load_contract
from dce.scorer import ReliabilityStore
from dce.validator import ContractValidator, ValidationResult

CONTRACTS_DIR = Path(__file__).parent.parent / "contracts" / "examples"


@pytest.fixture()
def tmp_store(tmp_path):
    return ReliabilityStore(tmp_path / "test.db")


@pytest.fixture()
def orders_contract():
    return load_contract(CONTRACTS_DIR / "orders" / "v1.0.0.yaml")


def _make_result(contract, passed: bool) -> ValidationResult:
    df = pd.DataFrame({
        "order_id":    [f"ORD-{i:04d}" for i in range(200)],
        "customer_id": [f"CUST-{i % 50:04d}" for i in range(200)],
        "order_date":  ["2026-05-01T10:00:00Z"] * 200,
        "status":      ["confirmed"] * 200,
        "total_amount": [50.0 if passed else -1.0] * 200,
        "item_count":   [1] * 200,
        "discount_pct": [float("nan")] * 200,
    })
    return ContractValidator(contract).validate(df)


def test_record_and_score(tmp_store, orders_contract):
    for _ in range(8):
        tmp_store.record(_make_result(orders_contract, passed=True))
    for _ in range(2):
        tmp_store.record(_make_result(orders_contract, passed=False))

    score = tmp_store.score("order-service", "orders")
    assert score is not None
    assert score.total_runs == 10
    assert score.passed_runs == 8
    assert abs(score.reliability_score - 0.8) < 0.001


def test_no_runs_returns_none(tmp_store):
    assert tmp_store.score("ghost-producer", "ghost-contract") is None


def test_all_scores_empty(tmp_store):
    assert tmp_store.all_scores() == []


def test_all_scores_multiple_producers(tmp_store, orders_contract):
    tmp_store.record(_make_result(orders_contract, passed=True))
    scores = tmp_store.all_scores()
    assert len(scores) == 1
    assert scores[0].producer == "order-service"


def test_window_respected(tmp_store, orders_contract):
    for _ in range(50):
        tmp_store.record(_make_result(orders_contract, passed=True))
    for _ in range(50):
        tmp_store.record(_make_result(orders_contract, passed=False))

    # Window of last 10 — all failures (latest 50 failures come last)
    score = tmp_store.score("order-service", "orders", window=10)
    assert score.reliability_score == 0.0

    # Window of all 100
    score_full = tmp_store.score("order-service", "orders", window=100)
    assert abs(score_full.reliability_score - 0.5) < 0.001
