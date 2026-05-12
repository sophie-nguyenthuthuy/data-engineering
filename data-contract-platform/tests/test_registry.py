"""Tests for the contract registry."""

from pathlib import Path
import pytest
from dce.registry import ContractRegistry

CONTRACTS_DIR = Path(__file__).parent.parent / "contracts" / "examples"


@pytest.fixture()
def registry():
    return ContractRegistry(CONTRACTS_DIR)


def test_ids(registry):
    ids = registry.ids()
    assert "orders" in ids
    assert "user-events" in ids


def test_latest_is_highest_version(registry):
    latest = registry.latest("orders")
    assert latest.version == "2.0.0"


def test_get_specific_version(registry):
    c = registry.get("orders", "1.0.0")
    assert c.version == "1.0.0"


def test_get_missing_contract_raises(registry):
    with pytest.raises(KeyError):
        registry.get("nonexistent-contract")


def test_previous_version(registry):
    prev = registry.previous("orders", "2.0.0")
    assert prev is not None
    assert prev.version == "1.1.0"


def test_by_producer(registry):
    contracts = registry.by_producer("order-service")
    assert len(contracts) == 1
    assert contracts[0].id == "orders"


def test_all_latest(registry):
    all_latest = registry.all_latest()
    versions = {c.id: c.version for c in all_latest}
    assert versions["orders"] == "2.0.0"
