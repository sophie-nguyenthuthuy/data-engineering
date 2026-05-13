"""Partition-scenario tests."""

from __future__ import annotations

import pytest

from cfs.partition import PartitionScenario


def test_partition_rejects_unknown_side():
    sc = PartitionScenario()
    with pytest.raises(ValueError):
        sc.write_on("c", "u1", "compA", "f", "v")


def test_partition_blocks_foreign_component_pre_heal():
    sc = PartitionScenario()
    sc.write_on("a", "u1", "compA", "f1", "A1", wall=1.0)
    with pytest.raises(ValueError):
        # Side 'a' must not write the component owned by 'b' before heal.
        sc.write_on("a", "u1", "compB", "f1", "X", wall=2.0)


def test_partition_resolver_returns_consistent_snapshot_pre_heal():
    sc = PartitionScenario()
    sc.write_on("a", "u1", "compA", "f1", "A1", wall=1.0)
    sc.write_on("b", "u1", "compB", "f1", "B1", wall=2.0)
    rv = sc.get("u1", ["f1"])
    # Both writes are concurrent in vector-clock terms; resolver returns the
    # one whose clock is ≤ the current entity-clock (pointwise max) — both
    # qualify, latest wall time wins.
    assert rv.features == {"f1": "B1"}
    assert sc.resolver().verify("u1", rv)


def test_partition_heal_allows_cross_writes():
    sc = PartitionScenario()
    sc.write_on("a", "u1", "compA", "f1", "A1", wall=1.0)
    sc.heal()
    # After heal, side 'a' can write compB.
    sc.write_on("a", "u1", "compB", "f2", "joint", wall=2.0)
    rv = sc.get("u1", ["f1", "f2"])
    assert rv.features == {"f1": "A1", "f2": "joint"}
    assert sc.resolver().verify("u1", rv)


def test_partition_full_lifecycle_verifies_at_every_step():
    sc = PartitionScenario()
    sc.write_on("a", "u1", "compA", "f1", "A1", wall=1.0)
    sc.write_on("b", "u1", "compB", "f2", "B1", wall=2.0)
    pre = sc.get("u1", ["f1", "f2"])
    assert sc.resolver().verify("u1", pre)

    sc.heal()
    sc.write_on("a", "u1", "compB", "f1", "A2", wall=3.0)
    sc.write_on("b", "u1", "compA", "f2", "B2", wall=4.0)
    post = sc.get("u1", ["f1", "f2"])
    assert post.features == {"f1": "A2", "f2": "B2"}
    assert sc.resolver().verify("u1", post)
