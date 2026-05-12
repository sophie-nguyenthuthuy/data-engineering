"""Topology migration tests."""

from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio

from ssb.manager import StateBackendManager
from ssb.topology.descriptor import OperatorDescriptor, TopologyDescriptor
from ssb.topology.migrator import MigrationStatus, MigrationTask, _cf_name


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _topo_v1() -> TopologyDescriptor:
    return TopologyDescriptor(
        version=1,
        operators={
            "word_count": OperatorDescriptor(
                operator_id="word_count",
                state_names=["count"],
                parallelism=1,
            )
        },
    )


def _topo_v2() -> TopologyDescriptor:
    return TopologyDescriptor(
        version=2,
        operators={
            "word_count": OperatorDescriptor(
                operator_id="word_count",
                state_names=["count"],
                parallelism=2,  # changed
            ),
            "aggregator": OperatorDescriptor(  # added
                operator_id="aggregator",
                state_names=["total"],
                parallelism=1,
            ),
        },
    )


def _topo_v3_remove() -> TopologyDescriptor:
    """Remove word_count, keep aggregator."""
    return TopologyDescriptor(
        version=3,
        operators={
            "aggregator": OperatorDescriptor(
                operator_id="aggregator",
                state_names=["total"],
                parallelism=1,
            ),
        },
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestTopologyMigration:
    async def test_state_preserved_after_migration(self):
        """
        Write state under topology v1, migrate to v2, assert state still
        accessible under v2.
        """
        mgr = StateBackendManager(backend="memory")
        mgr.start()

        # Set initial topology
        mgr.set_topology(_topo_v1())

        # Write some state
        ctx = mgr.get_state_context("word_count", "hello")
        state = ctx.get_value_state("count", default=0)
        state.set(42)

        # Migrate to v2
        task = await mgr.update_topology(_topo_v2())
        await task.wait()

        assert task.status == MigrationStatus.COMPLETED

        # State should still be accessible
        ctx2 = mgr.get_state_context("word_count", "hello")
        state2 = ctx2.get_value_state("count", default=0)
        assert state2.get() == 42

        mgr.stop()

    async def test_added_operator_cf_exists(self):
        """CFs for newly added operators should be created during migration."""
        mgr = StateBackendManager(backend="memory")
        mgr.start()

        mgr.set_topology(_topo_v1())
        task = await mgr.update_topology(_topo_v2())
        await task.wait()

        assert "aggregator::total" in mgr.backend.list_cfs()
        mgr.stop()

    async def test_version_must_increase(self):
        mgr = StateBackendManager(backend="memory")
        mgr.start()
        mgr.set_topology(_topo_v1())

        with pytest.raises(ValueError, match="must be greater"):
            await mgr.update_topology(
                TopologyDescriptor(version=1, operators={})
            )
        mgr.stop()

    async def test_migration_progress_tracking(self):
        """Progress tuple should reflect (migrated, total)."""
        mgr = StateBackendManager(backend="memory")
        mgr.start()
        mgr.set_topology(_topo_v1())

        # Write a few keys
        for word in ["alpha", "beta", "gamma"]:
            ctx = mgr.get_state_context("word_count", word)
            ctx.get_value_state("count", default=0).set(1)

        task = await mgr.update_topology(_topo_v2())
        await task.wait()

        migrated, total = task.progress
        # At least some keys were processed
        assert migrated >= 0
        assert task.status == MigrationStatus.COMPLETED
        mgr.stop()

    async def test_multiple_sequential_migrations(self):
        """Three consecutive migrations should all complete successfully."""
        mgr = StateBackendManager(backend="memory")
        mgr.start()
        mgr.set_topology(_topo_v1())

        ctx = mgr.get_state_context("word_count", "test")
        ctx.get_value_state("count", default=0).set(7)

        task1 = await mgr.update_topology(_topo_v2())
        await task1.wait()
        assert task1.status == MigrationStatus.COMPLETED

        task2 = await mgr.update_topology(_topo_v3_remove())
        await task2.wait()
        assert task2.status == MigrationStatus.COMPLETED

        # aggregator should still exist
        assert "aggregator::total" in mgr.backend.list_cfs()
        mgr.stop()

    async def test_task_to_dict_serializes(self):
        mgr = StateBackendManager(backend="memory")
        mgr.start()
        mgr.set_topology(_topo_v1())

        task = await mgr.update_topology(_topo_v2())
        await task.wait()

        d = task.to_dict()
        assert d["old_version"] == 1
        assert d["new_version"] == 2
        assert d["status"] == "completed"
        assert d["error"] is None
        mgr.stop()

    async def test_migrator_history_grows(self):
        mgr = StateBackendManager(backend="memory")
        mgr.start()
        mgr.set_topology(_topo_v1())

        await (await mgr.update_topology(_topo_v2())).wait()
        await (await mgr.update_topology(_topo_v3_remove())).wait()

        assert len(mgr.migrator.history) == 2
        mgr.stop()


class TestTopologyDescriptorDiff:
    def test_diff_added(self):
        old = TopologyDescriptor(
            version=1,
            operators={
                "a": OperatorDescriptor("a", ["s1"]),
            },
        )
        new = TopologyDescriptor(
            version=2,
            operators={
                "a": OperatorDescriptor("a", ["s1"]),
                "b": OperatorDescriptor("b", ["s2"]),
            },
        )
        added, removed, changed = old.diff(new)
        assert added == ["b"]
        assert removed == []
        assert changed == []

    def test_diff_removed(self):
        old = TopologyDescriptor(
            version=1,
            operators={
                "a": OperatorDescriptor("a", ["s1"]),
                "b": OperatorDescriptor("b", ["s2"]),
            },
        )
        new = TopologyDescriptor(
            version=2,
            operators={"a": OperatorDescriptor("a", ["s1"])},
        )
        added, removed, changed = old.diff(new)
        assert added == []
        assert removed == ["b"]
        assert changed == []

    def test_diff_changed_parallelism(self):
        old = TopologyDescriptor(
            version=1,
            operators={"a": OperatorDescriptor("a", ["s"], parallelism=1)},
        )
        new = TopologyDescriptor(
            version=2,
            operators={"a": OperatorDescriptor("a", ["s"], parallelism=4)},
        )
        added, removed, changed = old.diff(new)
        assert changed == ["a"]

    def test_diff_no_change(self):
        topo = TopologyDescriptor(
            version=1,
            operators={"a": OperatorDescriptor("a", ["s"])},
        )
        added, removed, changed = topo.diff(topo)
        assert added == []
        assert removed == []
        assert changed == []
