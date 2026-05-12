"""CRUD tests for all 5 state handle types."""

from __future__ import annotations

import pytest

from ssb.manager import StateBackendManager
from ssb.state.descriptor import StateDescriptor
from ssb.state.handle import (
    AggregatingStateHandle,
    ListStateHandle,
    MapStateHandle,
    ReducingStateHandle,
    ValueStateHandle,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ctx(manager: StateBackendManager, op: str = "op1", key: str = "k1"):
    return manager.get_state_context(op, key)


# ---------------------------------------------------------------------------
# ValueState
# ---------------------------------------------------------------------------


class TestValueState:
    def test_default_when_empty(self, manager):
        state = _ctx(manager).get_value_state("v1", default=0)
        assert state.get() == 0

    def test_set_and_get(self, manager):
        state = _ctx(manager).get_value_state("v2")
        state.set(42)
        assert state.get() == 42

    def test_overwrite(self, manager):
        state = _ctx(manager).get_value_state("v3")
        state.set("hello")
        state.set("world")
        assert state.get() == "world"

    def test_clear_returns_default(self, manager):
        state = _ctx(manager).get_value_state("v4", default=-1)
        state.set(99)
        state.clear()
        assert state.get() == -1

    def test_independent_record_keys(self, manager):
        s1 = manager.get_state_context("op1", "key_a").get_value_state("cnt", default=0)
        s2 = manager.get_state_context("op1", "key_b").get_value_state("cnt", default=0)
        s1.set(10)
        s2.set(20)
        assert s1.get() == 10
        assert s2.get() == 20

    def test_stores_complex_values(self, manager):
        state = _ctx(manager).get_value_state("complex")
        value = {"a": [1, 2, 3], "b": {"nested": True}}
        state.set(value)
        assert state.get() == value


# ---------------------------------------------------------------------------
# ListState
# ---------------------------------------------------------------------------


class TestListState:
    def test_empty_list_when_unset(self, manager):
        state = _ctx(manager).get_list_state("lst1")
        assert state.get() == []

    def test_add_and_get(self, manager):
        state = _ctx(manager).get_list_state("lst2")
        state.add(1)
        state.add(2)
        state.add(3)
        assert state.get() == [1, 2, 3]

    def test_update_replaces_list(self, manager):
        state = _ctx(manager).get_list_state("lst3")
        state.add(1)
        state.update([10, 20])
        assert state.get() == [10, 20]

    def test_clear(self, manager):
        state = _ctx(manager).get_list_state("lst4")
        state.add("x")
        state.clear()
        assert state.get() == []

    def test_mixed_types(self, manager):
        state = _ctx(manager).get_list_state("lst5")
        state.add(1)
        state.add("two")
        state.add({"three": 3})
        lst = state.get()
        assert len(lst) == 3
        assert lst[1] == "two"


# ---------------------------------------------------------------------------
# MapState
# ---------------------------------------------------------------------------


class TestMapState:
    def test_put_and_get(self, manager):
        state = _ctx(manager).get_map_state("mp1")
        state.put("k", "v")
        assert state.get("k") == "v"

    def test_get_missing_returns_none(self, manager):
        state = _ctx(manager).get_map_state("mp2")
        assert state.get("no_such_key") is None

    def test_remove(self, manager):
        state = _ctx(manager).get_map_state("mp3")
        state.put("a", 1)
        state.remove("a")
        assert state.get("a") is None

    def test_contains(self, manager):
        state = _ctx(manager).get_map_state("mp4")
        state.put("x", 100)
        assert state.contains("x") is True
        assert state.contains("y") is False

    def test_keys_values_items(self, manager):
        state = _ctx(manager).get_map_state("mp5")
        state.put("a", 1)
        state.put("b", 2)
        state.put("c", 3)
        assert sorted(state.keys()) == ["a", "b", "c"]
        assert sorted(state.values()) == [1, 2, 3]
        assert sorted(state.items()) == [("a", 1), ("b", 2), ("c", 3)]

    def test_clear(self, manager):
        state = _ctx(manager).get_map_state("mp6")
        state.put("a", 1)
        state.put("b", 2)
        state.clear()
        assert list(state.items()) == []

    def test_independent_record_keys(self, manager):
        s1 = manager.get_state_context("op1", "r1").get_map_state("m")
        s2 = manager.get_state_context("op1", "r2").get_map_state("m")
        s1.put("x", 1)
        s2.put("x", 999)
        assert s1.get("x") == 1
        assert s2.get("x") == 999


# ---------------------------------------------------------------------------
# ReducingState
# ---------------------------------------------------------------------------


class TestReducingState:
    def _sum_state(self, manager, name="rs1"):
        return _ctx(manager).get_reducing_state(name, reduce_fn=lambda a, b: a + b)

    def test_empty_returns_none(self, manager):
        state = self._sum_state(manager, "rs0")
        assert state.get() is None

    def test_single_add(self, manager):
        state = self._sum_state(manager, "rs1")
        state.add(5)
        assert state.get() == 5

    def test_multiple_adds(self, manager):
        state = self._sum_state(manager, "rs2")
        for i in range(1, 6):
            state.add(i)
        assert state.get() == 15

    def test_clear(self, manager):
        state = self._sum_state(manager, "rs3")
        state.add(10)
        state.clear()
        assert state.get() is None

    def test_max_reduce(self, manager):
        state = _ctx(manager).get_reducing_state("rs_max", reduce_fn=max)
        state.add(3)
        state.add(7)
        state.add(2)
        assert state.get() == 7


# ---------------------------------------------------------------------------
# AggregatingState
# ---------------------------------------------------------------------------


class TestAggregatingState:
    def _avg_state(self, manager, name="agg1"):
        """Accumulator is (sum, count); get_fn computes average."""
        return _ctx(manager).get_aggregating_state(
            name,
            add_fn=lambda acc, v: (acc[0] + v, acc[1] + 1),
            get_fn=lambda acc: acc[0] / acc[1] if acc[1] > 0 else 0,
            initial_acc=(0, 0),
        )

    def test_initial_get(self, manager):
        state = self._avg_state(manager, "agg0")
        assert state.get() == 0

    def test_single_value(self, manager):
        state = self._avg_state(manager, "agg1")
        state.add(10)
        assert state.get() == 10.0

    def test_average(self, manager):
        state = self._avg_state(manager, "agg2")
        for v in [10, 20, 30]:
            state.add(v)
        assert state.get() == 20.0

    def test_clear(self, manager):
        state = self._avg_state(manager, "agg3")
        state.add(5)
        state.clear()
        assert state.get() == 0

    def test_string_concat(self, manager):
        state = _ctx(manager).get_aggregating_state(
            "agg_str",
            add_fn=lambda acc, v: acc + v,
            get_fn=lambda acc: acc,
            initial_acc="",
        )
        state.add("hello")
        state.add(" ")
        state.add("world")
        assert state.get() == "hello world"
