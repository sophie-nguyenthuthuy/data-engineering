"""Sequential specifications (models) for linearizability checking.

Each model defines the legal sequential behaviour of a data structure.
`step(state, entry)` returns `(new_state, is_valid)`.
State must be hashable (use frozenset-of-tuples for dicts).
"""

from __future__ import annotations

from typing import Any, FrozenSet, Tuple

State = Any  # hashable


class RegisterModel:
    """Multi-key compare-free register: write(k,v) -> ok, read(k) -> v|None."""

    def initial_state(self) -> FrozenSet:
        return frozenset()  # empty dict encoded as frozenset of (k,v) pairs

    def step(self, state: FrozenSet, entry) -> Tuple[FrozenSet, bool]:
        kv = dict(state)

        if entry.f == "write":
            key, value = entry.invoke_value
            if not entry.ok:
                # Failed write: state unchanged and both outcomes are valid
                return state, True
            new_kv = {**kv, key: value}
            expected_response = "ok"
            valid = entry.response_value == expected_response
            return frozenset(new_kv.items()), valid

        elif entry.f == "read":
            key = entry.invoke_value
            expected = kv.get(key, None)
            if not entry.ok:
                return state, True
            valid = entry.response_value == expected
            return state, valid

        return state, True  # unknown ops are always valid

    def __repr__(self) -> str:
        return "RegisterModel"


class QueueModel:
    """FIFO queue: enqueue(v) -> ok, dequeue() -> v|'empty'."""

    def initial_state(self) -> tuple:
        return ()  # empty tuple = empty queue (hashable)

    def step(self, state: tuple, entry) -> Tuple[tuple, bool]:
        queue = list(state)

        if entry.f == "enqueue":
            value = entry.invoke_value
            if not entry.ok:
                return state, True
            valid = entry.response_value == "ok"
            return tuple(queue + [value]), valid

        elif entry.f == "dequeue":
            if not entry.ok:
                return state, True
            if queue:
                head = queue[0]
                valid = entry.response_value == head
                return tuple(queue[1:]), valid
            else:
                valid = entry.response_value == "empty"
                return state, valid

        return state, True

    def __repr__(self) -> str:
        return "QueueModel"


class CASRegisterModel:
    """Compare-and-swap register: write(v)->ok, read()->v, cas(old,new)->ok|fail."""

    def initial_state(self) -> tuple:
        return (None,)  # (current_value,)

    def step(self, state: tuple, entry) -> Tuple[tuple, bool]:
        (current,) = state

        if entry.f == "write":
            if not entry.ok:
                return state, True
            _, new_val = entry.invoke_value
            return (new_val,), entry.response_value == "ok"

        elif entry.f == "read":
            if not entry.ok:
                return state, True
            return state, entry.response_value == current

        elif entry.f == "cas":
            old, new = entry.invoke_value
            if not entry.ok:
                return state, True
            if current == old:
                valid = entry.response_value == "ok"
                return (new,), valid
            else:
                valid = entry.response_value == "fail"
                return state, valid

        return state, True

    def __repr__(self) -> str:
        return "CASRegisterModel"
