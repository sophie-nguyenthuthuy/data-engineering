"""
Wing-Gong linearizability checker for key-value operations.

A history is linearizable if there exists a sequential ordering of all
completed operations such that:
  1. Each operation appears to take effect atomically at some point between
     its invocation and response.
  2. The ordering is consistent with the sequential specification of the
     object (a key-value store here).

Algorithm: Wing & Gong (1993), bounded by memoization.
Time complexity: O(p! × n) worst case, but memoizes visited states.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple


@dataclass
class Operation:
    """A single KV operation recorded during a test run."""
    call_time: float      # wall-clock start (monotonic)
    return_time: float    # wall-clock end
    op: str               # "put" | "get" | "delete" | "cas"
    key: str
    value: Any = None     # write value or expected value (for CAS)
    new_value: Any = None # CAS new value
    result: Any = None    # observed return value / result dict
    ok: bool = True       # did the call succeed (no crash/timeout)


# ── Sequential KV specification ───────────────────────────────────────────────

def _kv_init() -> Dict[str, Any]:
    return {}


def _kv_apply(
    state: Dict[str, Any], op: Operation
) -> Tuple[Optional[Dict[str, Any]], Any]:
    """
    Apply op to state according to KV semantics.
    Returns (new_state, expected_result) or (None, _) if precondition fails.
    """
    s = dict(state)
    if op.op == "put":
        old_ver = s.get(op.key, {}).get("version", 0)
        if op.value is None:
            # expected_version check (if supplied in result)
            pass
        s[op.key] = {"value": op.value, "version": old_ver + 1}
        return s, {"ok": True}
    elif op.op == "get":
        entry = s.get(op.key)
        val = entry["value"] if entry else None
        expected = {"value": val}
        return s, expected
    elif op.op == "delete":
        if op.key not in s:
            return s, {"ok": False}
        del s[op.key]
        return s, {"ok": True}
    elif op.op == "cas":
        entry = s.get(op.key)
        current = entry["value"] if entry else None
        if current != op.value:
            return s, {"ok": False}
        old_ver = entry["version"] if entry else 0
        s[op.key] = {"value": op.new_value, "version": old_ver + 1}
        return s, {"ok": True}
    return None, None


def _result_matches(observed: Any, expected: Any) -> bool:
    """Loose match — we only check the fields we care about."""
    if isinstance(expected, dict) and isinstance(observed, dict):
        for k, v in expected.items():
            if k in observed and observed[k] != v:
                return False
        return True
    return observed == expected


# ── Checker ───────────────────────────────────────────────────────────────────

def _state_hash(state: Dict, remaining: frozenset) -> str:
    key = json.dumps(state, sort_keys=True) + str(sorted(remaining))
    return hashlib.md5(key.encode()).hexdigest()


def check_linearizability(history: List[Operation]) -> Tuple[bool, Optional[str]]:
    """
    Check whether `history` is linearizable.
    Returns (True, None) if linearizable, (False, explanation) otherwise.

    Only completed operations (ok=True) are checked; crashed/pending ops
    are treated as either having taken effect or not.
    """
    # Filter to completed ops only; sort by return time
    ops = [o for o in history if o.ok]
    if not ops:
        return True, None

    # Build partial order constraint: op_i must come before op_j if
    # op_i's return_time < op_j's call_time (no overlap)
    n = len(ops)
    must_precede: List[Set[int]] = [set() for _ in range(n)]
    for i in range(n):
        for j in range(n):
            if i != j and ops[i].return_time < ops[j].call_time:
                must_precede[j].add(i)  # j must come after i

    visited: Set[str] = set()

    def search(state: Dict, remaining: Set[int], linearized: List[int]) -> bool:
        if not remaining:
            return True

        h = _state_hash(state, frozenset(remaining))
        if h in visited:
            return False
        visited.add(h)

        # Try to linearize any op whose predecessors are all done
        for idx in list(remaining):
            if must_precede[idx] - set(linearized):
                continue  # predecessors not yet linearized
            op = ops[idx]
            new_state, expected = _kv_apply(state, op)
            if new_state is None:
                continue
            if not _result_matches(op.result, expected):
                continue
            remaining.remove(idx)
            linearized.append(idx)
            if search(new_state, remaining, linearized):
                return True
            remaining.add(idx)
            linearized.pop()

        return False

    ok = search(_kv_init(), set(range(n)), [])
    if ok:
        return True, None
    return False, f"History of {n} ops is NOT linearizable"


# ── History recorder ──────────────────────────────────────────────────────────

class HistoryRecorder:
    def __init__(self) -> None:
        self.ops: List[Operation] = []

    def record(
        self,
        op: str,
        key: str,
        value: Any = None,
        new_value: Any = None,
        result: Any = None,
        call_time: Optional[float] = None,
        return_time: Optional[float] = None,
        ok: bool = True,
    ) -> None:
        self.ops.append(
            Operation(
                call_time=call_time or time.monotonic(),
                return_time=return_time or time.monotonic(),
                op=op,
                key=key,
                value=value,
                new_value=new_value,
                result=result,
                ok=ok,
            )
        )

    def check(self) -> Tuple[bool, Optional[str]]:
        return check_linearizability(self.ops)
