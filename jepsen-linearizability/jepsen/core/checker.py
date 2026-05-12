"""Wing-Gong linearizability checker.

The Wing & Gong (1993) algorithm checks if a concurrent history is
linearizable against a sequential specification model.

Key idea: a history is linearizable iff we can find a total ordering
of completed operations such that:
  1. The ordering respects real-time (if A completes before B starts, A < B).
  2. The ordering satisfies the sequential model.

We search this space recursively, memoizing on (remaining_op_set, model_state).
Complexity is exponential in the worst case but caches aggressively.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, List, Optional, Tuple

from .history import Entry
from .models import RegisterModel


@dataclass
class CheckResult:
    linearizable: bool
    linearization: List[Entry]      # valid total order (empty if not linearizable)
    witness: Optional[List[Entry]]  # first violation witness, or None
    checked_ops: int
    elapsed_seconds: float
    model: Any


def check(entries: List[Entry], model=None) -> CheckResult:
    """Check linearizability of a list of history entries against model."""
    if model is None:
        model = RegisterModel()

    t0 = time.monotonic()

    if not entries:
        return CheckResult(
            linearizable=True,
            linearization=[],
            witness=None,
            checked_ops=0,
            elapsed_seconds=0.0,
            model=model,
        )

    # Only check completed or explicitly-failed operations.
    # Pending (crashed) ops are already handled in History.entries().
    op_map: Dict[int, Entry] = {e.index: e for e in entries}
    memo: Dict[Tuple[FrozenSet, Any], bool] = {}
    counter = [0]

    def candidates(remaining: FrozenSet) -> List[Entry]:
        """
        An entry can be linearized next if no other remaining entry
        has a response_time strictly before this entry's invoke_time.
        (i.e. no remaining op must precede it in real-time.)
        """
        result = []
        for idx in remaining:
            e = op_map[idx]
            can_be_next = not any(
                op_map[j].response_time < e.invoke_time
                for j in remaining
                if j != idx
            )
            if can_be_next:
                result.append(e)
        return result

    def dfs(remaining: FrozenSet, state: Any) -> Tuple[bool, List[Entry]]:
        counter[0] += 1
        key = (remaining, state)
        if key in memo:
            return memo[key], []

        if not remaining:
            memo[key] = True
            return True, []

        for entry in candidates(remaining):
            new_state, valid = model.step(state, entry)
            if valid:
                new_remaining = remaining - {entry.index}
                ok, rest = dfs(new_remaining, new_state)
                if ok:
                    memo[key] = True
                    return True, [entry] + rest

        memo[key] = False
        return False, []

    all_indices = frozenset(op_map.keys())
    initial = model.initial_state()

    ok, lin = dfs(all_indices, initial)

    return CheckResult(
        linearizable=ok,
        linearization=lin if ok else [],
        witness=None,
        checked_ops=counter[0],
        elapsed_seconds=time.monotonic() - t0,
        model=model,
    )
