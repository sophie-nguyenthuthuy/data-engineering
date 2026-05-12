"""
CEP Engine — ties ring buffer, compiled patterns, and callbacks together.

Usage
-----
from cep import CEPEngine, Pattern, make_event

engine = CEPEngine()

fraud = (
    Pattern("fraud")
    .begin(EventTypes.LOGIN_FAILURE)
    .then(EventTypes.PASSWORD_RESET, max_gap_ns=10_000_000_000)
    .then(EventTypes.LARGE_WITHDRAWAL, value_gte=500.0)
    .total_window(60_000_000_000)
)

@engine.on_match("fraud")
def alert(entity_id, pattern_name, matched_at_ns):
    print(f"Fraud detected for entity {entity_id}")

engine.register(fraud)
engine.push(make_event(EventTypes.LOGIN_FAILURE, entity_id=42))
"""

from __future__ import annotations

import time
from typing import Callable, Optional

import numpy as np

from .buffer import RingBuffer
from .compiler import CompiledPattern, PatternCompiler, _make_python_fallback
from .event import EVENT_DTYPE, make_event
from .pattern import Pattern

try:
    from numba import njit as _njit  # noqa: F401
    _NUMBA = True
except ImportError:
    _NUMBA = False


MatchCallback = Callable[[int, str, int], None]


class CEPEngine:
    """
    In-process CEP engine.

    Events are pushed synchronously; all registered pattern matchers run
    inside the same ``push()`` call with no I/O and no allocation on the
    hot path.
    """

    def __init__(self, buffer_capacity: int = 1 << 16):
        self._buffer = RingBuffer(capacity=buffer_capacity)
        self._compiler = PatternCompiler()
        self._compiled: dict[str, CompiledPattern] = {}
        self._callbacks: dict[str, list[MatchCallback]] = {}
        self._use_numba = _NUMBA

    # ------------------------------------------------------------------
    # Configuration

    def register(self, pattern: Pattern) -> "CEPEngine":
        """Compile and register a pattern.  Can be called before or after push()."""
        if self._use_numba:
            cp = self._compiler.compile(pattern)
        else:
            fn = _make_python_fallback(pattern)
            from .compiler import MAX_ENTITIES
            cp = CompiledPattern(
                name=pattern.name,
                match_fn=fn,
                step_arr=np.zeros(MAX_ENTITIES, dtype=np.int8),
                count_arr=np.zeros(MAX_ENTITIES, dtype=np.int32),
                start_ts_arr=np.zeros(MAX_ENTITIES, dtype=np.int64),
                last_ts_arr=np.zeros(MAX_ENTITIES, dtype=np.int64),
            )
        self._compiled[pattern.name] = cp
        if pattern.name not in self._callbacks:
            self._callbacks[pattern.name] = []
        return self

    def on_match(self, pattern_name: str) -> Callable:
        """Decorator: register a callback fired on each match."""
        def decorator(fn: MatchCallback) -> MatchCallback:
            self._callbacks.setdefault(pattern_name, []).append(fn)
            return fn
        return decorator

    def add_callback(self, pattern_name: str, fn: MatchCallback) -> None:
        self._callbacks.setdefault(pattern_name, []).append(fn)

    # ------------------------------------------------------------------
    # Hot path

    def push(self, event: np.void) -> list[tuple[str, int]]:
        """
        Ingest one event.

        Returns a (possibly empty) list of (pattern_name, entity_id) pairs
        for patterns that fired.  Callbacks are also invoked synchronously.
        """
        self._buffer.push(event)

        etype = np.int32(event["type_id"])
        entity = np.int64(event["entity_id"])
        ts = np.int64(event["timestamp"])
        value = np.float64(event["value"])
        flags = np.uint32(event["flags"])

        fired: list[tuple[str, int]] = []
        for name, cp in self._compiled.items():
            matched = cp.match_fn(
                etype, entity, ts, value, flags,
                cp.step_arr, cp.count_arr, cp.start_ts_arr, cp.last_ts_arr,
            )
            if matched:
                fired.append((name, int(entity)))
                for cb in self._callbacks.get(name, []):
                    cb(int(entity), name, int(ts))

        return fired

    def push_dict(self, d: dict) -> list[tuple[str, int]]:
        """Convenience wrapper: push a dict-encoded event."""
        ev = make_event(
            type_id=d["type_id"],
            entity_id=d["entity_id"],
            value=d.get("value", 0.0),
            flags=d.get("flags", 0),
            timestamp=d.get("timestamp"),
        )
        return self.push(ev)

    # ------------------------------------------------------------------
    # Introspection

    @property
    def buffer(self) -> RingBuffer:
        return self._buffer

    @property
    def patterns(self) -> list[str]:
        return list(self._compiled.keys())

    def entity_state(self, pattern_name: str, entity_id: int) -> dict:
        """Return the current NFA state for an entity (useful for debugging)."""
        from .compiler import MAX_ENTITIES
        cp = self._compiled[pattern_name]
        slot = entity_id % MAX_ENTITIES
        return {
            "step": int(cp.step_arr[slot]),
            "count": int(cp.count_arr[slot]),
            "start_ts": int(cp.start_ts_arr[slot]),
            "last_ts": int(cp.last_ts_arr[slot]),
        }

    def reset_entity(self, pattern_name: str, entity_id: int) -> None:
        self._compiled[pattern_name].reset_entity(entity_id)

    def close(self) -> None:
        self._buffer.close()
