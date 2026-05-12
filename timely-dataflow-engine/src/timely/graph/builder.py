"""Graph builder.

A fluent-ish API:
    g = GraphBuilder()
    g.source("input", initial_records)
    g.map("doubled", lambda x: x * 2, input="input")
    g.filter("positive", lambda x: x > 0, input="doubled")
    g.sink("out", input="positive")
    runtime = Runtime(g)
    runtime.run()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from timely.graph.operator import EmitFn, Operator

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from timely.timestamp.ts import Timestamp


@dataclass
class GraphBuilder:
    operators: dict[str, Operator] = field(default_factory=dict)
    initial_inputs: dict[str, list[tuple[Timestamp, Any]]] = field(default_factory=dict)
    sinks: dict[str, list[tuple[Timestamp, Any]]] = field(default_factory=dict)

    # ---- Construction primitives -----------------------------------------

    def add(self, op: Operator) -> Operator:
        if op.name in self.operators:
            raise ValueError(f"operator already exists: {op.name}")
        self.operators[op.name] = op
        return op

    def source(
        self,
        name: str,
        records: Iterable[tuple[Timestamp, Any]],
        downstream: str | None = None,
    ) -> Operator:
        """A source emits a fixed list of records on `run()`. If `downstream`
        is set, the source forwards each record to that op; otherwise records
        sit in the source's own queue (useful for chained sources)."""
        recs = list(records)
        self.initial_inputs[name] = recs

        if downstream is None:
            def _fn(ts: Timestamp, value: Any, emit: EmitFn) -> None:
                pass  # leaf source — records stay in queue
        else:
            def _fn(ts: Timestamp, value: Any, emit: EmitFn) -> None:
                emit(downstream, ts, value)

        return self.add(Operator(name=name, fn=_fn))

    def map(
        self,
        name: str,
        fn: Callable[[Any], Any],
        input: str,
        downstream: str | None = None,
    ) -> Operator:
        if downstream is None:
            downstream = name + "_out"

        def _fn(ts: Timestamp, value: Any, emit: EmitFn) -> None:
            emit(downstream, ts, fn(value))

        return self.add(Operator(name=name, fn=_fn, inputs=[input]))

    def filter(
        self,
        name: str,
        pred: Callable[[Any], bool],
        input: str,
        downstream: str | None = None,
    ) -> Operator:
        if downstream is None:
            downstream = name + "_out"

        def _fn(ts: Timestamp, value: Any, emit: EmitFn) -> None:
            if pred(value):
                emit(downstream, ts, value)

        return self.add(Operator(name=name, fn=_fn, inputs=[input]))

    def reduce(
        self,
        name: str,
        fn: Callable[[Any, Any], Any],
        input: str,
        downstream: str | None = None,
    ) -> Operator:
        """Stateful reduce: accumulates per (input) timestamp.

        Emits the running total each call.  Real Naiad reduce would only
        emit on frontier advance, but this is enough for tests.
        """
        if downstream is None:
            downstream = name + "_out"
        state: dict[Timestamp, Any] = {}

        def _fn(ts: Timestamp, value: Any, emit: EmitFn) -> None:
            state[ts] = fn(state.get(ts, value), value) if ts in state else value
            emit(downstream, ts, state[ts])

        return self.add(Operator(name=name, fn=_fn, inputs=[input]))

    def iterate(
        self,
        name: str,
        body: Callable[[Timestamp, Any, EmitFn], None],
        input: str,
    ) -> Operator:
        """Iterate-scope operator. `feedback=True` causes emit() to bump
        the timestamp's iteration component on output."""
        return self.add(Operator(name=name, fn=body, inputs=[input], feedback=True))

    def sink(self, name: str, input: str) -> Operator:
        records: list[tuple[Timestamp, Any]] = []
        self.sinks[name] = records

        def _fn(ts: Timestamp, value: Any, emit: EmitFn) -> None:
            records.append((ts, value))

        return self.add(Operator(name=name, fn=_fn, inputs=[input]))

    # ---- Validation ------------------------------------------------------

    def validate(self) -> None:
        """Ensure all referenced inputs exist."""
        for op in self.operators.values():
            for inp in op.inputs:
                if inp not in self.operators:
                    raise ValueError(f"{op.name} references missing input {inp!r}")


__all__ = ["GraphBuilder"]
