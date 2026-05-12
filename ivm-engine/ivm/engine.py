"""IVMEngine — the public API for the Incremental View Maintenance system.

Usage
-----
    from ivm.engine import IVMEngine
    from ivm import agg

    engine = IVMEngine()
    orders = engine.source("orders")

    # Define a view as a dataflow pipeline
    revenue = (
        orders
        .filter(lambda r: r["status"] == "completed")
        .group_by(["category"], {
            "total":  agg.Sum("amount"),
            "orders": agg.Count(),
        })
    )
    engine.register_view("revenue", revenue)

    # Ingest events
    engine.ingest("orders", {"id": 1, "category": "books", "amount": 42, "status": "completed"})

    # Query the current view state
    print(engine.query("revenue"))

    # Retract an event (correction / cancellation)
    engine.retract("orders", {"id": 1, "category": "books", "amount": 42, "status": "completed"})
"""
from __future__ import annotations

import time
from collections import Counter
from typing import Any, Callable, Dict, List, Optional, Tuple

from ivm.operators.source import SourceOperator
from ivm.operators.base import Operator
from ivm.types import Record, Timestamp, Update, freeze_record, now_ms


# ---------------------------------------------------------------------------
# ViewState — accumulates updates into a queryable snapshot
# ---------------------------------------------------------------------------

class ViewState:
    """Maintains the current materialized state of a view.

    Internally stores: frozen_record → net multiplicity.
    Records with multiplicity ≤ 0 are considered absent.
    """

    def __init__(self):
        # frozen_record -> net count
        self._counts: Counter = Counter()
        # full ordered delta log
        self._log: List[Update] = []

    def apply(self, updates: List[Update]) -> None:
        for u in updates:
            self._log.append(u)
            key = freeze_record(u.record)
            self._counts[key] += u.diff
            if self._counts[key] <= 0:
                del self._counts[key]

    def records(self) -> List[Record]:
        """All records with positive multiplicity (expanded for count > 1)."""
        result: List[Record] = []
        for frozen, count in self._counts.items():
            rec = dict(frozen)
            for _ in range(count):
                result.append(rec)
        return result

    def delta_log(self) -> List[Update]:
        """Full history of all deltas applied to this view."""
        return list(self._log)

    def recent_deltas(self, n: int = 10) -> List[Update]:
        return self._log[-n:]

    def count(self) -> int:
        return sum(self._counts.values())

    def __repr__(self) -> str:  # pragma: no cover
        return f"ViewState({self.count()} rows)"


# ---------------------------------------------------------------------------
# IVMEngine
# ---------------------------------------------------------------------------

class IVMEngine:
    """Orchestrates sources, pipelines, and view materialization.

    Thread safety: not thread-safe; use external locking if needed.
    """

    def __init__(self):
        self._sources: Dict[str, SourceOperator] = {}
        self._views: Dict[str, ViewState] = {}
        self._listeners: Dict[str, List[Callable[[List[Update]], None]]] = {}

    # ------------------------------------------------------------------
    # Building the dataflow
    # ------------------------------------------------------------------

    def source(self, name: str) -> SourceOperator:
        """Create (or retrieve) a named input stream source."""
        if name not in self._sources:
            self._sources[name] = SourceOperator(name)
        return self._sources[name]

    def register_view(self, name: str, operator: Operator) -> "IVMEngine":
        """Attach a view to the tail of an operator pipeline.

        The engine intercepts all output Updates and materialises them into a
        queryable ViewState.  Multiple views can share upstream operators.
        """
        state = ViewState()
        self._views[name] = state

        def capture(updates: List[Update]) -> None:
            state.apply(updates)
            for fn in self._listeners.get(name, []):
                fn(updates)

        operator.add_listener(capture)
        return self

    def on_view_update(self, view_name: str,
                       fn: Callable[[List[Update]], None]) -> "IVMEngine":
        """Register a callback fired whenever `view_name` receives new deltas."""
        self._listeners.setdefault(view_name, []).append(fn)
        return self

    # ------------------------------------------------------------------
    # Ingestion
    # ------------------------------------------------------------------

    def ingest(self, stream_name: str, record: Record,
               diff: int = 1, timestamp: Optional[int] = None) -> None:
        """Push a record into a source stream.

        Parameters
        ----------
        stream_name : str
        record : dict
        diff : int
            +1 for insertion (default), -1 for retraction.
        timestamp : int, optional
            Millisecond timestamp.  Defaults to wall clock.
        """
        if stream_name not in self._sources:
            raise KeyError(f"Unknown source stream: {stream_name!r}. "
                           f"Call engine.source({stream_name!r}) first.")
        ts = timestamp if timestamp is not None else now_ms()
        update = Update(record, ts, diff)
        self._sources[stream_name].handle([update])

    def retract(self, stream_name: str, record: Record,
                timestamp: Optional[int] = None) -> None:
        """Convenience wrapper for ingesting a retraction (diff = -1)."""
        self.ingest(stream_name, record, diff=-1, timestamp=timestamp)

    def ingest_batch(self, stream_name: str,
                     records: List[Record],
                     diff: int = 1,
                     timestamp: Optional[int] = None) -> None:
        """Ingest multiple records at once (single propagation pass)."""
        if stream_name not in self._sources:
            raise KeyError(f"Unknown source stream: {stream_name!r}.")
        ts = timestamp if timestamp is not None else now_ms()
        updates = [Update(r, ts, diff) for r in records]
        self._sources[stream_name].handle(updates)

    # ------------------------------------------------------------------
    # Querying
    # ------------------------------------------------------------------

    def query(self, view_name: str) -> List[Record]:
        """Return the current materialized records for a view."""
        self._check_view(view_name)
        return self._views[view_name].records()

    def delta_log(self, view_name: str) -> List[Update]:
        """Return the full delta log for a view."""
        self._check_view(view_name)
        return self._views[view_name].delta_log()

    def recent_deltas(self, view_name: str, n: int = 10) -> List[Update]:
        """Return the last n deltas applied to a view."""
        self._check_view(view_name)
        return self._views[view_name].recent_deltas(n)

    def row_count(self, view_name: str) -> int:
        self._check_view(view_name)
        return self._views[view_name].count()

    def _check_view(self, name: str) -> None:
        if name not in self._views:
            raise KeyError(f"Unknown view: {name!r}. "
                           f"Call engine.register_view({name!r}, ...) first.")

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def sources(self) -> List[str]:
        return list(self._sources.keys())

    def views(self) -> List[str]:
        return list(self._views.keys())

    def __repr__(self) -> str:  # pragma: no cover
        return (f"IVMEngine(sources={self.sources()}, views={self.views()})")
