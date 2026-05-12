"""Causal ordering via topological sort using vector clocks."""

from __future__ import annotations

from collections import defaultdict, deque
from typing import Sequence

from .event import Event
from .vector_clock import Relation


class CausalOrderError(Exception):
    """Raised when the event graph has a cycle (impossible in a well-formed log)."""


def causal_sort(events: Sequence[Event]) -> list[Event]:
    """Return events in a total order consistent with their causal (happens-before)
    partial order.

    Algorithm:
    1. Build a directed edge A -> B whenever A.vc < B.vc (A happened-before B).
    2. Run Kahn's topological sort.
    3. Tie-break concurrent events by (producer_id, sequence_num) so that the
       output is fully deterministic regardless of input ordering.
    """
    if not events:
        return []

    n = len(events)
    idx: dict[str, int] = {e.event_id: i for i, e in enumerate(events)}

    # in-degree and adjacency list
    in_degree = [0] * n
    adj: list[list[int]] = [[] for _ in range(n)]

    for i, a in enumerate(events):
        for j, b in enumerate(events):
            if i == j:
                continue
            rel = a.vector_clock.compare(b.vector_clock)
            if rel == Relation.BEFORE:
                # a must come before b
                adj[i].append(j)
                in_degree[j] += 1

    # Kahn's algorithm with a sorted queue for determinism.
    # We use a list + sort instead of heapq so the tie-break key is explicit.
    ready: list[int] = [i for i in range(n) if in_degree[i] == 0]
    ready.sort(key=lambda i: (events[i].producer_id, events[i].sequence_num))

    result: list[Event] = []
    while ready:
        i = ready.pop(0)
        result.append(events[i])
        newly_ready: list[int] = []
        for j in adj[i]:
            in_degree[j] -= 1
            if in_degree[j] == 0:
                newly_ready.append(j)
        # Insert in sorted order to maintain determinism.
        newly_ready.sort(key=lambda k: (events[k].producer_id, events[k].sequence_num))
        # Merge into ready list (both sorted).
        merged: list[int] = []
        ri, ni = 0, 0
        while ri < len(ready) and ni < len(newly_ready):
            re, ne = events[ready[ri]], events[newly_ready[ni]]
            if (re.producer_id, re.sequence_num) <= (ne.producer_id, ne.sequence_num):
                merged.append(ready[ri]); ri += 1
            else:
                merged.append(newly_ready[ni]); ni += 1
        merged.extend(ready[ri:])
        merged.extend(newly_ready[ni:])
        ready = merged

    if len(result) != n:
        processed = {e.event_id for e in result}
        cycle_nodes = [e.event_id for e in events if e.event_id not in processed]
        raise CausalOrderError(
            f"Cycle detected in causal graph. Involved events: {cycle_nodes}"
        )

    return result


def validate_monotone_sequences(events: Sequence[Event]) -> list[str]:
    """Return a list of error messages for any producer whose sequence numbers
    are not 0-based and strictly increasing."""
    by_producer: dict[str, list[Event]] = defaultdict(list)
    for e in events:
        by_producer[e.producer_id].append(e)

    errors: list[str] = []
    for producer, evts in by_producer.items():
        evts_sorted = sorted(evts, key=lambda e: e.sequence_num)
        for expected, e in enumerate(evts_sorted):
            if e.sequence_num != expected:
                errors.append(
                    f"Producer {producer!r}: expected seq {expected}, got {e.sequence_num} "
                    f"(event {e.event_id!r})"
                )
    return errors
