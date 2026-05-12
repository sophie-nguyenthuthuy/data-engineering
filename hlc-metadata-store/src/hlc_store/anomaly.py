from __future__ import annotations

from dataclasses import dataclass

from .region import CausalEvent


@dataclass
class CausalInversion:
    cause: CausalEvent
    effect: CausalEvent

    def __str__(self) -> str:
        return (
            f"CAUSAL INVERSION: '{self.cause.event_id}' → '{self.effect.event_id}' "
            f"but ts(cause)={self.cause.ts} >= ts(effect)={self.effect.ts}"
        )


def find_causal_inversions(events: list[CausalEvent]) -> list[CausalInversion]:
    """
    Given a list of events with causal links, return all pairs (e, f) where
    e causally precedes f (e → f) but ts(e) >= ts(f).

    Such inversions mean a system sorting by timestamp would place the effect
    *before* its cause — breaking read-your-writes and snapshot consistency.
    """
    by_id = {e.event_id: e for e in events}
    inversions: list[CausalInversion] = []

    for event in events:
        if event.caused_by is None:
            continue
        cause = by_id.get(event.caused_by)
        if cause is None:
            continue
        if cause.ts >= event.ts:
            inversions.append(CausalInversion(cause=cause, effect=event))

    return inversions


def count_stale_reads(events: list[CausalEvent]) -> int:
    """
    Count events where the timestamp is lower than a causally preceding event
    on the *same* node, i.e., the node's own clock went backward.
    """
    by_node: dict[str, list[CausalEvent]] = {}
    for e in events:
        by_node.setdefault(e.node_id, []).append(e)

    count = 0
    for node_events in by_node.values():
        node_events.sort(key=lambda e: e.wall_ms_at_event)
        for i in range(1, len(node_events)):
            if node_events[i].ts < node_events[i - 1].ts:
                count += 1
    return count
