"""
AS OF temporal join engine for out-of-order event streams.

Join semantics
--------------
For each left event L at time T_l with key K, emit the pair
(L, R*) where R* is the latest right event with:

    R.key == K
    T_l - lookback_window <= R.event_time <= T_l

If no such R exists, emit (L, None).

Late-arrival corrections
------------------------
When a right event R' arrives with event_time T_r' that is within the
right stream's lateness window (i.e. reclaimably late), the engine
identifies every previously emitted (L_i, R_old) where R' would now be
the strictly better AS OF match.  For each such pair it emits:

    JoinResult(L_i, R_old, retraction=True)   # withdraw old result
    JoinResult(L_i, R',    retraction=False)   # new corrected result
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional

from .event import Event, JoinResult, STREAM_LEFT, STREAM_RIGHT
from .interval_tree import IntervalTree
from .watermark import WatermarkTracker


@dataclass
class _EmittedEntry:
    """Mutable record stored alongside each emitted left event for future correction."""
    left_event: Event
    matched_right: Optional[Event]  # the right event we matched (None = no match)

    @property
    def matched_right_time(self) -> Optional[int]:
        return self.matched_right.event_time if self.matched_right else None


class AsOfJoinEngine:
    """
    AS OF temporal join with late-arrival correction.

    Parameters
    ----------
    lookback_window : int
        Maximum milliseconds the right-side event may precede the left event
        and still be considered a valid AS OF match.  Use a very large integer
        for an unbounded lookback.
    left_lateness_bound : int
        How many milliseconds behind the left-stream frontier a left event may
        arrive and still be processed.  Late left events are joined as usual but
        do not trigger corrections on the right side.
    right_lateness_bound : int
        How many milliseconds behind the right-stream frontier a right event may
        arrive and still trigger corrections for already-emitted left joins.
        Right events that arrive later than this bound are discarded.
    """

    def __init__(
        self,
        lookback_window: int,
        left_lateness_bound: int = 0,
        right_lateness_bound: int = 0,
    ) -> None:
        if lookback_window < 0:
            raise ValueError("lookback_window must be >= 0")
        self.lookback_window = lookback_window
        self._left_wm = WatermarkTracker(left_lateness_bound)
        self._right_wm = WatermarkTracker(right_lateness_bound)

        # Build side: per-key IntervalTree(event_time -> [Event, ...])
        self._right_trees: Dict[str, IntervalTree] = defaultdict(IntervalTree)

        # Correction index: per-key IntervalTree(left_event_time -> [_EmittedEntry, ...])
        # Values are mutable so we can update matched_right in-place.
        self._emitted: Dict[str, IntervalTree] = defaultdict(IntervalTree)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def process_event(self, event: Event) -> List[JoinResult]:
        """
        Ingest a single event from either stream.

        Returns a list of JoinResult records.  For an on-time left event this is
        always a single record.  A late right event may return an even number of
        records: alternating retraction / correction pairs.
        """
        if event.stream_id == STREAM_LEFT:
            return self._process_left(event)
        return self._process_right(event)

    def advance_left_watermark(self, watermark: int) -> None:
        """Push the left-stream watermark forward externally."""
        self._left_wm.advance_to(watermark)

    def advance_right_watermark(self, watermark: int) -> None:
        """Push the right-stream watermark forward externally."""
        self._right_wm.advance_to(watermark)

    @property
    def left_watermark(self) -> int:
        return self._left_wm.watermark

    @property
    def right_watermark(self) -> int:
        return self._right_wm.watermark

    # ------------------------------------------------------------------
    # Left (probe) stream
    # ------------------------------------------------------------------

    def _process_left(self, event: Event) -> List[JoinResult]:
        if self._left_wm.is_irreparably_late(event.event_time):
            # Drop — too late to be useful.
            return []

        self._left_wm.observe(event.event_time)
        right_match = self._lookup_right(event.key, event.event_time)

        entry = _EmittedEntry(left_event=event, matched_right=right_match)
        self._emitted[event.key].insert(event.event_time, entry)

        return [JoinResult(left_event=event, right_event=right_match)]

    def _lookup_right(self, key: str, left_time: int) -> Optional[Event]:
        """
        Find the latest right event for *key* with event_time in
        [left_time - lookback_window, left_time].
        """
        tree = self._right_trees.get(key)
        if not tree:
            return None
        result = tree.predecessor(left_time)
        if result is None:
            return None
        ts, events = result
        if ts < left_time - self.lookback_window:
            return None
        # Among events sharing the same timestamp take the most recently inserted one.
        return events[-1]

    # ------------------------------------------------------------------
    # Right (build) stream
    # ------------------------------------------------------------------

    def _process_right(self, event: Event) -> List[JoinResult]:
        if self._right_wm.is_irreparably_late(event.event_time):
            # Beyond the lateness budget — discard silently.
            return []

        is_late = self._right_wm.is_reclaimably_late(event.event_time)
        self._right_wm.observe(event.event_time)

        # Always insert into the build-side tree.
        self._right_trees[event.key].insert(event.event_time, event)

        if is_late:
            return self._compute_corrections(event)
        return []

    # ------------------------------------------------------------------
    # Late-arrival correction
    # ------------------------------------------------------------------

    def _compute_corrections(self, late_right: Event) -> List[JoinResult]:
        """
        Determine which already-emitted (L, R_old) pairs need to be updated
        because *late_right* is now the correct AS OF match for L.

        A late right event at T_r is a better match for a left event at T_l iff:
            1.  T_r ≤ T_l                         (right precedes left — AS OF semantics)
            2.  T_l - lookback_window ≤ T_r       (within the lookback)
            3.  T_r > R_old.event_time  (strictly better than what we emitted)
               or R_old is None         (we previously had no match)

        From conditions 1 and 2: T_r ≤ T_l ≤ T_r + lookback_window
        so we range-query the emitted tree for left times in that band.
        """
        key = late_right.key
        emitted_tree = self._emitted.get(key)
        if not emitted_tree:
            return []

        lo = late_right.event_time
        hi = late_right.event_time + self.lookback_window
        affected = emitted_tree.range_query(lo, hi)

        corrections: List[JoinResult] = []
        for t_l, entries in affected:
            for entry in entries:
                old_right_time = entry.matched_right_time

                # Is late_right a strictly better AS OF match?
                if old_right_time is not None and late_right.event_time <= old_right_time:
                    continue  # existing match is at least as good

                # Double-check bounds (range_query already enforces these, but be explicit).
                if not (t_l - self.lookback_window <= late_right.event_time <= t_l):
                    continue

                old_right_event = entry.matched_right  # may be None

                corrections.append(
                    JoinResult(
                        left_event=entry.left_event,
                        right_event=old_right_event,
                        retraction=True,
                    )
                )
                corrections.append(
                    JoinResult(
                        left_event=entry.left_event,
                        right_event=late_right,
                        retraction=False,
                    )
                )

                # Update in-place so subsequent late events see the current best match.
                entry.matched_right = late_right

        return corrections
