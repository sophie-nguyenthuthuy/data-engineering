"""Tests for late-arrival detection and correction emission."""
import pytest
from temporal_join import AsOfJoinEngine, Event, JoinResult, STREAM_LEFT, STREAM_RIGHT


def left(key: str, t: int, **payload) -> Event:
    return Event(key=key, event_time=t, stream_id=STREAM_LEFT, payload=payload)


def right(key: str, t: int, **payload) -> Event:
    return Event(key=key, event_time=t, stream_id=STREAM_RIGHT, payload=payload)


def assert_retract_then_emit(corrections: list, left_time: int, old_right_t, new_right_t):
    """Verify a retract/emit correction pair for a given left event time."""
    assert len(corrections) >= 2
    retract = next(r for r in corrections if r.retraction and r.left_event.event_time == left_time)
    emit = next(r for r in corrections if not r.retraction and r.left_event.event_time == left_time)
    old_t = retract.right_event.event_time if retract.right_event else None
    new_t = emit.right_event.event_time if emit.right_event else None
    assert old_t == old_right_t, f"expected retracted R@{old_right_t}, got R@{old_t}"
    assert new_t == new_right_t, f"expected corrected R@{new_right_t}, got R@{new_t}"


class TestSingleCorrection:
    def test_late_right_replaces_none_match(self):
        """Late right event provides a match where previously there was none."""
        engine = AsOfJoinEngine(lookback_window=60_000, right_lateness_bound=5_000)
        # Left arrives; no right yet.
        engine.process_event(left("u1", 1000))
        # On-time right advances the frontier.
        engine.process_event(right("u1", 6000))
        # Late right at T=500 — within lateness bound (6000 - 5000 = 1000 watermark > 500? No, 500 < 1000)
        # watermark = 6000 - 5000 = 1000; T=500 < 1000 → irreparably late → dropped
        corrections = engine.process_event(right("u1", 500))
        assert corrections == []

    def test_late_right_within_lateness_window(self):
        """Late right that is reclaimably late triggers a correction."""
        engine = AsOfJoinEngine(lookback_window=60_000, right_lateness_bound=5_000)
        engine.process_event(left("u1", 1000))
        # Advance frontier to 3000; watermark = 3000 - 5000 = -2000
        engine.process_event(right("u1", 3000))
        # Late right at T=800: watermark=-2000, 800 >= -2000, and 800 < 3000 → reclaimably late
        corrections = engine.process_event(right("u1", 800))
        assert_retract_then_emit(corrections, left_time=1000, old_right_t=None, new_right_t=800)

    def test_late_right_upgrades_earlier_match(self):
        """Late right is a better (more recent) match than what was emitted."""
        engine = AsOfJoinEngine(lookback_window=60_000, right_lateness_bound=10_000)
        engine.process_event(right("u1", 100))   # early right
        engine.process_event(left("u1", 1000))   # joined with R@100
        # Advance frontier to 5000: watermark = 5000-10000 = -5000
        # R@800 satisfies: -5000 <= 800 < 5000  → reclaimably late
        engine.process_event(right("u1", 5_000))
        corrections = engine.process_event(right("u1", 800))
        assert_retract_then_emit(corrections, left_time=1000, old_right_t=100, new_right_t=800)

    def test_late_right_not_better_than_existing_match(self):
        """Late right older than the current match must not trigger a correction."""
        engine = AsOfJoinEngine(lookback_window=60_000, right_lateness_bound=10_000)
        engine.process_event(right("u1", 900))   # close match
        engine.process_event(left("u1", 1000))   # joined with R@900
        engine.process_event(right("u1", 12_000))
        # Late right at T=400 — older than R@900, not a better match
        corrections = engine.process_event(right("u1", 400))
        assert corrections == []

    def test_late_right_outside_lookback_window(self):
        """Late right is within lateness bound but outside the lookback window."""
        engine = AsOfJoinEngine(lookback_window=200, right_lateness_bound=10_000)
        engine.process_event(left("u1", 1000))
        engine.process_event(right("u1", 12_000))
        # Late right at T=700 — 1000-700=300 > lookback=200 → should not match
        corrections = engine.process_event(right("u1", 700))
        assert corrections == []

    def test_right_after_left_time_not_a_match(self):
        """Right event at T_r > T_l cannot be an AS OF match for that left event."""
        engine = AsOfJoinEngine(lookback_window=60_000, right_lateness_bound=10_000)
        engine.process_event(left("u1", 1000))
        engine.process_event(right("u1", 12_000))
        # Late right at T=1500 (after the left event at 1000) — not a valid AS OF match
        corrections = engine.process_event(right("u1", 1500))
        assert corrections == []


class TestMultipleCorrections:
    def test_one_late_right_corrects_multiple_lefts(self):
        """A single late right event may correct several left events at once."""
        engine = AsOfJoinEngine(lookback_window=60_000, right_lateness_bound=10_000)
        engine.process_event(left("u1", 1000))
        engine.process_event(left("u1", 2000))
        engine.process_event(left("u1", 3000))
        # Advance frontier to 5000: watermark = -5000; R@500 is reclaimably late
        engine.process_event(right("u1", 5_000))
        corrections = engine.process_event(right("u1", 500))
        left_times_corrected = {c.left_event.event_time for c in corrections if not c.retraction}
        assert 1000 in left_times_corrected
        assert 2000 in left_times_corrected
        assert 3000 in left_times_corrected

    def test_sequential_late_rights_chain_corrections(self):
        """Two successive late right events each improve on the previous best match."""
        engine = AsOfJoinEngine(lookback_window=60_000, right_lateness_bound=20_000)
        engine.process_event(left("u1", 1000))
        engine.process_event(right("u1", 25_000))  # frontier=25000, watermark=5000

        # First late right at T=600 (< 25000, > watermark=-∞ initially ... let's be precise)
        # watermark = 25000 - 20000 = 5000; 600 < 5000 → irreparably late, dropped
        c1 = engine.process_event(right("u1", 600))
        assert c1 == []

        # Push frontier out further so T=600 is reclaimable
        engine2 = AsOfJoinEngine(lookback_window=60_000, right_lateness_bound=20_000)
        engine2.process_event(left("u1", 1000))
        engine2.process_event(right("u1", 15_000))  # watermark = -5000

        c1 = engine2.process_event(right("u1", 600))   # late, reclaimable: -5000 ≤ 600 < 15000
        assert_retract_then_emit(c1, left_time=1000, old_right_t=None, new_right_t=600)

        c2 = engine2.process_event(right("u1", 800))   # even better late match
        assert_retract_then_emit(c2, left_time=1000, old_right_t=600, new_right_t=800)


class TestKeyIsolationCorrections:
    def test_late_right_only_corrects_matching_key(self):
        engine = AsOfJoinEngine(lookback_window=60_000, right_lateness_bound=10_000)
        engine.process_event(left("u1", 1000))
        engine.process_event(left("u2", 1000))
        # Advance frontiers to 5000: watermark = -5000; R@500 is reclaimably late
        engine.process_event(right("u1", 5_000))
        engine.process_event(right("u2", 5_000))

        corrections = engine.process_event(right("u1", 500))
        corrected_keys = {c.left_event.key for c in corrections}
        assert "u1" in corrected_keys
        assert "u2" not in corrected_keys


class TestIrreparablyLateRight:
    def test_irreparably_late_right_discarded(self):
        engine = AsOfJoinEngine(lookback_window=60_000, right_lateness_bound=1_000)
        engine.process_event(left("u1", 1000))
        # Advance frontier to 10000; watermark = 9000
        engine.process_event(right("u1", 10_000))
        # Right at T=100 — 100 < watermark=9000 → irreparably late
        corrections = engine.process_event(right("u1", 100))
        assert corrections == []


class TestCorrectionRetractPayload:
    def test_retraction_carries_old_right_payload(self):
        """The retraction record must reference the originally matched right event."""
        engine = AsOfJoinEngine(lookback_window=60_000, right_lateness_bound=10_000)
        r_old = right("u1", 100, value="old")
        engine.process_event(r_old)
        engine.process_event(left("u1", 1000))
        # Advance frontier to 5000: watermark = -5000; R@800 is reclaimably late
        engine.process_event(right("u1", 5_000))

        r_late = right("u1", 800, value="late")
        corrections = engine.process_event(r_late)

        retract = next(c for c in corrections if c.retraction)
        emit = next(c for c in corrections if not c.retraction)

        assert retract.right_event.payload["value"] == "old"
        assert emit.right_event.payload["value"] == "late"
