import pytest
from replay_engine.event import Event
from replay_engine.vector_clock import VectorClock
from replay_engine.exactly_once import ExactlyOnceTracker, ViolationKind


def make_event(eid, producer, seq, vc_dict=None):
    vc = VectorClock(vc_dict or {producer: seq})
    return Event(
        event_id=eid,
        producer_id=producer,
        sequence_num=seq,
        timestamp=float(seq),
        vector_clock=vc,
        payload={},
    )


class TestExactlyOnceTracker:
    def test_clean_log_no_violations(self):
        tracker = ExactlyOnceTracker()
        tracker.track(make_event("e0", "P", 0))
        tracker.track(make_event("e1", "P", 1))
        assert tracker.violations() == []

    def test_duplicate_delivery(self):
        tracker = ExactlyOnceTracker()
        e = make_event("e0", "P", 0)
        tracker.track(e)
        violations = tracker.track(e)
        assert len(violations) == 1
        assert violations[0].kind == ViolationKind.DUPLICATE_DELIVERY

    def test_missing_predecessor(self):
        """Track event that depends on another producer's seq=0 without tracking it first."""
        tracker = ExactlyOnceTracker()
        # B0 depends on A at seq 0 but A hasn't been tracked yet
        b0 = make_event("b0", "B", 0, {"A": 0, "B": 0})
        violations = tracker.track(b0)
        assert any(v.kind == ViolationKind.MISSING_PREDECESSOR for v in violations)

    def test_no_missing_predecessor_when_satisfied(self):
        tracker = ExactlyOnceTracker()
        a0 = make_event("a0", "A", 0, {"A": 0})
        b0 = make_event("b0", "B", 0, {"A": 0, "B": 0})
        tracker.track(a0)
        violations = tracker.track(b0)
        assert not any(v.kind == ViolationKind.MISSING_PREDECESSOR for v in violations)

    def test_out_of_order_within_producer(self):
        tracker = ExactlyOnceTracker()
        tracker.track(make_event("e0", "P", 0))
        # Skip seq=1, jump to seq=2
        violations = tracker.track(make_event("e2", "P", 2))
        assert any(v.kind == ViolationKind.OUT_OF_ORDER for v in violations)

    def test_report_counts(self):
        tracker = ExactlyOnceTracker()
        e = make_event("e0", "P", 0)
        tracker.track(e)
        tracker.track(e)  # duplicate
        report = tracker.report()
        assert report["processed_count"] == 1
        assert report["total_violations"] == 1
        assert "DUPLICATE_DELIVERY" in report["by_kind"]
