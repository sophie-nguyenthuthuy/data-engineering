import random
import pytest
from replay_engine.event import Event
from replay_engine.vector_clock import VectorClock
from replay_engine.udf_detector import UDFDetector, NonDeterminismError


def make_event(eid="e0", producer="P", seq=0):
    return Event(
        event_id=eid,
        producer_id=producer,
        sequence_num=seq,
        timestamp=0.0,
        vector_clock=VectorClock({producer: seq}),
        payload={"value": 42},
    )


class TestUDFDetector:
    def test_deterministic_udf_passes(self):
        def double(event):
            return event.payload["value"] * 2

        detector = UDFDetector("double", double, num_runs=3)
        result = detector(make_event())
        assert result == 84
        assert detector.violations() == []

    def test_non_deterministic_udf_raises(self):
        counter = {"n": 0}

        def flaky(event):
            counter["n"] += 1
            return counter["n"]  # different each call

        detector = UDFDetector("flaky", flaky, num_runs=2)
        with pytest.raises(NonDeterminismError) as exc_info:
            detector(make_event())
        assert exc_info.value.udf_name == "flaky"
        assert len(detector.violations()) == 1

    def test_report_structure(self):
        def identity(event):
            return event.event_id

        detector = UDFDetector("identity", identity, num_runs=2)
        detector(make_event("e0"))
        detector(make_event("e1", seq=1))
        report = detector.report()
        assert report["total_events_processed"] == 2
        assert report["total_violations"] == 0
        assert report["avg_latency_ms"] is not None

    def test_violation_records_event_id(self):
        call_count = {"n": 0}

        def flaky(event):
            call_count["n"] += 1
            if call_count["n"] % 2 == 0:
                return "second"
            return "first"

        detector = UDFDetector("flaky", flaky, num_runs=2)
        with pytest.raises(NonDeterminismError) as exc_info:
            detector(make_event("special"))
        assert exc_info.value.event_id == "special"
