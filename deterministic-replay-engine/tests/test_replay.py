import pytest
from replay_engine.event import Event, EventLog
from replay_engine.vector_clock import VectorClock
from replay_engine.replay import ReplayEngine


def make_event(eid, producer, seq, vc_dict=None, payload=None):
    vc = VectorClock(vc_dict or {producer: seq})
    return Event(
        event_id=eid,
        producer_id=producer,
        sequence_num=seq,
        timestamp=float(seq),
        vector_clock=vc,
        payload=payload or {},
    )


def simple_log():
    log = EventLog()
    log.append(make_event("a0", "A", 0))
    log.append(make_event("a1", "A", 1))
    log.append(make_event("b0", "B", 0, {"A": 0, "B": 0}))
    log.append(make_event("b1", "B", 1, {"A": 1, "B": 1}))
    return log


class TestReplayEngine:
    def test_basic_replay_succeeds(self):
        engine = ReplayEngine()
        result = engine.replay(simple_log())
        assert result.success
        assert len(result.ordered_events) == 4

    def test_causal_order_preserved(self):
        engine = ReplayEngine()
        result = engine.replay(simple_log())
        ids = [e.event_id for e in result.ordered_events]
        # a0 must precede b0 (b0 depends on A:0)
        assert ids.index("a0") < ids.index("b0")
        # a1 must precede b1 (b1 depends on A:1)
        assert ids.index("a1") < ids.index("b1")

    def test_udf_applied(self):
        def upper(event):
            return event.event_id.upper()

        engine = ReplayEngine(udfs={"upper": upper})
        result = engine.replay(simple_log())
        # Last step output should be the uppercased event_id
        assert result.steps[-1].output == result.steps[-1].event.event_id.upper()

    def test_non_deterministic_udf_fails(self):
        counter = {"n": 0}

        def flaky(event):
            counter["n"] += 1
            return counter["n"]

        engine = ReplayEngine(udfs={"flaky": flaky}, udf_runs=2)
        result = engine.replay(simple_log())
        assert not result.success
        assert result.udf_reports["flaky"]["total_violations"] > 0

    def test_duplicate_event_detected(self):
        log = EventLog()
        e = make_event("e0", "P", 0)
        log.append(e)
        # Manually inject a second event with same content hash via a clone
        import copy
        e2 = copy.deepcopy(e)
        e2_dict = e2.to_dict()
        e2_dict["event_id"] = "e0_dup"
        from replay_engine.event import Event as Ev
        dup = Ev.from_dict(e2_dict)
        log.append(dup)
        # Both events are unique by event_id, so no EventLog error, but
        # exactly-once tracker will flag the out-of-order for the second
        engine = ReplayEngine()
        result = engine.replay(log)
        # seq 0 then seq 0 again → out-of-order violation
        assert not result.success

    def test_summary_string(self):
        engine = ReplayEngine()
        result = engine.replay(simple_log())
        s = result.summary()
        assert "Replayed 4 events" in s

    def test_empty_log(self):
        engine = ReplayEngine()
        result = engine.replay(EventLog())
        assert result.success
        assert result.ordered_events == []

    def test_jsonl_roundtrip(self):
        log = simple_log()
        jsonl = log.to_jsonl()
        restored = EventLog.from_jsonl(jsonl)
        assert len(restored) == len(log)
        engine = ReplayEngine()
        result = engine.replay(restored)
        assert result.success
