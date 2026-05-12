"""Core engine integration tests."""

import pytest
import numpy as np

from cep import CEPEngine, Pattern, make_event


class E:
    A, B, C, D = 1, 2, 3, 4


SEC = 1_000_000_000  # 1 second in ns


@pytest.fixture
def three_step_pattern():
    return (
        Pattern("seq_abc")
        .begin(E.A)
        .then(E.B, max_gap_ns=5 * SEC)
        .then(E.C, max_gap_ns=5 * SEC)
        .total_window(20 * SEC)
    )


@pytest.fixture
def engine(three_step_pattern):
    eng = CEPEngine()
    eng.register(three_step_pattern)
    return eng


# ------------------------------------------------------------------

def push_seq(engine, types, base_ts=1_000 * SEC, gap_ns=SEC, entity=1):
    t = base_ts
    results = []
    for tp in types:
        results.append(engine.push(make_event(tp, entity, timestamp=t)))
        t += gap_ns
    return results


class TestBasicSequence:
    def test_match_fires(self, engine):
        results = push_seq(engine, [E.A, E.B, E.C])
        fired = [r for r in results if r]
        assert len(fired) == 1
        assert fired[0][0] == ("seq_abc", 1)

    def test_no_match_wrong_order(self, engine):
        results = push_seq(engine, [E.B, E.A, E.C])
        assert all(r == [] for r in results)

    def test_no_match_missing_step(self, engine):
        results = push_seq(engine, [E.A, E.C])
        assert all(r == [] for r in results)

    def test_interleaved_noise(self, engine):
        results = push_seq(engine, [E.A, E.D, E.D, E.B, E.D, E.C])
        fired = [r for r in results if r]
        assert len(fired) == 1

    def test_multiple_entities_independent(self, engine):
        t = 1000 * SEC
        for eid in [10, 20, 30]:
            for tp in [E.A, E.B, E.C]:
                engine.push(make_event(tp, eid, timestamp=t))
                t += SEC

        # Each entity should have produced exactly one match
        fired: list = []
        engine2 = CEPEngine()
        p = Pattern("seq_abc2").begin(E.A).then(E.B).then(E.C).total_window(20 * SEC)
        engine2.register(p)
        engine2.add_callback("seq_abc2", lambda eid, pn, ts: fired.append(eid))

        t = 1000 * SEC
        for eid in [10, 20, 30]:
            for tp in [E.A, E.B, E.C]:
                engine2.push(make_event(tp, eid, timestamp=t))
                t += SEC
        assert sorted(fired) == [10, 20, 30]


class TestWindowExpiry:
    def test_total_window_resets(self, engine):
        base = 1000 * SEC
        engine.push(make_event(E.A, 1, timestamp=base))
        engine.push(make_event(E.B, 1, timestamp=base + 3 * SEC))
        # C arrives after total window (20s) expires
        engine.push(make_event(E.C, 1, timestamp=base + 25 * SEC))
        state = engine.entity_state("seq_abc", 1)
        assert state["step"] == 0  # reset, no match

    def test_gap_window_resets_step(self):
        p = (
            Pattern("gap_test")
            .begin(E.A)
            .then(E.B, max_gap_ns=2 * SEC)
            .then(E.C, max_gap_ns=2 * SEC)
            .total_window(30 * SEC)
        )
        eng = CEPEngine()
        eng.register(p)
        base = 1000 * SEC
        eng.push(make_event(E.A, 1, timestamp=base))
        eng.push(make_event(E.B, 1, timestamp=base + 1 * SEC))
        # C arrives 10s after B — exceeds max_gap
        # Next A should re-anchor
        eng.push(make_event(E.C, 1, timestamp=base + 11 * SEC))
        state = eng.entity_state("gap_test", 1)
        # Still at step 2 (total window not expired) but C arrived too late
        # pattern should either not match or be in reset state
        fired = eng.push(make_event(E.C, 1, timestamp=base + 12 * SEC))
        assert fired == []


class TestCountStep:
    def test_count_required(self):
        p = (
            Pattern("count_test")
            .begin(E.A, count=3)
            .then(E.B)
            .total_window(30 * SEC)
        )
        eng = CEPEngine()
        eng.register(p)
        base = 1000 * SEC
        eng.push(make_event(E.A, 1, timestamp=base))
        eng.push(make_event(E.A, 1, timestamp=base + SEC))
        # Only 2 A's — B should not advance
        r = eng.push(make_event(E.B, 1, timestamp=base + 2 * SEC))
        assert r == []
        # Third A
        eng.push(make_event(E.A, 1, timestamp=base + 3 * SEC))
        # Now B should match
        r = eng.push(make_event(E.B, 1, timestamp=base + 4 * SEC))
        assert len(r) == 1

    def test_count_resets_across_entities(self):
        p = Pattern("cnt2").begin(E.A, count=2).then(E.B).total_window(20 * SEC)
        eng = CEPEngine()
        eng.register(p)
        base = 1000 * SEC
        # Entity 1
        eng.push(make_event(E.A, 1, timestamp=base))
        eng.push(make_event(E.A, 1, timestamp=base + SEC))
        r1 = eng.push(make_event(E.B, 1, timestamp=base + 2 * SEC))
        # Entity 2 has only seen 1 A
        eng.push(make_event(E.A, 2, timestamp=base))
        r2 = eng.push(make_event(E.B, 2, timestamp=base + SEC))
        assert len(r1) == 1
        assert r2 == []


class TestCallbacks:
    def test_callback_invoked(self, engine):
        received = []
        engine.on_match("seq_abc")(lambda eid, pn, ts: received.append((eid, pn)))
        push_seq(engine, [E.A, E.B, E.C], entity=99)
        assert received == [(99, "seq_abc")]

    def test_multiple_callbacks(self, engine):
        log: list = []
        engine.add_callback("seq_abc", lambda eid, pn, ts: log.append("cb1"))
        engine.add_callback("seq_abc", lambda eid, pn, ts: log.append("cb2"))
        push_seq(engine, [E.A, E.B, E.C], entity=77)
        assert log == ["cb1", "cb2"]


class TestBuffer:
    def test_buffer_stores_events(self, engine):
        push_seq(engine, [E.A, E.B, E.C], entity=1)
        recent = engine.buffer.read_recent(10)
        assert len(recent) == 3

    def test_ring_wraps(self):
        from cep.buffer import RingBuffer
        buf = RingBuffer(capacity=4)
        for i in range(6):
            buf.push(make_event(E.A, i, timestamp=i * SEC))
        recent = buf.read_recent(4)
        assert len(recent) == 4
        buf.close()
