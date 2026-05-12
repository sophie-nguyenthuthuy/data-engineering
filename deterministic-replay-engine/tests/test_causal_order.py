import pytest
from replay_engine.event import Event
from replay_engine.vector_clock import VectorClock
from replay_engine.causal_order import causal_sort, validate_monotone_sequences, CausalOrderError


def make_event(event_id, producer, seq, vc_dict, payload=None):
    return Event(
        event_id=event_id,
        producer_id=producer,
        sequence_num=seq,
        timestamp=float(seq),
        vector_clock=VectorClock(vc_dict),
        payload=payload or {},
    )


class TestCausalSort:
    def test_single_producer_ordered(self):
        events = [
            make_event("e2", "P", 1, {"P": 1}),
            make_event("e1", "P", 0, {"P": 0}),
        ]
        result = causal_sort(events)
        assert [e.event_id for e in result] == ["e1", "e2"]

    def test_two_producers_independent(self):
        """Events from two producers with no causal link; order is deterministic by (producer, seq)."""
        events = [
            make_event("b0", "B", 0, {"B": 0}),
            make_event("a0", "A", 0, {"A": 0}),
        ]
        result = causal_sort(events)
        # A < B lexicographically, so a0 comes first
        assert result[0].event_id == "a0"
        assert result[1].event_id == "b0"

    def test_cross_producer_dependency(self):
        """B0 depends on A0 (vector clock of B0 has A:0)."""
        a0 = make_event("a0", "A", 0, {"A": 0})
        b0 = make_event("b0", "B", 0, {"A": 0, "B": 0})  # B0 saw A at seq 0
        events = [b0, a0]
        result = causal_sort(events)
        assert result[0].event_id == "a0"
        assert result[1].event_id == "b0"

    def test_chain_of_dependencies(self):
        a0 = make_event("a0", "A", 0, {"A": 0})
        b0 = make_event("b0", "B", 0, {"A": 0, "B": 0})
        c0 = make_event("c0", "C", 0, {"A": 0, "B": 0, "C": 0})
        result = causal_sort([c0, b0, a0])
        ids = [e.event_id for e in result]
        assert ids.index("a0") < ids.index("b0")
        assert ids.index("b0") < ids.index("c0")

    def test_empty(self):
        assert causal_sort([]) == []

    def test_deterministic_across_input_orderings(self):
        events = [
            make_event("b0", "B", 0, {"B": 0}),
            make_event("a0", "A", 0, {"A": 0}),
            make_event("a1", "A", 1, {"A": 1}),
        ]
        import itertools
        results = set()
        for perm in itertools.permutations(events):
            r = causal_sort(list(perm))
            results.add(tuple(e.event_id for e in r))
        assert len(results) == 1, f"Non-deterministic: {results}"


class TestValidateMonotoneSequences:
    def test_valid(self):
        events = [
            make_event("e0", "P", 0, {"P": 0}),
            make_event("e1", "P", 1, {"P": 1}),
        ]
        assert validate_monotone_sequences(events) == []

    def test_gap(self):
        events = [
            make_event("e0", "P", 0, {"P": 0}),
            make_event("e2", "P", 2, {"P": 2}),
        ]
        errors = validate_monotone_sequences(events)
        assert len(errors) == 1
        assert "expected seq 1" in errors[0]
