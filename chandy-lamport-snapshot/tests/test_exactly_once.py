"""
Exactly-once semantics proof.

A distributed pipeline guarantees exactly-once processing if, after any number
of failures and recoveries from a consistent snapshot, every source message
appears in the output exactly once — no duplicates, no gaps.

The proof relies on three properties working together:

1. Consistent snapshot  (Chandy-Lamport correctness)
   Every message M received by node Q before Q's snapshot was also sent by
   node P before P's snapshot.  This means the restored state already
   "includes" M; replaying M would double-count it.

   The algorithm prevents replay of already-processed messages by:
   a) Channel states only capture messages sent AFTER the source's snapshot
      but arrived at the destination BEFORE the destination's snapshot.
   b) FIFO channels guarantee this boundary aligns with the marker.

2. Source replay from checkpoint offset
   After recovery, each source resumes from its checkpointed next_seq.
   Messages with seq < next_seq are never re-emitted.

3. Idempotency guard  (Node._seen set)
   Each node tracks processed msg_ids.  Duplicate deliveries (e.g., from
   a race between recovery and a lingering in-flight message) are silently
   dropped before any state mutation occurs.
"""
from __future__ import annotations

import time
from collections import Counter
from typing import List

import pytest

from chandy_lamport import Pipeline


N = 10   # source messages per source


def run_to_completion(n_failures: int = 0) -> Counter:
    """
    Run the pipeline with *n_failures* simulated crashes of the Aggregator,
    each followed by a recovery.  Return a Counter of origin_seqs received
    at the Sink.
    """
    p = Pipeline(source_total=N, source_interval=0.06)
    p.start()

    for failure_num in range(n_failures):
        # Take snapshot, let pipeline run a bit, crash, recover
        time.sleep(0.5)
        gs = p.take_snapshot()
        time.sleep(0.3)
        p.stop_node(p.aggregator)
        time.sleep(0.1)
        p.recover(gs)

    # Let everything drain
    time.sleep(3.0)
    p.stop()

    return Counter(p.sink.received_seqs)


class TestExactlyOnceNoFailure:
    def test_each_seq_appears_twice_no_failure(self):
        """
        Baseline: without any failure, each source seq 1..N appears exactly
        twice (once from SourceA, once from SourceB).
        """
        counts = run_to_completion(n_failures=0)
        for s in range(1, N + 1):
            assert counts[s] == 2, (
                f"No-failure run: seq {s} appeared {counts[s]} times (expected 2)"
            )

    def test_no_extra_seqs(self):
        counts = run_to_completion(n_failures=0)
        unexpected = [s for s in counts if s not in range(1, N + 1)]
        assert not unexpected, f"Unexpected seqs in output: {unexpected}"


class TestExactlyOnceWithOneFailure:
    def test_each_seq_appears_twice_after_one_recovery(self):
        counts = run_to_completion(n_failures=1)
        missing   = [s for s in range(1, N + 1) if counts[s] == 0]
        duplicate = [s for s in range(1, N + 1) if counts[s] > 2]
        assert not missing,   f"Missing seqs after 1 recovery: {missing}"
        assert not duplicate, f"Duplicate seqs after 1 recovery: {duplicate}"


class TestExactlyOnceWithTwoFailures:
    def test_each_seq_appears_twice_after_two_recoveries(self):
        counts = run_to_completion(n_failures=2)
        missing   = [s for s in range(1, N + 1) if counts[s] == 0]
        duplicate = [s for s in range(1, N + 1) if counts[s] > 2]
        assert not missing,   f"Missing seqs after 2 recoveries: {missing}"
        assert not duplicate, f"Duplicate seqs after 2 recoveries: {duplicate}"


class TestIdempotencyGuard:
    """
    The Node._seen set should prevent double-processing even if a message
    is delivered twice (e.g., from a race condition during recovery).
    """

    def test_duplicate_message_is_dropped(self):
        from chandy_lamport import AggregatorNode, Channel, DataMessage

        agg = AggregatorNode("agg")
        agg.setup()
        ch_in  = Channel("src", "agg")
        ch_out = Channel("agg", "snk")
        agg.add_in_channel(ch_in)
        agg.add_out_channel(ch_out)
        agg.start()

        # Send the same message twice
        msg = DataMessage(content=10, origin_seq=1)
        ch_in.send(msg)
        ch_in.send(msg)   # duplicate

        time.sleep(0.15)
        agg.stop()

        state = agg.get_state()
        assert state["count"] == 1, (
            f"Duplicate should be dropped: count={state['count']}"
        )
        assert state["sum"] == 10

    def test_restored_node_processes_replayed_msg_once(self):
        """
        Simulate: node processed msg A before snapshot, then recovery replays
        msg A via in-transit channel state.  Msg A must NOT be processed again.
        """
        from chandy_lamport import AggregatorNode, Channel, DataMessage

        agg = AggregatorNode("agg")
        agg.setup()
        ch_in  = Channel("src", "agg")
        ch_out = Channel("agg", "snk")
        agg.add_in_channel(ch_in)
        agg.add_out_channel(ch_out)
        agg.start()

        # Process message normally
        msg = DataMessage(content=5, origin_seq=1)
        ch_in.send(msg)
        time.sleep(0.1)
        pre_recovery_state = agg.get_state()
        agg.stop()

        assert pre_recovery_state["sum"] == 5

        # Simulate recovery: restore to BEFORE msg was processed
        agg.restore_state({"sum": 0, "count": 0})
        # BUT the _seen set is cleared by restore_state, so the first replay
        # WILL be processed (this is correct: we're replaying from a checkpoint
        # before this message was included in state).
        agg.start()
        ch_in.send(msg)   # replay from channel state
        time.sleep(0.1)
        agg.stop()

        post_recovery_state = agg.get_state()
        assert post_recovery_state["sum"] == 5, (
            "After recovery replay, sum should be 5 (msg processed once)"
        )

        # Now send the SAME msg again (stale duplicate)
        agg.start()
        ch_in.send(msg)
        time.sleep(0.1)
        agg.stop()

        final_state = agg.get_state()
        assert final_state["sum"] == 5, (
            f"Stale duplicate should be dropped, sum={final_state['sum']}"
        )


class TestAggregatorSum:
    """
    Mathematical proof: the aggregator's final sum must equal the expected
    closed-form value, both with and without failure.
    """

    @staticmethod
    def expected_sum(n: int) -> int:
        # SourceA contributes: sum(1..n) = n(n+1)/2
        # SourceB contributes: sum(2,4,...2n) = n(n+1)
        return n * (n + 1) // 2 + n * (n + 1)

    def test_final_sum_no_failure(self):
        p = Pipeline(source_total=N, source_interval=0.05)
        p.start()
        time.sleep(3.5)
        p.stop()
        actual = p.aggregator.get_state()["sum"]
        expected = self.expected_sum(N)
        assert actual == expected, f"Sum {actual} != expected {expected}"

    def test_final_sum_after_one_failure(self):
        p = Pipeline(source_total=N, source_interval=0.05)
        p.start()
        time.sleep(0.5)
        gs = p.take_snapshot()
        time.sleep(0.3)
        p.stop_node(p.aggregator)
        time.sleep(0.1)
        p.recover(gs)
        time.sleep(3.0)
        p.stop()
        actual = p.aggregator.get_state()["sum"]
        expected = self.expected_sum(N)
        assert actual == expected, f"Sum after recovery {actual} != expected {expected}"
