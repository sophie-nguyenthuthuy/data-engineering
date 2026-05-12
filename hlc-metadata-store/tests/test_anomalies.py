"""
End-to-end tests proving HLC eliminates causal inversions that NTP can't prevent.

Each test runs the same causal scenario twice:
  - use_hlc=False  →  expect inversions
  - use_hlc=True   →  expect zero inversions
"""
from __future__ import annotations

import pytest

from hlc_store.anomaly import find_causal_inversions
from hlc_store.region import Region


def _run_three_region_chain(use_hlc: bool, drifts: tuple[int, int, int]):
    """
    Causal chain: us-east writes → replicates to eu-west → replicates to ap-south.
    Returns all CausalEvent objects from all three regions.
    """
    d_us, d_eu, d_ap = drifts
    us = Region("us-east", drift_ms=d_us, use_hlc=use_hlc)
    eu = Region("eu-west", drift_ms=d_eu, use_hlc=use_hlc)
    ap = Region("ap-south", drift_ms=d_ap, use_hlc=use_hlc)

    e1 = us.write("config", "v1")
    e2 = us.replicate_to(eu, "config", caused_by_event=e1)
    e3 = eu.replicate_to(ap, "config", caused_by_event=e2)

    return us.events() + eu.events() + ap.events()


class TestCausalInversionElimination:
    def test_hlc_no_inversions_moderate_drift(self):
        events = _run_three_region_chain(use_hlc=True, drifts=(+200, -150, +50))
        inversions = find_causal_inversions(events)
        assert inversions == [], "\n".join(str(i) for i in inversions)

    def test_hlc_no_inversions_extreme_drift(self):
        events = _run_three_region_chain(use_hlc=True, drifts=(+500, -500, +500))
        inversions = find_causal_inversions(events)
        assert inversions == [], "\n".join(str(i) for i in inversions)

    def test_wall_clock_produces_inversions_when_drifted(self):
        """
        With a heavily drifted wall clock, the source node (fast clock) stamps
        events higher than the replica nodes (slow clocks), causing inversions
        in the causal chain.
        """
        # us-east is 500ms AHEAD; replicas are at real time.
        # us-east tick → (t+500, 0); eu-west ignores that, stamps (t, 0) → INVERSION.
        events = _run_three_region_chain(use_hlc=False, drifts=(+500, 0, 0))
        inversions = find_causal_inversions(events)
        assert len(inversions) > 0, (
            "Expected wall clock to produce causal inversions under +500ms drift "
            "but found none — test scenario may need recalibration."
        )

    def test_hlc_no_inversions_with_clock_jump(self):
        """Simulate an NTP correction (clock jumps backward) mid-sequence."""
        us = Region("us-east", drift_ms=0, use_hlc=True)
        eu = Region("eu-west", drift_ms=0, use_hlc=True)

        e1 = us.write("schema", "v1")
        e2 = us.replicate_to(eu, "schema", caused_by_event=e1)

        # Simulate NTP correcting eu-west backward by 300ms
        eu.drift_ms = -300

        e3 = eu.write("schema", "v2", caused_by_event=None)
        e4 = eu.replicate_to(us, "schema", caused_by_event=e3)

        all_events = us.events() + eu.events()
        inversions = find_causal_inversions(all_events)
        assert inversions == [], "\n".join(str(i) for i in inversions)


class TestStaleReadProtection:
    def test_causal_get_never_returns_stale(self):
        """
        Client writes v2 at timestamp T, then reads back.
        causal_get(after=T) must return v2, not v1.
        """
        from hlc_store.clock import HybridLogicalClock
        from hlc_store.store import MetadataStore

        primary_clk = HybridLogicalClock("primary")
        primary = MetadataStore(primary_clk)
        replica_clk = HybridLogicalClock("replica", drift_ms=-200)
        replica = MetadataStore(replica_clk)

        _ = primary.put("endpoint", "http://old-host")
        write_ts = primary.put("endpoint", "http://new-host")

        # Replicate to replica with the source timestamp
        replica.put("endpoint", "http://new-host", remote_ts=write_ts)

        result = replica.causal_get("endpoint", after=write_ts, timeout_s=1.0)
        assert result is not None
        value, _ = result
        assert value == "http://new-host", f"Stale read! Got: {value}"
