"""
Benchmark: causal inversions and recovery latency under simulated clock drift.

Run with:
    python benchmarks/benchmark_drift.py
"""
from __future__ import annotations

import statistics
import time
from dataclasses import dataclass

from hlc_store.anomaly import find_causal_inversions
from hlc_store.clock import HybridLogicalClock, WallClock
from hlc_store.region import CausalEvent, Region
from hlc_store.timestamp import HLCTimestamp


# ─── helpers ──────────────────────────────────────────────────────────────────

def _build_chain(use_hlc: bool, drifts: list[int], n_rounds: int) -> list[CausalEvent]:
    """
    Build a causal chain of n_rounds across len(drifts) regions in round-robin.
    Region[i] writes, then replicates to Region[(i+1) % n_regions].
    """
    n = len(drifts)
    regions = [
        Region(f"r{i}", drift_ms=drifts[i], use_hlc=use_hlc) for i in range(n)
    ]

    prev_event = regions[0].write("k", "init")
    for round_i in range(n_rounds):
        src_idx = round_i % n
        dst_idx = (round_i + 1) % n
        src = regions[src_idx]
        dst = regions[dst_idx]
        write_ev = src.write("k", f"v{round_i}", caused_by_event=prev_event)
        prev_event = src.replicate_to(dst, "k", caused_by_event=write_ev)

    all_events: list[CausalEvent] = []
    for r in regions:
        all_events.extend(r.events())
    return all_events


# ─── Benchmark 1: inversion rate ──────────────────────────────────────────────

@dataclass
class InversionResult:
    label: str
    n_events: int
    n_inversions: int

    @property
    def pct(self) -> float:
        return 100 * self.n_inversions / max(self.n_events, 1)


def bench_inversion_rate(n_rounds: int = 200) -> list[InversionResult]:
    drift_scenarios = [
        ("low drift  (±50ms)",  [-50, 0, +50]),
        ("med drift  (±200ms)", [-200, 0, +200]),
        ("high drift (±500ms)", [-500, 0, +500]),
    ]
    results = []
    for label, drifts in drift_scenarios:
        for use_hlc, kind in [(False, "WallClock"), (True, "HLC     ")]:
            events = _build_chain(use_hlc, drifts, n_rounds)
            inversions = find_causal_inversions(events)
            results.append(InversionResult(
                label=f"{kind} | {label}",
                n_events=len(events),
                n_inversions=len(inversions),
            ))
    return results


# ─── Benchmark 2: recovery after NTP clock jump ───────────────────────────────

@dataclass
class RecoveryResult:
    drift_before_ms: int
    jump_ms: int            # how far the clock jumped backward
    events_to_recover: int  # events until HLC self-heals (no more inversions)


def bench_recovery(jump_magnitudes: list[int] | None = None) -> list[RecoveryResult]:
    if jump_magnitudes is None:
        jump_magnitudes = [100, 250, 500, 1000]

    results = []
    for jump_ms in jump_magnitudes:
        region = Region("primary", drift_ms=0, use_hlc=True)
        other = Region("replica", drift_ms=0, use_hlc=True)

        # Warm up
        for i in range(10):
            ev = region.write("k", f"warm{i}")
            region.replicate_to(other, "k", caused_by_event=ev)

        # Simulate NTP jump backward on `other`
        other.drift_ms = -jump_ms

        # Count how many events it takes before zero inversions in a rolling window
        window: list[CausalEvent] = []
        events_until_healed = 0
        for i in range(500):
            ev = region.write("k", f"post{i}")
            recv_ev = region.replicate_to(other, "k", caused_by_event=ev)
            window = region.events()[-20:] + other.events()[-20:]
            inversions = find_causal_inversions(window)
            if inversions:
                events_until_healed = i + 1
        # If no inversions found at all, recovery is immediate
        results.append(RecoveryResult(
            drift_before_ms=0,
            jump_ms=jump_ms,
            events_to_recover=events_until_healed,
        ))
    return results


# ─── Benchmark 3: throughput ──────────────────────────────────────────────────

def bench_throughput(n_ops: int = 5_000) -> dict[str, float]:
    hlc = HybridLogicalClock("bench")
    wall = WallClock("bench")

    start = time.perf_counter()
    for _ in range(n_ops):
        hlc.tick()
    hlc_time = time.perf_counter() - start

    start = time.perf_counter()
    for _ in range(n_ops):
        wall.tick()
    wall_time = time.perf_counter() - start

    return {
        "hlc_ops_per_sec": n_ops / hlc_time,
        "wall_ops_per_sec": n_ops / wall_time,
        "overhead_pct": 100 * (hlc_time - wall_time) / wall_time,
    }


# ─── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    bar = "─" * 70

    print(f"\n{'═' * 70}")
    print(" HLC Metadata Store — Benchmark Suite")
    print(f"{'═' * 70}\n")

    # 1. Inversion rate
    print("BENCHMARK 1 — Causal inversion rate under clock drift")
    print(bar)
    print(f"{'System & drift scenario':<38} {'events':>8} {'inversions':>12} {'rate':>8}")
    print(bar)
    for r in bench_inversion_rate(n_rounds=200):
        print(f"{r.label:<38} {r.n_events:>8} {r.n_inversions:>12} {r.pct:>7.1f}%")
    print()

    # 2. Recovery
    print("BENCHMARK 2 — HLC recovery after NTP clock jump (backward)")
    print(bar)
    print(f"{'Jump magnitude':>20}   {'Events until self-healed':>25}")
    print(bar)
    for r in bench_recovery([50, 100, 250, 500, 1000]):
        healed = r.events_to_recover if r.events_to_recover > 0 else "immediate"
        print(f"{r.jump_ms:>18}ms   {str(healed):>25}")
    print()

    # 3. Throughput
    print("BENCHMARK 3 — Tick throughput (single thread)")
    print(bar)
    t = bench_throughput(n_ops=10_000)
    print(f"  HLC  tick rate : {t['hlc_ops_per_sec']:>10,.0f} ops/sec")
    print(f"  Wall tick rate : {t['wall_ops_per_sec']:>10,.0f} ops/sec")
    print(f"  HLC overhead   : {t['overhead_pct']:>+9.1f}%")
    print()


if __name__ == "__main__":
    main()
