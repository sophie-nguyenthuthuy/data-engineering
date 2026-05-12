#!/usr/bin/env python3
"""
End-to-end demo of the Tiered Storage Orchestrator using in-process fake tiers.

Run with:
    python examples/demo.py

No Redis, Postgres, or S3 required — everything runs in memory / temp files.
"""
from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

# Make sure the project root is on the path when running directly
sys.path.insert(0, str(Path(__file__).parent.parent))

from tiered_storage.config import StorageConfig
from tiered_storage.cost_model import CostConfig, CostModel, TierUsage
from tiered_storage.lifecycle import LifecycleEngine
from tiered_storage.orchestrator import TieredStorageOrchestrator
from tiered_storage.rehydration import RehydrationManager
from tiered_storage.router import ReadRouter
from tiered_storage.schemas import LifecyclePolicy, RehydrationPriority, Tier, TierMetrics
from tiered_storage.tiers.cold import ColdTier
from tiered_storage.tracking.access_patterns import AccessPatternTracker

# Re-use the FakeTier from tests
import importlib, types
spec = importlib.util.spec_from_file_location(
    "conftest", Path(__file__).parent.parent / "tests" / "conftest.py"
)
conftest = importlib.util.module_from_spec(spec)
spec.loader.exec_module(conftest)
FakeTier = conftest.FakeTier
make_record = conftest.make_record

SEP = "─" * 60


def banner(text: str) -> None:
    print(f"\n{SEP}\n  {text}\n{SEP}")


async def main() -> None:
    import tempfile, os
    tmpdir = tempfile.mkdtemp(prefix="tso_demo_")

    # ── Build stack ───────────────────────────────────────────────────
    hot  = FakeTier(Tier.HOT)
    warm = FakeTier(Tier.WARM)
    cold = ColdTier(local_path=os.path.join(tmpdir, "cold"))
    cold.connect()

    tracker = AccessPatternTracker()
    policy  = LifecyclePolicy(
        hot_to_warm_idle_days=1,
        warm_to_cold_idle_days=2,
        hot_min_access_freq=0.5,
        warm_min_access_freq=0.1,
    )
    rehydration = RehydrationManager(cold_tier=cold, warm_tier=warm, hot_tier=hot)
    lifecycle   = LifecycleEngine(policy, hot, warm, cold, tracker)
    router      = ReadRouter(
        hot_tier=hot,
        warm_tier=warm,
        cold_tier=cold,
        rehydration_manager=rehydration,
        tracker=tracker,
        promote_freq_threshold=3.0,
        block_on_cold=True,
    )

    # ── 1. Write data ─────────────────────────────────────────────────
    banner("1. Writing 5 records to hot tier")
    for i in range(1, 6):
        rec = make_record(f"user:{i}", value={"name": f"User {i}", "score": i * 10})
        await router.put(rec)
        print(f"  PUT user:{i} → hot")

    # ── 2. Read all keys (should all be HOT hits) ─────────────────────
    banner("2. Reading all keys (expect HOT hits)")
    for i in range(1, 6):
        result = await router.get(f"user:{i}")
        print(f"  GET user:{i} → tier={result.tier_hit.value:<4}  latency={result.latency_ms:.2f}ms")

    print(f"\n  Router stats: {router.stats.summary()}")

    # ── 3. Age two records and run lifecycle ──────────────────────────
    banner("3. Aging user:1 and user:2 → lifecycle demotes them to warm")
    for key in ("user:1", "user:2"):
        rec = hot._store[key]
        rec.last_accessed_at = time.time() - 2 * 86400   # 2 days idle
        rec.created_at       = time.time() - 2 * 86400

    report = await lifecycle.run_cycle()
    print(f"  {report.summary()}")
    for m in report.hot_to_warm:
        print(f"  hot→warm: {m.key}  reason={m.reason}")

    # ── 4. Read demoted key (should be WARM hit) ──────────────────────
    banner("4. Reading user:1 after demotion (expect WARM hit)")
    result = await router.get("user:1")
    print(f"  GET user:1 → tier={result.tier_hit.value}  latency={result.latency_ms:.2f}ms")

    # ── 5. Age warm records and push to cold ──────────────────────────
    banner("5. Aging warm records → lifecycle pushes them to cold")
    for key in ("user:1", "user:2"):
        if key in warm._store:
            rec = warm._store[key]
            rec.last_accessed_at = time.time() - 3 * 86400
            rec.created_at       = time.time() - 3 * 86400

    report2 = await lifecycle.run_cycle()
    print(f"  {report2.summary()}")
    for m in report2.warm_to_cold:
        print(f"  warm→cold: {m.key}  reason={m.reason}")

    # ── 6. Cold read → triggers rehydration ──────────────────────────
    banner("6. Reading user:1 from cold tier (triggers rehydration)")
    result = await router.get("user:1", block_on_cold=False)
    if result.rehydration_job:
        job = result.rehydration_job
        print(f"  Rehydration job queued: {job.job_id}")
        print(f"  Priority={job.priority.value}  SLA in {job.eta_seconds:.0f}s")
        # Drain the queue inline
        processed = await rehydration.run_once()
        print(f"  Processed {processed} restore job(s)")
        warm_rec = await warm.get("user:1")
        print(f"  user:1 now on warm tier: {warm_rec is not None}")

    # ── 7. SLA report ────────────────────────────────────────────────
    banner("7. Rehydration SLA report")
    sla = rehydration.sla_report()
    for k, v in sla.items():
        print(f"  {k}: {v}")

    # ── 8. Cost model ────────────────────────────────────────────────
    banner("8. Monthly cost projection")
    hot_m  = await hot.metrics()
    warm_m = await warm.metrics()
    cold_m = await cold.metrics()

    model = CostModel(CostConfig())
    breakdown = model.project_from_metrics(
        hot_m, warm_m, cold_m,
        hot_reads_per_day=5000,
        warm_reads_per_day=500,
        cold_reads_per_day=20,
    )
    print(breakdown.summary())
    print(f"\n  Savings from warm tier vs keeping on hot:")
    savings = model.savings_from_demotion(warm_m.total_size_bytes / 1e9, "hot_postgres", "warm")
    print(f"    ${savings:.4f}/month saved on {warm_m.total_size_bytes/1e9:.4f} GB")

    # ── 9. Access pattern summary ────────────────────────────────────
    banner("9. Access pattern tracker — top accessed keys")
    hot_keys = tracker.hottest_keys(5)
    for s in hot_keys:
        print(f"  {s.key:<12} accesses={s.access_count}  ema_freq={s.ema_freq:.3f}/day")

    print(f"\n{SEP}\n  Demo complete!\n{SEP}\n")


if __name__ == "__main__":
    asyncio.run(main())
