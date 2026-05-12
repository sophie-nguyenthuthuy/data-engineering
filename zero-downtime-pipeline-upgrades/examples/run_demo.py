"""
Demo: zero-downtime upgrade from WordCountV1 → WordCountV2.

Run from the repo root:
    python -m examples.run_demo

What to watch for
-----------------
* Phase 1  — only v1 serves traffic; v2 runs in shadow. Divergence is logged
             for every record that differs.
* Phase 2  — once 100 clean records are accumulated the shifter steps traffic
             toward v2 in 20 % increments.
* Promotion — at 100 % v2 the orchestrator logs "PROMOTED".
* The final status printout shows the full shift history.
"""

import logging
import random
import sys
import time
import os

# Make sure the repo root is on the path when running directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pipeline_deployer import DeploymentConfig, DeploymentOrchestrator

from examples.word_count_v1 import WordCountV1
from examples.word_count_v2 import WordCountV2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)

# ── Sentence bank ──────────────────────────────────────────────────────────────
CLEAN_SENTENCES = [
    "the quick brown fox jumps over the lazy dog",
    "stream processing enables real time data pipelines",
    "blue green deployments reduce risk during upgrades",
    "shadow mode runs the candidate version invisibly",
    "divergence tracking compares outputs field by field",
]

# Sentences with punctuation — v1 and v2 will differ on these
PUNCTUATED_SENTENCES = [
    "Hello, world! How are you today?",
    "It's a bird — no, it's a plane!",
    "We sell: apples, oranges, and bananas.",
    "The 2024-01-15 release fixed 3 bugs (see changelog).",
    "\"To be, or not to be\" — that is the question.",
]


def generate_stream(n: int = 600):
    """Yield n synthetic records; ~30 % have punctuation."""
    for i in range(n):
        if random.random() < 0.30:
            text = random.choice(PUNCTUATED_SENTENCES)
        else:
            text = random.choice(CLEAN_SENTENCES)
        yield {"doc_id": f"doc-{i:04d}", "text": text}


def main():
    print("\n" + "=" * 65)
    print("  Zero-Downtime Pipeline Upgrade Demo")
    print("  WordCountV1 (whitespace split) → WordCountV2 (regex tokeniser)")
    print("=" * 65 + "\n")

    config = DeploymentConfig(
        divergence_threshold=0.15,       # allow up to 15 % divergence rate
        rollback_threshold=0.60,         # rollback only if > 60 % diverge
        traffic_shift_step=0.20,         # 20 % steps
        traffic_shift_interval_sec=3.0,  # shift every 3 s (demo speed)
        min_samples_for_promotion=100,   # need 100 shadow samples first
        comparison_window_size=200,
        enable_auto_promotion=True,
        enable_auto_rollback=True,
    )

    orchestrator = DeploymentOrchestrator(
        v1=WordCountV1(),
        v2=WordCountV2(),
        config=config,
    )

    orchestrator.start()

    processed = 0
    for output in orchestrator.process_stream(generate_stream(600), progress_every=100):
        processed += 1
        # Simulate realistic inter-record latency
        time.sleep(0.01)

    print(f"\nProcessed {processed} records total.\n")

    final = orchestrator.complete()

    print("\n" + "=" * 65)
    print("  Final Deployment Status")
    print("=" * 65)
    stats = final["runner_stats"]
    print(f"  State            : {final['state']}")
    print(f"  Promoted         : {final['promoted']}")
    print(f"  Rolled back      : {final['rolled_back']}")
    print(f"  Records processed: {stats['records_processed']}")
    print(f"  v2 traffic %     : {stats['v2_percentage'] * 100:.0f}%")
    print(f"  Divergence rate  : {stats['window_divergence_rate'] * 100:.2f}%")
    print(f"  Mean div. score  : {stats['mean_divergence_score']:.4f}")
    print(f"  v1 errors        : {stats['v1_errors']}")
    print(f"  v2 errors        : {stats['v2_errors']}")
    print(f"  Elapsed          : {final['elapsed_seconds']}s")

    print("\n  Shift History:")
    for event in final["shift_history"]:
        ts = time.strftime("%H:%M:%S", time.localtime(event["ts"]))
        v2pct = event.get("v2_percentage", 0) * 100
        div = event.get("divergence", None)
        div_str = f"  divergence={div:.2%}" if div is not None else ""
        reason = event.get("reason", "")
        reason_str = f"  reason={reason}" if reason else ""
        print(f"    [{ts}] {event['event']:15s}  v2={v2pct:.0f}%{div_str}{reason_str}")

    print()


if __name__ == "__main__":
    main()
