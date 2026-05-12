"""Reproduce the plan robustness results from the Bao paper.

Usage:
    python experiments/reproduce_bao.py \
        --dbname imdb --host localhost --port 5432 \
        --queries experiments/job_queries \
        --results results/bao_comparison.json \
        --profile-cache results/plan_profiles.json \
        --train-steps 200

The script:
  1. Loads JOB queries
  2. Profiles all 15 hint sets per query (or loads cache)
  3. Runs the Bao selector (online learning)
  4. Runs the baseline (PostgreSQL default)
  5. Prints the robustness report (Table 2 equivalent from the paper)
"""
from __future__ import annotations
import argparse
import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
)
logger = logging.getLogger("reproduce_bao")

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from cle.db.connector import DBConfig, ConnectionPool
from cle.plan.encoder import Vocabulary
from cle.model.trainer import Trainer, TrainConfig
from cle.bao.selector import BaoSelector
from cle.evaluation.benchmark import load_job_queries, run_comparison_benchmark
from cle.evaluation.robustness import PlanProfiler, robustness_report


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Reproduce Bao paper results")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=5432)
    p.add_argument("--dbname", default="imdb")
    p.add_argument("--user", default="postgres")
    p.add_argument("--password", default="postgres")
    p.add_argument("--queries", default="experiments/job_queries", type=Path)
    p.add_argument("--results", default="results/bao_comparison.json", type=Path)
    p.add_argument("--profile-cache", default="results/plan_profiles.json", type=Path)
    p.add_argument("--train-steps", type=int, default=200)
    p.add_argument("--hidden-size", type=int, default=128)
    p.add_argument("--timeout-ms", type=int, default=120_000)
    p.add_argument("--no-adaptive", action="store_true", help="Disable adaptive recompilation")
    p.add_argument("--no-neural-bandit", action="store_true", help="Use Bayesian bandit only")
    p.add_argument("--checkpoint", type=Path, default=None, help="Load model from checkpoint")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    db_config = DBConfig(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
    )
    pool = ConnectionPool(db_config)
    logger.info("Connected: %s", pool.get_pg_version()[:60])

    pool.enable_hint_plan()

    queries = load_job_queries(args.queries)
    if not queries:
        logger.error("No queries found in %s — run generate_job_sample.py first", args.queries)
        sys.exit(1)
    logger.info("Loaded %d queries", len(queries))

    # Build model + trainer
    vocab = Vocabulary()
    train_cfg = TrainConfig(
        hidden_size=args.hidden_size,
        checkpoint_dir="checkpoints",
    )
    trainer = Trainer(train_cfg)
    if args.checkpoint:
        trainer.load(args.checkpoint)

    # Build Bao selector
    selector = BaoSelector(
        pool=pool,
        trainer=trainer,
        vocab=vocab,
        use_neural_bandit=not args.no_neural_bandit,
        adaptive=not args.no_adaptive,
        default_timeout_ms=args.timeout_ms,
    )

    # ── Phase 1: Profile all hint sets (optional, expensive) ─────────────────
    profile_cache = args.profile_cache
    if profile_cache.exists():
        logger.info("Using plan profile cache: %s", profile_cache)
    else:
        logger.info("Profiling %d queries × 15 hint sets…", len(queries))
        profiler = PlanProfiler(pool, timeout_ms=args.timeout_ms)
        profiles = profiler.profile_workload(queries, cache_path=profile_cache)
        logger.info("Profiling complete")

    # ── Phase 2: Bao online learning run ─────────────────────────────────────
    logger.info("Running Bao workload (%d queries)…", len(queries))
    bao_results = selector.run_workload([sql for _, sql in queries])
    chosen_arms = {
        name: r.chosen_arm
        for (name, _), r in zip(queries, bao_results)
        if r is not None
    }

    # ── Phase 3: Final training pass ─────────────────────────────────────────
    if args.train_steps > 0:
        logger.info("Running %d offline training steps…", args.train_steps)
        history = trainer.train_epochs(args.train_steps)
        if history:
            last = history[-1]
            logger.info(
                "Training done — step=%d card_loss=%.4f",
                last.get("step"), last.get("card_loss", 0),
            )
    trainer.save("final")

    # ── Phase 4: Robustness report ───────────────────────────────────────────
    if profile_cache.exists():
        from cle.evaluation.robustness import _load_profiles
        profiles = _load_profiles(profile_cache)
        robustness_report(profiles, chosen_arms, baseline_arm=0)

    # ── Phase 5: Full comparison benchmark ───────────────────────────────────
    report = run_comparison_benchmark(
        pool=pool,
        bao_selector=selector,
        queries=queries,
        timeout_ms=args.timeout_ms,
        results_path=args.results,
    )

    pool.close()
    logger.info("Done. Results → %s", args.results)


if __name__ == "__main__":
    main()
