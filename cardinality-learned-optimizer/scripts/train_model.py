"""Offline model training script.

Collects EXPLAIN ANALYZE samples for a set of queries, then trains
the QueryOptimizer model offline (without live query execution).

Usage:
    python scripts/train_model.py \
        --dbname imdb \
        --queries experiments/job_queries \
        --epochs 500 \
        --output checkpoints/pretrained.pt
"""
from __future__ import annotations
import argparse
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s — %(message)s")
logger = logging.getLogger("train_model")

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from cle.db.connector import DBConfig, ConnectionPool
from cle.db.interceptor import QueryInterceptor
from cle.plan.encoder import Vocabulary, encode_tree
from cle.model.trainer import Trainer, TrainConfig
from cle.evaluation.benchmark import load_job_queries
from cle.evaluation.metrics import workload_q_error_stats, print_metric_table


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=5432)
    p.add_argument("--dbname", default="imdb")
    p.add_argument("--user", default="postgres")
    p.add_argument("--password", default="postgres")
    p.add_argument("--queries", default="experiments/job_queries", type=Path)
    p.add_argument("--epochs", type=int, default=500)
    p.add_argument("--hidden-size", type=int, default=128)
    p.add_argument("--output", default="checkpoints/pretrained.pt", type=Path)
    p.add_argument("--timeout-ms", type=int, default=60_000)
    return p.parse_args()


def main() -> None:
    args = parse_args()

    pool = ConnectionPool(DBConfig(
        host=args.host, port=args.port, dbname=args.dbname,
        user=args.user, password=args.password,
    ))
    interceptor = QueryInterceptor(pool)
    queries = load_job_queries(args.queries)
    if not queries:
        logger.error("No queries found in %s", args.queries)
        sys.exit(1)

    vocab = Vocabulary()
    trainer = Trainer(TrainConfig(hidden_size=args.hidden_size))

    # Collect samples
    logger.info("Collecting EXPLAIN ANALYZE samples for %d queries…", len(queries))
    analyzed_trees = []
    for name, sql in queries:
        try:
            plan, _ = interceptor.explain_analyze(sql, args.timeout_ms)
            vocab.update_from_node(plan)
            tree = encode_tree(plan, vocab, include_actuals=True)
            trainer.add_cardinality_sample(tree)
            analyzed_trees.append(tree)
            logger.info("  ✓ %s (%d nodes)", name, tree.n)
        except Exception as e:
            logger.warning("  ✗ %s: %s", name, e)

    logger.info("Collected %d trees. Training for %d steps…", len(analyzed_trees), args.epochs)
    history = trainer.train_epochs(args.epochs)

    # Evaluate q-error
    stats = trainer.q_error_stats(analyzed_trees)
    print_metric_table(stats, "Training Set Q-Error After Training")

    trainer.save("pretrained")
    if args.output != Path("checkpoints/pretrained.pt"):
        import shutil
        shutil.copy("checkpoints/model_pretrained.pt", args.output)

    pool.close()
    logger.info("Model saved → checkpoints/model_pretrained.pt")


if __name__ == "__main__":
    main()
