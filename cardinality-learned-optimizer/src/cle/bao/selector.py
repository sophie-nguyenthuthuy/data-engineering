"""Bao plan selector: wrap the full Neo/Bao loop.

For each incoming query:
  1. Generate the default plan (hint_id=0)
  2. Ask the bandit which hint set to use
  3. Execute the chosen plan
  4. Feed actual latency back to both the bandit and the trainer
  5. If adaptive recompile fires, record the speedup separately
"""
from __future__ import annotations
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from ..db.connector import ConnectionPool
from ..db.hint_injector import BAO_HINT_SETS, hint_set_to_pg_hints, reset_hint_set
from ..db.interceptor import QueryInterceptor, ExecutionRecord
from ..adaptive.recompiler import AdaptiveRecompiler, RecompileResult
from ..plan.encoder import Vocabulary, encode_tree
from ..model.gnn import QueryOptimizer
from ..model.trainer import Trainer
from .bandit import ThompsonSamplingBandit, NeuralBandit

logger = logging.getLogger(__name__)


@dataclass
class BaoResult:
    sql: str
    chosen_arm: int
    hint_set: dict
    record: ExecutionRecord
    recompile_result: Optional[RecompileResult]
    latency_ms: float
    speedup_vs_default: float = 1.0
    adaptive_speedup: float = 1.0


class BaoSelector:
    """Full Bao optimizer loop."""

    def __init__(
        self,
        pool: ConnectionPool,
        trainer: Trainer,
        vocab: Vocabulary,
        use_neural_bandit: bool = True,
        adaptive: bool = True,
        adaptive_threshold: float = 100.0,
        default_timeout_ms: int = 60_000,
    ) -> None:
        self.pool = pool
        self.trainer = trainer
        self.vocab = vocab
        self.adaptive = adaptive
        self.default_timeout_ms = default_timeout_ms
        self.interceptor = QueryInterceptor(pool)
        self.recompiler = AdaptiveRecompiler(pool, threshold=adaptive_threshold)

        model = trainer.model
        if use_neural_bandit:
            self.bandit = NeuralBandit(model, num_arms=len(BAO_HINT_SETS))
        else:
            self.bandit = ThompsonSamplingBandit(num_arms=len(BAO_HINT_SETS))

        self.results: list[BaoResult] = []

    def run_query(self, sql: str) -> BaoResult:
        # Get default plan for arm selection
        try:
            default_plan = self.interceptor.explain_dry(sql)
        except Exception as e:
            logger.error("explain_dry failed: %s", e)
            raise

        self.vocab.update_from_node(default_plan)
        encoded = encode_tree(default_plan, self.vocab, include_actuals=False)

        # Bandit selects hint set
        if isinstance(self.bandit, NeuralBandit):
            chosen_arm = self.bandit.select(encoded, device=self.trainer.device)
        else:
            chosen_arm = self.bandit.select()

        hint_set = BAO_HINT_SETS[chosen_arm]
        hints = hint_set_to_pg_hints(hint_set)

        logger.info("Bao selected arm=%d hints=%s", chosen_arm, hint_set)

        # Execute (with adaptive recompile if enabled)
        if self.adaptive:
            recompile_result = self.recompiler.run(
                sql, hint_id=chosen_arm, base_hints=hints or None,
                timeout_ms=self.default_timeout_ms,
            )
            record = recompile_result.recompiled_record or recompile_result.original_record
            latency_ms = record.latency_ms
            adaptive_speedup = recompile_result.speedup if recompile_result.triggered else 1.0
        else:
            try:
                plan, latency_ms = self.interceptor.explain_analyze(
                    f"/*+ {hints} */\n{sql}" if hints else sql,
                    self.default_timeout_ms,
                )
                record = ExecutionRecord(
                    sql=sql, hint_sql=None, plan_dry=default_plan,
                    plan_analyzed=plan, latency_ms=latency_ms, hint_id=chosen_arm,
                )
                recompile_result = None
                adaptive_speedup = 1.0
            except Exception as e:
                logger.error("Query execution failed: %s", e)
                raise

        # Feed latency back to bandit
        self.bandit.update(chosen_arm, latency_ms)

        # Compute speedup vs default (arm 0)
        speedup_vs_default = 1.0
        if chosen_arm != 0:
            try:
                _, default_latency = self.interceptor.explain_analyze(sql, self.default_timeout_ms)
                speedup_vs_default = default_latency / max(latency_ms, 0.001)
            except Exception:
                pass

        # Add training samples
        if record.plan_analyzed is not None:
            analyzed_tree = encode_tree(record.plan_analyzed, self.vocab, include_actuals=True)
            self.trainer.add_cardinality_sample(analyzed_tree, chosen_arm)
            self.trainer.add_cost_sample(analyzed_tree, chosen_arm, latency_ms)

        # Online training step
        metrics = self.trainer.train_step()
        if metrics:
            logger.debug("Training step: %s", metrics)

        result = BaoResult(
            sql=sql,
            chosen_arm=chosen_arm,
            hint_set=hint_set,
            record=record,
            recompile_result=recompile_result,
            latency_ms=latency_ms,
            speedup_vs_default=speedup_vs_default,
            adaptive_speedup=adaptive_speedup,
        )
        self.results.append(result)

        # Periodic checkpoint
        if len(self.results) % 50 == 0:
            self.trainer.save(f"step_{len(self.results)}")

        return result

    def run_workload(self, queries: list[str]) -> list[BaoResult]:
        results = []
        for i, sql in enumerate(queries):
            logger.info("Query %d/%d", i + 1, len(queries))
            try:
                r = self.run_query(sql)
                results.append(r)
                logger.info(
                    "  arm=%d latency=%.1fms speedup=%.2f× adaptive=%.2f×",
                    r.chosen_arm, r.latency_ms, r.speedup_vs_default, r.adaptive_speedup,
                )
            except Exception as e:
                logger.error("Query %d failed: %s", i + 1, e)
        return results
