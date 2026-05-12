"""Training loop for QueryOptimizer with experience-replay buffer.

Two training objectives:
  1. Cardinality loss — MSE on log-cardinality per node (q-error minimization)
  2. Cost loss       — MSE on log query latency (Bao bandit regression)
"""
from __future__ import annotations
import logging
import math
import random
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import torch
import torch.nn as nn
from torch import Tensor
from torch.optim import AdamW
from torch.optim.lr_scheduler import CosineAnnealingLR

from .gnn import QueryOptimizer
from ..plan.encoder import EncodedTree

logger = logging.getLogger(__name__)


@dataclass
class CardinalityExample:
    tree: EncodedTree       # has log_cardinalities as target
    hint_id: int = 0


@dataclass
class CostExample:
    tree: EncodedTree
    hint_id: int
    log_latency: float      # log(actual_ms)


@dataclass
class TrainConfig:
    hidden_size: int = 128
    num_hints: int = 15
    lr: float = 3e-4
    weight_decay: float = 1e-4
    batch_size: int = 32
    cardinality_buffer_size: int = 10_000
    cost_buffer_size: int = 5_000
    cardinality_weight: float = 1.0
    cost_weight: float = 0.5
    grad_clip: float = 1.0
    checkpoint_dir: str = "checkpoints"


class Trainer:
    def __init__(self, config: TrainConfig, device: torch.device | None = None) -> None:
        self.config = config
        self.device = device or torch.device("cpu")
        self.model = QueryOptimizer(
            hidden_size=config.hidden_size,
            num_hints=config.num_hints,
        ).to(self.device)
        self.optimizer = AdamW(
            self.model.parameters(),
            lr=config.lr,
            weight_decay=config.weight_decay,
        )
        self.card_buf: deque[CardinalityExample] = deque(maxlen=config.cardinality_buffer_size)
        self.cost_buf: deque[CostExample] = deque(maxlen=config.cost_buffer_size)
        self.steps = 0
        self.ckpt_dir = Path(config.checkpoint_dir)
        self.ckpt_dir.mkdir(parents=True, exist_ok=True)

    def add_cardinality_sample(self, tree: EncodedTree, hint_id: int = 0) -> None:
        """Add an (analyzed plan tree) with actual cardinalities as training signal."""
        if tree.log_cardinalities is None:
            raise ValueError("Tree must have log_cardinalities set (include_actuals=True)")
        self.card_buf.append(CardinalityExample(tree=tree, hint_id=hint_id))

    def add_cost_sample(self, tree: EncodedTree, hint_id: int, latency_ms: float) -> None:
        self.cost_buf.append(CostExample(
            tree=tree,
            hint_id=hint_id,
            log_latency=math.log(max(latency_ms, 0.001)),
        ))

    def train_step(self) -> dict[str, float]:
        if len(self.card_buf) < max(4, self.config.batch_size // 4):
            return {}

        self.model.train()
        self.optimizer.zero_grad()
        total_loss = torch.tensor(0.0, device=self.device)
        metrics: dict[str, float] = {}

        # ── Cardinality loss ──────────────────────────────────────────────────
        card_batch = random.sample(
            list(self.card_buf),
            min(self.config.batch_size, len(self.card_buf)),
        )
        card_losses = []
        for ex in card_batch:
            t = ex.tree.to(self.device)
            out = self.model(t, ex.hint_id)
            loss = nn.functional.mse_loss(
                out["log_cardinalities"], t.log_cardinalities
            )
            card_losses.append(loss)
        if card_losses:
            card_loss = torch.stack(card_losses).mean()
            total_loss = total_loss + self.config.cardinality_weight * card_loss
            metrics["card_loss"] = float(card_loss.detach())

        # ── Cost loss ─────────────────────────────────────────────────────────
        if len(self.cost_buf) >= 4:
            cost_batch = random.sample(
                list(self.cost_buf),
                min(self.config.batch_size // 2, len(self.cost_buf)),
            )
            cost_losses = []
            for ex in cost_batch:
                t = ex.tree.to(self.device)
                out = self.model(t, ex.hint_id)
                target = torch.tensor(ex.log_latency, device=self.device)
                cost_losses.append(nn.functional.mse_loss(out["log_cost"], target))
            if cost_losses:
                cost_loss = torch.stack(cost_losses).mean()
                total_loss = total_loss + self.config.cost_weight * cost_loss
                metrics["cost_loss"] = float(cost_loss.detach())

        total_loss.backward()
        nn.utils.clip_grad_norm_(self.model.parameters(), self.config.grad_clip)
        self.optimizer.step()
        self.steps += 1
        metrics["total_loss"] = float(total_loss.detach())
        metrics["step"] = self.steps
        return metrics

    def train_epochs(self, n_steps: int = 500) -> list[dict[str, float]]:
        history = []
        for _ in range(n_steps):
            m = self.train_step()
            if m:
                history.append(m)
                if self.steps % 100 == 0:
                    logger.info(
                        "step=%d card_loss=%.4f cost_loss=%.4f",
                        self.steps,
                        m.get("card_loss", 0),
                        m.get("cost_loss", 0),
                    )
        return history

    def save(self, tag: str = "latest") -> Path:
        path = self.ckpt_dir / f"model_{tag}.pt"
        torch.save({
            "model_state": self.model.state_dict(),
            "optimizer_state": self.optimizer.state_dict(),
            "steps": self.steps,
            "config": self.config,
        }, path)
        logger.info("Saved checkpoint → %s", path)
        return path

    def load(self, path: str | Path) -> None:
        ckpt = torch.load(path, map_location=self.device)
        self.model.load_state_dict(ckpt["model_state"])
        self.optimizer.load_state_dict(ckpt["optimizer_state"])
        self.steps = ckpt["steps"]
        logger.info("Loaded checkpoint from %s (step %d)", path, self.steps)

    def q_error_stats(self, trees: list[EncodedTree]) -> dict[str, float]:
        """Compute q-error statistics over a list of trees with actuals."""
        self.model.eval()
        q_errors = []
        with torch.no_grad():
            for tree in trees:
                if tree.log_cardinalities is None:
                    continue
                t = tree.to(self.device)
                out = self.model(t)
                pred = out["log_cardinalities"]
                target = t.log_cardinalities
                # q-error = exp(|log(pred/actual)|)
                q_err = torch.exp(torch.abs(pred - target))
                q_errors.extend(q_err.cpu().tolist())
        if not q_errors:
            return {}
        q_errors.sort()
        n = len(q_errors)
        return {
            "q_error_mean": sum(q_errors) / n,
            "q_error_median": q_errors[n // 2],
            "q_error_90th": q_errors[int(0.9 * n)],
            "q_error_95th": q_errors[int(0.95 * n)],
            "q_error_max": q_errors[-1],
        }
