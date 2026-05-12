"""Full GNN model: cardinality estimator + plan cost predictor.

Two heads share the PlanTreeEncoder backbone:
  1. CardinalityHead — per-node log-cardinality prediction (used for adaptive re-planning)
  2. CostHead       — scalar query latency prediction (used by the Bao bandit)
"""
from __future__ import annotations
import torch
import torch.nn as nn
from torch import Tensor
from .tree_lstm import PlanTreeEncoder
from ..plan.encoder import EncodedTree


class CardinalityHead(nn.Module):
    def __init__(self, hidden_size: int) -> None:
        super().__init__()
        self.mlp = nn.Sequential(
            nn.Linear(hidden_size, hidden_size // 2),
            nn.GELU(),
            nn.Linear(hidden_size // 2, 1),
        )

    def forward(self, node_embs: Tensor) -> Tensor:
        """Returns [N] log-cardinality predictions."""
        return self.mlp(node_embs).squeeze(-1)


class CostHead(nn.Module):
    """Predict scalar query cost from root embedding + optional hint vector."""

    def __init__(self, hidden_size: int, num_hints: int = 15) -> None:
        super().__init__()
        self.hint_proj = nn.Embedding(num_hints, hidden_size // 4)
        in_dim = hidden_size + hidden_size // 4
        self.mlp = nn.Sequential(
            nn.Linear(in_dim, hidden_size),
            nn.GELU(),
            nn.Dropout(0.1),
            nn.Linear(hidden_size, hidden_size // 2),
            nn.GELU(),
            nn.Linear(hidden_size // 2, 1),
        )

    def forward(self, root_emb: Tensor, hint_id: int | Tensor) -> Tensor:
        """Returns scalar predicted log-latency."""
        if not isinstance(hint_id, Tensor):
            hint_id = torch.tensor(hint_id, device=root_emb.device)
        hint_id = hint_id.long().to(root_emb.device)
        hint_emb = self.hint_proj(hint_id)              # [H//4]
        combined = torch.cat([root_emb, hint_emb], dim=-1)
        return self.mlp(combined).squeeze(-1)


class QueryOptimizer(nn.Module):
    """End-to-end model combining tree encoding + both prediction heads."""

    def __init__(
        self,
        hidden_size: int = 128,
        num_hints: int = 15,
    ) -> None:
        super().__init__()
        self.encoder = PlanTreeEncoder(hidden_size=hidden_size)
        self.cardinality_head = CardinalityHead(hidden_size)
        self.cost_head = CostHead(hidden_size, num_hints)

    def forward(
        self,
        tree: EncodedTree,
        hint_id: int | Tensor = 0,
    ) -> dict[str, Tensor]:
        node_embs, root_emb = self.encoder(tree)
        log_cards = self.cardinality_head(node_embs)
        log_cost = self.cost_head(root_emb, hint_id)
        return {
            "log_cardinalities": log_cards,   # [N]
            "log_cost": log_cost,             # scalar
            "node_embeddings": node_embs,     # [N, H]
            "root_embedding": root_emb,       # [H]
        }

    @torch.no_grad()
    def predict_cardinalities(
        self, tree: EncodedTree, device: torch.device | None = None
    ) -> Tensor:
        """Return predicted per-node cardinalities (not log-scale) as [N]."""
        if device:
            tree = tree.to(device)
            self.to(device)
        self.eval()
        out = self(tree)
        return torch.exp(out["log_cardinalities"])

    @torch.no_grad()
    def predict_cost(
        self,
        tree: EncodedTree,
        hint_id: int,
        device: torch.device | None = None,
    ) -> float:
        if device:
            tree = tree.to(device)
            self.to(device)
        self.eval()
        out = self(tree, hint_id)
        return float(torch.exp(out["log_cost"]))
