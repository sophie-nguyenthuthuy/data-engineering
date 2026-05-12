"""N-ary Tree-LSTM for query plan trees.

Bottom-up message passing: each node aggregates its children's hidden states
before computing its own, producing a single hidden vector per node that
captures the semantics of the entire subtree rooted there.

Reference:
  Tai et al., "Improved Semantic Representations From Tree-Structured Long
  Short-Term Memory Networks," ACL 2015.

  Marcus et al., "Neo: A Learned Query Optimizer," VLDB 2019 — same topology
  applied to query plan trees.
"""
from __future__ import annotations
import torch
import torch.nn as nn
from torch import Tensor
from ..plan.encoder import EncodedTree, FEATURE_DIM


class ChildSumTreeLSTM(nn.Module):
    """Child-sum variant: sums child hidden states before gating.

    Works for arbitrary-arity trees (no fixed fan-out needed).
    Hidden size h; input size is FEATURE_DIM.
    """

    def __init__(self, input_size: int, hidden_size: int) -> None:
        super().__init__()
        self.hidden_size = hidden_size

        # iou: input, output, update gates (computed from node feature + sum-of-children)
        self.W_iou = nn.Linear(input_size, 3 * hidden_size, bias=False)
        self.U_iou = nn.Linear(hidden_size, 3 * hidden_size)

        # forget gate per child (function of child hidden state)
        self.W_f = nn.Linear(input_size, hidden_size, bias=False)
        self.U_f = nn.Linear(hidden_size, hidden_size)

    def _node_forward(
        self,
        x: Tensor,                   # [hidden] node feature
        child_h: Tensor,             # [k, hidden] children hidden states
        child_c: Tensor,             # [k, hidden] children cell states
    ) -> tuple[Tensor, Tensor]:
        """Compute h, c for a single node given its feature and children states."""
        h_sum = child_h.sum(dim=0)   # [hidden]

        iou = self.W_iou(x) + self.U_iou(h_sum)   # [3*hidden]
        i, o, u = iou.chunk(3, dim=-1)
        i, o, u = torch.sigmoid(i), torch.sigmoid(o), torch.tanh(u)

        # Per-child forget gates
        if child_h.size(0) > 0:
            f = torch.sigmoid(
                self.W_f(x).unsqueeze(0) + self.U_f(child_h)
            )           # [k, hidden]
            fc = (f * child_c).sum(dim=0)   # [hidden]
        else:
            fc = torch.zeros(self.hidden_size, device=x.device)

        c = i * u + fc
        h = o * torch.tanh(c)
        return h, c

    def forward(self, tree: EncodedTree) -> Tensor:
        """Return hidden states for all nodes, shape [N, hidden]."""
        device = tree.node_features.device
        n = tree.n
        H = torch.zeros(n, self.hidden_size, device=device)
        C = torch.zeros(n, self.hidden_size, device=device)

        # Process nodes in reverse BFS order (leaves first → root last).
        # EncodedTree stores nodes in BFS order, so we process n-1 → 0.
        for pos in range(n - 1, -1, -1):
            x = tree.node_features[pos]       # [FEATURE_DIM]
            cids = tree.children_ids[pos]
            if cids:
                child_h = H[cids]             # [k, hidden]
                child_c = C[cids]
            else:
                child_h = torch.zeros(0, self.hidden_size, device=device)
                child_c = torch.zeros(0, self.hidden_size, device=device)
            H[pos], C[pos] = self._node_forward(x, child_h, child_c)

        return H   # [N, hidden]


class PlanTreeEncoder(nn.Module):
    """Encode a full plan tree to per-node hidden states + a global embedding."""

    def __init__(self, hidden_size: int = 128, input_size: int = FEATURE_DIM) -> None:
        super().__init__()
        self.input_proj = nn.Sequential(
            nn.Linear(input_size, hidden_size),
            nn.LayerNorm(hidden_size),
            nn.GELU(),
        )
        self.tree_lstm = ChildSumTreeLSTM(hidden_size, hidden_size)
        self.hidden_size = hidden_size

    def forward(self, tree: EncodedTree) -> tuple[Tensor, Tensor]:
        """Returns (node_embeddings [N, H], root_embedding [H])."""
        # Project raw features
        proj = self.input_proj(tree.node_features)   # [N, H]
        # Swap projected features in temporarily for tree_lstm
        orig = tree.node_features
        tree.node_features = proj
        node_embs = self.tree_lstm(tree)             # [N, H]
        tree.node_features = orig
        root_emb = node_embs[0]                      # root is always position 0
        return node_embs, root_emb
