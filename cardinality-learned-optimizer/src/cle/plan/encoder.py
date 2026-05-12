"""Encode a PlanNode tree into tensors consumable by the TreeLSTM/GNN."""
from __future__ import annotations
import math
from typing import Optional
import torch
from torch import Tensor
from .node import PlanNode, OPERATOR_TYPES


# ── vocabulary for relation names and column tokens ──────────────────────────
class Vocabulary:
    def __init__(self) -> None:
        self._token2id: dict[str, int] = {"<unk>": 0, "<pad>": 1}
        self._next = 2

    def add(self, token: str) -> int:
        if token not in self._token2id:
            self._token2id[token] = self._next
            self._next += 1
        return self._token2id[token]

    def get(self, token: str) -> int:
        return self._token2id.get(token, 0)

    def __len__(self) -> int:
        return self._next

    def update_from_node(self, node: PlanNode) -> None:
        for n in node.all_nodes():
            if n.relation_name:
                self.add(n.relation_name)
            if n.alias:
                self.add(n.alias)


# ── per-node feature vector ───────────────────────────────────────────────────
# Layout (fixed-width):
#   [0:24]    operator one-hot        (24 dims)
#   [24]      log(estimated_rows)     (1 dim)
#   [25]      log(estimated_cost)     (1 dim)
#   [26]      estimated_width / 100   (1 dim, normalized)
#   [27]      has_filter              (1 dim)
#   [28]      has_join_cond           (1 dim)
#   [29]      depth / 10              (1 dim)
#   [30]      relation_id / vocab_sz  (1 dim)
# Total: 31 dims

FEATURE_DIM = 31


def node_to_feature(node: PlanNode, vocab: Vocabulary) -> list[float]:
    feats: list[float] = []

    # operator one-hot
    feats.extend([float(x) for x in node.operator_one_hot()])

    # log-scaled cardinality / cost
    feats.append(math.log(max(node.estimated_rows, 1.0)) / 20.0)   # rough normalise
    feats.append(math.log(max(node.estimated_cost_total, 1.0)) / 20.0)
    feats.append(node.estimated_width / 100.0)

    # boolean flags
    feats.append(1.0 if (node.filter or node.join_filter) else 0.0)
    feats.append(1.0 if (node.hash_cond or node.merge_cond) else 0.0)

    # structural
    feats.append(node.depth / 10.0)

    # relation embedding (single lookup; 0 = unknown)
    rel = node.relation_name or node.alias or ""
    feats.append(vocab.get(rel) / max(len(vocab), 1))

    assert len(feats) == FEATURE_DIM, f"Expected {FEATURE_DIM} features, got {len(feats)}"
    return feats


# ── tree topology helpers ─────────────────────────────────────────────────────

def build_index_map(root: PlanNode) -> dict[int, int]:
    """Map node_id → position in BFS order (used to build adjacency for GNN)."""
    idx: dict[int, int] = {}
    queue = [root]
    pos = 0
    while queue:
        node = queue.pop(0)
        idx[node.node_id] = pos
        pos += 1
        queue.extend(node.children)
    return idx


class EncodedTree:
    """All tensors needed by the TreeLSTM for one plan tree."""

    def __init__(
        self,
        node_features: Tensor,      # [N, FEATURE_DIM]
        parent_ids: list[int],      # length N; -1 for root
        children_ids: list[list[int]],  # length N; list of child positions
        log_cardinalities: Optional[Tensor] = None,  # [N] training target
    ) -> None:
        self.node_features = node_features
        self.parent_ids = parent_ids
        self.children_ids = children_ids
        self.log_cardinalities = log_cardinalities
        self.n = node_features.size(0)

    def to(self, device: torch.device) -> "EncodedTree":
        self.node_features = self.node_features.to(device)
        if self.log_cardinalities is not None:
            self.log_cardinalities = self.log_cardinalities.to(device)
        return self


def encode_tree(
    root: PlanNode,
    vocab: Vocabulary,
    include_actuals: bool = False,
) -> EncodedTree:
    nodes = root.all_nodes()
    idx_map = build_index_map(root)

    features = []
    parent_ids = []
    children_ids = []
    log_cards: list[float] = []

    def _walk(node: PlanNode, parent_pos: int) -> None:
        pos = idx_map[node.node_id]
        features.append(node_to_feature(node, vocab))
        parent_ids.append(parent_pos)
        children_ids.append([idx_map[c.node_id] for c in node.children])
        if include_actuals and node.actual_rows_total is not None:
            log_cards.append(math.log(max(node.actual_rows_total, 1.0)))
        else:
            log_cards.append(math.log(max(node.estimated_rows, 1.0)))
        for child in node.children:
            _walk(child, pos)

    _walk(root, -1)

    feat_tensor = torch.tensor(features, dtype=torch.float32)
    target = torch.tensor(log_cards, dtype=torch.float32) if log_cards else None

    return EncodedTree(feat_tensor, parent_ids, children_ids, target)
