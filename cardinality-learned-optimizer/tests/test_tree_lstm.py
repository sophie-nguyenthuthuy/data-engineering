"""Tests for TreeLSTM and GNN model."""
import json
import math
from pathlib import Path
import pytest
import torch

from cle.plan.parser import parse_explain_json
from cle.plan.encoder import Vocabulary, encode_tree, FEATURE_DIM
from cle.model.tree_lstm import ChildSumTreeLSTM, PlanTreeEncoder
from cle.model.gnn import QueryOptimizer, CardinalityHead, CostHead

FIXTURE = Path(__file__).parent / "fixtures" / "sample_plan.json"


@pytest.fixture
def encoded_tree():
    data = json.loads(FIXTURE.read_text())
    root = parse_explain_json(data)
    vocab = Vocabulary()
    vocab.update_from_node(root)
    return encode_tree(root, vocab, include_actuals=True)


def test_feature_dim(encoded_tree):
    assert encoded_tree.node_features.shape[1] == FEATURE_DIM


def test_tree_shape(encoded_tree):
    assert encoded_tree.n == 4
    assert len(encoded_tree.parent_ids) == 4
    assert len(encoded_tree.children_ids) == 4


def test_log_cardinalities_set(encoded_tree):
    assert encoded_tree.log_cardinalities is not None
    assert encoded_tree.log_cardinalities.shape == (4,)
    # log of actual rows should be positive (rows > 1)
    assert (encoded_tree.log_cardinalities > 0).all()


def test_tree_lstm_forward(encoded_tree):
    model = ChildSumTreeLSTM(FEATURE_DIM, hidden_size=32)
    H = model(encoded_tree)
    assert H.shape == (4, 32)
    assert not torch.isnan(H).any()


def test_plan_encoder_forward(encoded_tree):
    encoder = PlanTreeEncoder(hidden_size=64)
    node_embs, root_emb = encoder(encoded_tree)
    assert node_embs.shape == (4, 64)
    assert root_emb.shape == (64,)


def test_query_optimizer_forward(encoded_tree):
    model = QueryOptimizer(hidden_size=64, num_hints=15)
    out = model(encoded_tree, hint_id=0)
    assert "log_cardinalities" in out
    assert "log_cost" in out
    assert out["log_cardinalities"].shape == (4,)
    assert out["log_cost"].shape == ()


def test_predict_cardinalities(encoded_tree):
    model = QueryOptimizer(hidden_size=32)
    cards = model.predict_cardinalities(encoded_tree)
    assert cards.shape == (4,)
    # predicted cardinalities should be positive
    assert (cards > 0).all()


def test_cost_head_different_hints(encoded_tree):
    model = QueryOptimizer(hidden_size=32, num_hints=15)
    costs = [model.predict_cost(encoded_tree, i) for i in range(15)]
    # Different hints should generally produce different cost estimates
    # (at least some variation)
    assert len(set(round(c, 2) for c in costs)) > 1


def test_gradient_flow(encoded_tree):
    model = QueryOptimizer(hidden_size=32)
    out = model(encoded_tree, 0)
    loss = out["log_cardinalities"].mean() + out["log_cost"]
    loss.backward()
    # All parameters should have gradients
    for name, param in model.named_parameters():
        assert param.grad is not None, f"No gradient for {name}"


def test_model_trains(encoded_tree):
    from cle.model.trainer import Trainer, TrainConfig
    cfg = TrainConfig(hidden_size=32, batch_size=4)
    trainer = Trainer(cfg)
    # Add several copies of the same sample
    for _ in range(20):
        trainer.add_cardinality_sample(encoded_tree)
        trainer.add_cost_sample(encoded_tree, 0, 150.0)
    metrics = trainer.train_step()
    assert "total_loss" in metrics
    assert math.isfinite(metrics["total_loss"])
