"""End-to-end convergence tests for a 50-worker cluster."""
import pytest
from src.cluster import Cluster


@pytest.fixture
def processed_cluster():
    c = Cluster(n_workers=50, seed=99)
    c.generate_and_process(total_rows=10_000, null_rate=0.05)
    return c


def test_workers_diverge_before_merge(processed_cluster):
    """Each worker only sees its own partition — counts differ."""
    counts = [w.metrics.null_count.value() for w in processed_cluster.workers]
    assert max(counts) > min(counts), "Workers should diverge before merge"


def test_full_merge_converges(processed_cluster):
    processed_cluster.merge_full()
    assert processed_cluster.is_converged()


def test_gossip_merge_converges(processed_cluster):
    # log(50)/log(3) ≈ 3.6 → 6 rounds is more than enough
    for _ in range(6):
        processed_cluster.merge_gossip(fanout=3)
    assert processed_cluster.is_converged()


def test_ring_merge_eventually_converges(processed_cluster):
    # Ring needs O(n) rounds to propagate across all nodes
    for _ in range(50):
        processed_cluster.merge_ring()
    assert processed_cluster.is_converged()


def test_merge_is_monotone(processed_cluster):
    """No worker ever loses counts during merge."""
    before = [w.metrics.null_count.value() for w in processed_cluster.workers]
    processed_cluster.merge_full()
    after = [w.metrics.null_count.value() for w in processed_cluster.workers]
    assert all(a >= b for a, b in zip(after, before))


def test_global_total_matches_row_count(processed_cluster):
    processed_cluster.merge_full()
    s = processed_cluster.global_summary()
    assert s["total_observed"] == 10_000


def test_null_rate_plausible(processed_cluster):
    processed_cluster.merge_full()
    s = processed_cluster.global_summary()
    # Generated with null_rate=0.05; allow ±2%
    assert 0.03 <= s["null_rate"] <= 0.07


def test_convergence_variance_zero_after_full_merge(processed_cluster):
    processed_cluster.merge_full()
    v = processed_cluster.convergence_variance()
    assert all(spread == 0 for spread in v.values())


def test_pncounter_convergence(processed_cluster):
    processed_cluster.merge_full()
    anomaly_counts = [w.metrics.anomaly_count.value() for w in processed_cluster.workers]
    assert len(set(anomaly_counts)) == 1  # all identical


def test_or_set_convergence(processed_cluster):
    processed_cluster.merge_full()
    sets = [w.metrics.anomaly_types.elements() for w in processed_cluster.workers]
    assert all(s == sets[0] for s in sets[1:])


def test_hyperloglog_convergence(processed_cluster):
    processed_cluster.merge_full()
    counts = [w.metrics.distinct_values.count() for w in processed_cluster.workers]
    assert len(set(counts)) == 1
