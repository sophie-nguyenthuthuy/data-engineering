"""Belief propagation on a small factor graph.

Toy example: binary nodes, exchange messages until convergence. This is
NOT a serious BP implementation — it's here to exercise the runtime's
iterate scope on a different problem shape.
"""

from __future__ import annotations

from timely.graph.builder import GraphBuilder
from timely.graph.runtime import Runtime
from timely.timestamp.ts import Timestamp


def belief_propagation(
    n_nodes: int = 4, n_factors: int = 3, tol: float = 1e-3, max_iter: int = 30,
) -> tuple[list[float], int]:
    """Naive sum-product iteration until messages stop changing."""
    state = {
        "msgs": [0.5] * n_nodes,
        "iter": 0,
    }

    def loop_body(ts: Timestamp, _value: object, emit) -> None:
        prev = state["msgs"]
        # Simple averaging dynamic: each msg = average of neighbours' msgs
        new = [(prev[(i - 1) % n_nodes] + prev[(i + 1) % n_nodes]) / 2 for i in range(n_nodes)]
        # Damping to avoid oscillation
        new = [0.5 * prev[i] + 0.5 * new[i] for i in range(n_nodes)]
        state["msgs"] = new
        state["iter"] += 1
        diff = sum(abs(a - b) for a, b in zip(prev, new, strict=False))
        if diff < tol or state["iter"] >= max_iter:
            emit("converged", ts, tuple(new))
        else:
            emit("loop", ts, None)

    g = GraphBuilder()
    g.iterate("loop", loop_body, input="seed")
    g.source("seed", [(Timestamp(0, 0), None)], downstream="loop")
    g.sink("converged", input="loop")
    rt = Runtime(g)
    rt.run()
    final_ts, final_msgs = g.sinks["converged"][0]
    return list(final_msgs), final_ts.iteration


__all__ = ["belief_propagation"]
