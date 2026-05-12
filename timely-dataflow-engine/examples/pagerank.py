"""Iterative PageRank using Timely Dataflow.

We compute PR on a tiny graph until convergence. Each iteration is a new
iteration timestamp; convergence terminates the loop.
"""
from __future__ import annotations

from src import Graph, Timestamp


def main():
    # Tiny directed graph:
    #   0 -> 1, 0 -> 2
    #   1 -> 2
    #   2 -> 0
    edges = {0: [1, 2], 1: [2], 2: [0]}
    n = 3
    damping = 0.85
    tol = 1e-4

    # State: current rank vector per iteration (closed over by the operator)
    state = {"ranks": [1.0 / n] * n, "prev": [0.0] * n}

    g = Graph()
    g.add_sink("converged")

    def step(ts, _value, emit):
        prev = state["ranks"]
        new = [(1 - damping) / n] * n
        for u, neigh in edges.items():
            if not neigh:
                continue
            share = damping * prev[u] / len(neigh)
            for v in neigh:
                new[v] += share
        state["prev"] = prev
        state["ranks"] = new
        diff = sum(abs(a - b) for a, b in zip(prev, new))
        if diff < tol:
            emit("converged", ts, tuple(new))
        else:
            emit("loop", ts, None)

    g.add("loop", step, feedback=True)
    g.send("loop", Timestamp(0, 0), None)
    g.run()
    final_ts, final_ranks = g.sinks["converged"][0]
    print(f"Converged at {final_ts} after {final_ts.iteration} iterations")
    for i, r in enumerate(final_ranks):
        print(f"  PR[{i}] = {r:.6f}")
    print(f"  Sum    = {sum(final_ranks):.6f}")


if __name__ == "__main__":
    main()
