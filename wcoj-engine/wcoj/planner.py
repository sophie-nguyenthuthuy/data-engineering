"""Query planner: cyclic join detection and algorithm selection.

The planner models a join query as a hypergraph where
  - nodes  = variables
  - hyperedges = relations (each edge = set of variables in that relation)

Acyclicity is tested via the GYO (Graham–Yu–Ozsoyoglu) reduction:
repeatedly remove "ear" hyperedges until none remain or the graph is empty.
A hypergraph is acyclic iff all edges can be removed.

Decision logic
--------------
  acyclic  → HashJoin     (near-optimal for acyclic queries)
  cyclic   → LFTJ         (worst-case optimal; 10-100× faster on graphs)

Public API
----------
is_acyclic(query)        → bool
explain(query)           → str   (human-readable explanation)
execute(query, force)    → np.ndarray
PlannerResult            (named tuple with metadata)
"""
from __future__ import annotations

import time
from typing import List, NamedTuple, Optional

import numpy as np

from .hash_join import hash_join
from .lftj import lftj
from .query import JoinQuery


# ------------------------------------------------------------------ #
#  Cyclic / acyclic detection via GYO reduction                       #
# ------------------------------------------------------------------ #

def is_acyclic(query: JoinQuery) -> bool:
    """Return True if the join hypergraph is acyclic (has a join tree).

    GYO reduction: an edge e is an "ear" if the set of variables it shares
    with all OTHER edges is a subset of some single other edge.  Remove ears
    iteratively; if all edges are removed the hypergraph is acyclic.
    """
    edges: List[set] = [set(r.variables) for r in query.relations]

    changed = True
    while changed and edges:
        changed = False
        for i in range(len(edges)):
            edge = edges[i]
            # Variables of this edge that appear in at least one other edge.
            shared: set = set()
            for j, other in enumerate(edges):
                if j != i:
                    shared |= edge & other

            # Ear condition: shared vars are a subset of some other single edge.
            if not shared:
                # Isolated edge — trivially an ear.
                edges.pop(i)
                changed = True
                break
            for j, other in enumerate(edges):
                if j != i and shared <= other:
                    edges.pop(i)
                    changed = True
                    break
            if changed:
                break

    return len(edges) == 0


def detect_patterns(query: JoinQuery) -> List[str]:
    """Identify known cyclic patterns in the query hypergraph."""
    patterns = []
    vars_per_rel = [set(r.variables) for r in query.relations]
    n = len(vars_per_rel)

    # Triangle: three binary relations forming a 3-cycle.
    for i in range(n):
        for j in range(i + 1, n):
            for k in range(j + 1, n):
                a, b, c = vars_per_rel[i], vars_per_rel[j], vars_per_rel[k]
                if (len(a) == 2 and len(b) == 2 and len(c) == 2
                        and len(a | b | c) == 3
                        and len(a & b) == 1 and len(b & c) == 1 and len(a & c) == 1):
                    patterns.append("triangle")

    # 4-clique
    if n >= 6:
        for i in range(n):
            if len(vars_per_rel[i]) == 2:
                for j in range(i + 1, n):
                    if len(vars_per_rel[j]) == 2:
                        shared = vars_per_rel[i] & vars_per_rel[j]
                        if not shared:
                            patterns.append("4-clique-candidate")
                            break

    return list(dict.fromkeys(patterns))  # deduplicate, preserve order


# ------------------------------------------------------------------ #
#  Planner output                                                     #
# ------------------------------------------------------------------ #

class PlannerResult(NamedTuple):
    algorithm: str              # "lftj" | "hash_join"
    acyclic: bool
    patterns: List[str]
    var_order: List[str]
    n_results: int
    elapsed_s: float
    data: np.ndarray


# ------------------------------------------------------------------ #
#  Public executor                                                    #
# ------------------------------------------------------------------ #

def execute(
    query: JoinQuery,
    force: Optional[str] = None,
) -> PlannerResult:
    """Plan and execute a join query.

    Args:
        query:  The join query to execute.
        force:  If "lftj" or "hash_join", override the planner's choice.

    Returns:
        PlannerResult with timing and result data.
    """
    acyclic = is_acyclic(query)
    patterns = detect_patterns(query)

    if force is not None:
        algorithm = force
    else:
        algorithm = "hash_join" if acyclic else "lftj"

    var_order = query.variable_order()

    t0 = time.perf_counter()
    if algorithm == "lftj":
        data = lftj(query, var_order)
    else:
        data = hash_join(query)
    elapsed = time.perf_counter() - t0

    return PlannerResult(
        algorithm=algorithm,
        acyclic=acyclic,
        patterns=patterns,
        var_order=var_order,
        n_results=len(data),
        elapsed_s=elapsed,
        data=data,
    )


def explain(query: JoinQuery) -> str:
    """Return a human-readable explanation of the planner's decision."""
    acyclic = is_acyclic(query)
    patterns = detect_patterns(query)
    algo = "HashJoin" if acyclic else "Leapfrog Triejoin (WCOJ)"
    reason = (
        "query hypergraph is acyclic — hash join is near-optimal"
        if acyclic
        else "cyclic query hypergraph detected — WCOJ avoids intermediate blowup"
    )
    lines = [
        f"Query: {len(query.relations)} relations, "
        f"{len(query.variables)} variables",
        f"Hypergraph: {'acyclic' if acyclic else 'CYCLIC'}",
        f"Detected patterns: {patterns if patterns else 'none'}",
        f"Selected algorithm: {algo}",
        f"Reason: {reason}",
        f"Variable order: {query.variable_order()}",
    ]
    return "\n".join(lines)
