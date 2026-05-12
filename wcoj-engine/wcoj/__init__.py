"""WCOJ Engine — Worst-Case Optimal Join algorithms.

Quick start
-----------
>>> from wcoj import Relation, JoinQuery, execute
>>> import numpy as np
>>>
>>> # Triangle query on a small graph: R(x,y), S(y,z), T(x,z)
>>> edges = np.array([[0,1],[0,2],[1,2],[1,3],[2,3]], dtype=np.int64)
>>> R = Relation("R", ["x","y"], edges)
>>> S = Relation("S", ["y","z"], edges)
>>> T = Relation("T", ["x","z"], edges)
>>> q = JoinQuery([R, S, T])
>>> result = execute(q)
>>> print(result.algorithm, result.n_results)
lftj 2
"""

from .generic_join import generic_join
from .hash_join import hash_join
from .lftj import lftj
from .planner import PlannerResult, execute, explain, is_acyclic
from .query import JoinQuery, Relation
from .trie import TrieIterator

__all__ = [
    "Relation",
    "JoinQuery",
    "TrieIterator",
    "lftj",
    "generic_join",
    "hash_join",
    "is_acyclic",
    "explain",
    "execute",
    "PlannerResult",
]
