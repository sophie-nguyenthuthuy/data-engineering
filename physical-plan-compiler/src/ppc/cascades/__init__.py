"""Cascades-style cost-based query optimizer.

The optimizer searches the space of physical plans equivalent to a given
logical plan and returns the minimum-cost one.

Architecture:
    Memo               — DAG of (logical-equivalent) Groups
    Group              — set of equivalent expressions; tracks `best_expr`
    GroupExpression    — one physical or logical realisation
    Rule               — transformation: matches a pattern, produces alternatives
    Optimizer          — top-down memoized search with dominance pruning
"""

from __future__ import annotations

from ppc.cascades.memo import Group, GroupExpression, Memo
from ppc.cascades.optimizer import Optimizer
from ppc.cascades.properties import PhysicalProperties

__all__ = ["Group", "GroupExpression", "Memo", "Optimizer", "PhysicalProperties"]
