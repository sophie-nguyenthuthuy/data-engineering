"""Shared fixtures."""

from __future__ import annotations

import pytest

from beps.tree.tree import BEpsilonTree


@pytest.fixture
def tree() -> BEpsilonTree:
    return BEpsilonTree(node_size=16, epsilon=0.5)


@pytest.fixture
def small_tree() -> BEpsilonTree:
    return BEpsilonTree(node_size=4, epsilon=0.5)
