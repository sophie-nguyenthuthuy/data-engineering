"""Shared fixtures."""

from __future__ import annotations

import pytest

from ivm.correlated.subquery import CorrelatedSubqueryIVM
from ivm.nested.max_of_sum import MaxOfSum
from ivm.nested.sum_of_max import SumOfMax
from ivm.window.lag_lead import LagLeadIVM
from ivm.window.rank import DenseRankIVM, RankIVM
from ivm.window.row_number import RowNumberIVM
from ivm.window.sliding_sum import SlidingSumIVM


@pytest.fixture
def row_number() -> RowNumberIVM:
    return RowNumberIVM()


@pytest.fixture
def rank_ivm() -> RankIVM:
    return RankIVM()


@pytest.fixture
def dense_rank() -> DenseRankIVM:
    return DenseRankIVM()


@pytest.fixture
def lag_lead() -> LagLeadIVM:
    return LagLeadIVM()


@pytest.fixture
def sliding() -> SlidingSumIVM:
    return SlidingSumIVM(window_size=5)


@pytest.fixture
def correlated() -> CorrelatedSubqueryIVM:
    return CorrelatedSubqueryIVM()


@pytest.fixture
def max_of_sum() -> MaxOfSum:
    return MaxOfSum()


@pytest.fixture
def sum_of_max() -> SumOfMax:
    return SumOfMax()
