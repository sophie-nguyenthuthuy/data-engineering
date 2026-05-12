"""LAG and LEAD."""

from __future__ import annotations

import pytest


def test_lag_basic(lag_lead):
    for t, v in [(1.0, "a"), (2.0, "b"), (3.0, "c")]:
        lag_lead.insert("p", t, v)
    assert lag_lead.lag("p", 2.0) == "a"     # LAG(1)
    assert lag_lead.lag("p", 3.0, 2) == "a"  # LAG(2)


def test_lag_at_start_returns_none(lag_lead):
    lag_lead.insert("p", 1.0, "a")
    lag_lead.insert("p", 2.0, "b")
    assert lag_lead.lag("p", 1.0) is None    # no row before


def test_lead_basic(lag_lead):
    for t, v in [(1.0, "a"), (2.0, "b"), (3.0, "c")]:
        lag_lead.insert("p", t, v)
    assert lag_lead.lead("p", 1.0) == "b"
    assert lag_lead.lead("p", 1.0, 2) == "c"


def test_lead_at_end_returns_none(lag_lead):
    for t, v in [(1.0, "a"), (2.0, "b")]:
        lag_lead.insert("p", t, v)
    assert lag_lead.lead("p", 2.0) is None


def test_k_must_be_positive(lag_lead):
    with pytest.raises(ValueError):
        lag_lead.lag("p", 1.0, k=0)
    with pytest.raises(ValueError):
        lag_lead.lead("p", 1.0, k=0)


def test_partitions_isolated(lag_lead):
    lag_lead.insert("p1", 1.0, "x")
    lag_lead.insert("p1", 2.0, "y")
    lag_lead.insert("p2", 1.0, "a")
    assert lag_lead.lag("p1", 2.0) == "x"
    assert lag_lead.lag("p2", 1.0) is None
