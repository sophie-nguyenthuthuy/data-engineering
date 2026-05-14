"""Workload + Query catalog tests."""

from __future__ import annotations

import pytest

from pvc.workloads.base import Query, Workload
from pvc.workloads.nytaxi import NY_TAXI_QUERIES
from pvc.workloads.tpch import TPCH_QUERIES


def test_query_validates_fields():
    with pytest.raises(ValueError):
        Query(id="", description="x", sql="SELECT 1")
    with pytest.raises(ValueError):
        Query(id="q", description="x", sql="")
    with pytest.raises(ValueError):
        Query(id="q", description="", sql="SELECT 1")


def test_workload_rejects_empty_queries():
    with pytest.raises(ValueError):
        Workload(name="w", queries=())


def test_workload_rejects_empty_name():
    q = Query(id="q", description="x", sql="SELECT 1")
    with pytest.raises(ValueError):
        Workload(name="", queries=(q,))


def test_workload_rejects_duplicate_query_id():
    q = Query(id="q", description="x", sql="SELECT 1")
    with pytest.raises(ValueError):
        Workload(name="w", queries=(q, q))


def test_workload_by_id_returns_query():
    q1 = Query(id="a", description="a", sql="SELECT 1")
    q2 = Query(id="b", description="b", sql="SELECT 2")
    w = Workload(name="w", queries=(q1, q2))
    assert w.by_id("a") == q1
    with pytest.raises(KeyError):
        w.by_id("nope")


def test_tpch_catalog_has_ten_queries():
    assert len(TPCH_QUERIES) == 10
    ids = {q.id for q in TPCH_QUERIES.queries}
    assert ids == {"Q1", "Q3", "Q4", "Q5", "Q6", "Q10", "Q11", "Q12", "Q14", "Q19"}


def test_ny_taxi_catalog_has_five_queries():
    assert len(NY_TAXI_QUERIES) == 5
    ids = {q.id for q in NY_TAXI_QUERIES.queries}
    assert "NYT-1" in ids


def test_tpch_queries_have_non_empty_sql():
    for q in TPCH_QUERIES.queries:
        assert q.sql.strip()
