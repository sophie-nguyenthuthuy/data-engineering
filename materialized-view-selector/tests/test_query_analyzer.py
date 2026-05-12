"""Tests for query analyser."""

import pytest
from datetime import datetime, timezone

from mv_selector.models import QueryRecord, Warehouse
from mv_selector.query_analyzer import QueryAnalyzer, fingerprint, normalise_sql


def _q(sql: str, cost: float = 1.0) -> QueryRecord:
    return QueryRecord(
        sql=sql,
        warehouse=Warehouse.BIGQUERY,
        executed_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        duration_ms=1000,
        bytes_processed=1_000_000_000,
        cost_usd=cost,
    )


REPEATED_SQL = "SELECT user_id, SUM(revenue) FROM orders GROUP BY user_id"
WORKLOAD = [_q(REPEATED_SQL, cost=2.0) for _ in range(5)] + [
    _q("SELECT * FROM users WHERE id = 1"),
    _q("SELECT * FROM products LIMIT 100"),
]


class TestFingerprint:
    def test_same_query_same_fp(self):
        assert fingerprint("SELECT 1") == fingerprint("SELECT 1")

    def test_different_literals_same_fp(self):
        assert fingerprint("SELECT * FROM t WHERE id = 1") == fingerprint(
            "SELECT * FROM t WHERE id = 99"
        )

    def test_different_query_different_fp(self):
        assert fingerprint("SELECT a FROM t") != fingerprint("SELECT b FROM t")


class TestNormalise:
    def test_strips_literals(self):
        n = normalise_sql("SELECT * FROM t WHERE id = 42 AND name = 'foo'")
        assert "42" not in n
        assert "foo" not in n
        assert "?" in n

    def test_uppercases(self):
        n = normalise_sql("select * from foo")
        assert n == n.upper()


class TestQueryAnalyzer:
    def test_repeated_query_becomes_candidate(self):
        analyzer = QueryAnalyzer(min_query_frequency=3, min_cost_threshold_usd=1.0)
        candidates = analyzer.analyse(WORKLOAD)
        names = [c.name for c in candidates]
        assert len(candidates) >= 1

    def test_single_query_not_candidate(self):
        analyzer = QueryAnalyzer(min_query_frequency=3)
        workload = [_q("SELECT DISTINCT status FROM jobs")]
        candidates = analyzer.analyse(workload)
        assert candidates == []

    def test_candidate_has_positive_benefit(self):
        analyzer = QueryAnalyzer(min_query_frequency=2, min_cost_threshold_usd=0.0)
        candidates = analyzer.analyse(WORKLOAD)
        for c in candidates:
            assert c.estimated_benefit_usd > 0

    def test_max_candidates_respected(self):
        big_workload = [
            _q(f"SELECT a{i}, b{i} FROM table_{i} GROUP BY a{i}", cost=0.5)
            for i in range(100)
            for _ in range(5)
        ]
        analyzer = QueryAnalyzer(
            min_query_frequency=2, min_cost_threshold_usd=0.0, max_candidates=20
        )
        candidates = analyzer.analyse(big_workload)
        assert len(candidates) <= 20

    def test_referenced_tables_extracted(self):
        workload = [
            _q("SELECT a, b FROM my_dataset.my_table WHERE x > 1")
        ] * 5
        analyzer = QueryAnalyzer(min_query_frequency=2, min_cost_threshold_usd=0.0)
        candidates = analyzer.analyse(workload)
        if candidates:
            assert any("my_table" in t for c in candidates for t in c.referenced_tables)
