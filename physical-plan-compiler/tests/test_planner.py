from src import Source, Filter, Aggregate, Join, plan, conversion_cost


def test_simple_filter_picks_an_engine():
    lp = Filter(Source("orders", 1_000_000), "amount > 100", selectivity=0.2)
    p = plan(lp)
    assert p.op.kind == "filter"
    assert p.op.engine in {"spark", "dbt", "duckdb", "flink"}
    assert p.total_cost > 0


def test_small_data_prefers_duckdb():
    """Tiny source: setup cost dominates → DuckDB wins."""
    lp = Filter(Source("small", 1_000), "x > 0")
    p = plan(lp)
    assert p.op.engine == "duckdb", f"expected duckdb for small, got {p.op.engine}"


def test_aggregate_pipeline():
    lp = Aggregate(
        Filter(Source("events", 10_000_000), "type = 'click'", 0.1),
        group_by=["user"],
        aggs=["count(*)"],
    )
    p = plan(lp)
    # Should be a single pipeline; cost > 0
    assert p.op.kind == "aggregate"
    assert p.total_cost > 0


def test_join_two_sources():
    lp = Join(
        Source("orders", 5_000_000),
        Source("customers", 100_000),
        join_key=["customer_id"],
    )
    p = plan(lp)
    assert p.op.kind == "join"
    # Children should be scans
    assert all(c.kind == "scan" for c in p.op.children)


def test_conversion_cost_zero_same_engine():
    assert conversion_cost("spark", "spark") == 0.0
    assert conversion_cost("dbt", "duckdb") > 0


def test_picks_lower_cost_plan():
    """Force two paths; verify we pick the cheaper."""
    # Same filter on small data — DuckDB should beat Spark setup cost
    lp = Filter(Source("tiny", 10_000), "z = 1")
    p_default = plan(lp, target_engines=["spark", "dbt", "duckdb", "flink"])
    p_spark_only = plan(lp, target_engines=["spark"])
    assert p_default.total_cost <= p_spark_only.total_cost


def test_huge_data_prefers_scalable_engine():
    """Very large data → spark or dbt should win over duckdb."""
    lp = Aggregate(
        Source("huge", 1_000_000_000),
        group_by=["k"],
        aggs=["sum(v)"],
    )
    p = plan(lp)
    # Spark or dbt should be chosen — these scale per-byte cheaper than duckdb
    assert p.op.engine in {"spark", "dbt"}


def test_deterministic_plan():
    """Same logical plan → same physical plan (memoization)."""
    lp = Filter(Source("t", 100_000), "x > 0")
    p1 = plan(lp)
    p2 = plan(lp)
    assert p1.total_cost == p2.total_cost
    assert p1.op.engine == p2.op.engine
