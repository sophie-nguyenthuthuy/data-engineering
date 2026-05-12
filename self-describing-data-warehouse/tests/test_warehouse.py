"""
Core test suite for the self-describing warehouse.
Run: pytest
"""

import pytest
from warehouse.core.warehouse import SelfDescribingWarehouse
from warehouse.core.registry import TableMeta, ColumnMeta


@pytest.fixture
def wh():
    w = SelfDescribingWarehouse(db_path=":memory:")
    yield w
    w.close()


@pytest.fixture
def wh_with_table(wh):
    wh.create_table(
        "CREATE TABLE sales (id TEXT NOT NULL, amount REAL NOT NULL, region TEXT)"
    )
    wh.insert_many("sales", [
        {"id": "S1", "amount": 100.0, "region": "NA"},
        {"id": "S2", "amount": 200.0, "region": "EMEA"},
        {"id": "S3", "amount": 150.0, "region": "NA"},
        {"id": "S4", "amount": 300.0, "region": "APAC"},
    ])
    wh.registry.register_table(TableMeta(
        table_name="sales",
        description="Sales fact table with revenue by region",
        owner="test@acme.com",
        domain="finance",
        source_system="Stripe",
        update_frequency="daily",
        tags=["sales", "revenue", "finance"],
        columns=[
            ColumnMeta("id",     "TEXT", "Sale ID",         is_nullable=False),
            ColumnMeta("amount", "REAL", "Revenue in USD",  is_nullable=False),
            ColumnMeta("region", "TEXT", "Geographic region"),
        ],
    ))
    return wh


# ------------------------------------------------------------------ #
#  Registry                                                            #
# ------------------------------------------------------------------ #

class TestRegistry:
    def test_register_and_retrieve(self, wh_with_table):
        meta = wh_with_table.registry.get_table("sales")
        assert meta is not None
        assert meta["table_name"] == "sales"
        assert meta["owner"] == "test@acme.com"
        assert meta["domain"] == "finance"
        assert "revenue" in meta["tags"]

    def test_columns_registered(self, wh_with_table):
        meta = wh_with_table.registry.get_table("sales")
        col_names = [c["column_name"] for c in meta["columns"]]
        assert "id" in col_names
        assert "amount" in col_names
        assert "region" in col_names

    def test_list_tables(self, wh_with_table):
        tables = wh_with_table.registry.list_tables()
        assert any(t["table_name"] == "sales" for t in tables)

    def test_filter_by_domain(self, wh_with_table):
        finance = wh_with_table.registry.list_tables(domain="finance")
        assert all(t["domain"] == "finance" for t in finance)
        product = wh_with_table.registry.list_tables(domain="product")
        assert len(product) == 0

    def test_deprecate(self, wh_with_table):
        wh_with_table.registry.deprecate_table("sales", "Replaced by sales_v2")
        tables = wh_with_table.registry.list_tables()
        assert not any(t["table_name"] == "sales" for t in tables)
        all_tables = wh_with_table.registry.list_tables(include_deprecated=True)
        dep = next(t for t in all_tables if t["table_name"] == "sales")
        assert dep["is_deprecated"] == 1
        assert dep["deprecation_note"] == "Replaced by sales_v2"


# ------------------------------------------------------------------ #
#  Quality                                                             #
# ------------------------------------------------------------------ #

class TestQuality:
    def test_quality_run(self, wh_with_table):
        result = wh_with_table.quality.run("sales")
        assert result["row_count"] == 4
        assert 0.0 <= result["quality_score"] <= 100.0
        assert result["null_rate"] >= 0.0

    def test_quality_history(self, wh_with_table):
        wh_with_table.quality.run("sales")
        wh_with_table.quality.run("sales")
        history = wh_with_table.quality.history("sales", limit=10)
        assert len(history) >= 2

    def test_perfect_data_scores_high(self, wh):
        wh.create_table("CREATE TABLE perfect (id TEXT NOT NULL, val REAL NOT NULL)")
        wh.insert_many("perfect", [{"id": str(i), "val": float(i)} for i in range(10)])
        wh.registry.register_table(TableMeta(
            table_name="perfect", description="d", owner="o", domain="d"
        ))
        result = wh.quality.run("perfect")
        assert result["quality_score"] >= 95.0

    def test_trend_stable_on_single_run(self, wh_with_table):
        wh_with_table.quality.run("sales")
        assert wh_with_table.quality.trend("sales") == "stable"


# ------------------------------------------------------------------ #
#  Freshness                                                           #
# ------------------------------------------------------------------ #

class TestFreshness:
    def test_fresh_table(self, wh_with_table):
        from datetime import datetime, timezone, timedelta
        recent = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        result = wh_with_table.freshness.record("sales", recent, expected_interval_hours=24)
        assert result["freshness_score"] == 100.0
        assert result["status"] == "fresh"

    def test_stale_table(self, wh_with_table):
        from datetime import datetime, timezone, timedelta
        old = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
        result = wh_with_table.freshness.record("sales", old, expected_interval_hours=24)
        assert result["freshness_score"] < 20.0
        assert result["status"] == "very_stale"

    def test_stale_tables_query(self, wh_with_table):
        from datetime import datetime, timezone, timedelta
        old = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
        wh_with_table.freshness.record("sales", old, expected_interval_hours=24)
        stale = wh_with_table.freshness.stale_tables(threshold_score=90)
        assert any(r["table_name"] == "sales" for r in stale)


# ------------------------------------------------------------------ #
#  Usage                                                               #
# ------------------------------------------------------------------ #

class TestUsage:
    def test_record_and_stats(self, wh_with_table):
        wh_with_table.usage.record("sales", queried_by="alice", query="SELECT * FROM sales")
        wh_with_table.usage.record("sales", queried_by="bob",   query="SELECT COUNT(*) FROM sales")
        stats = wh_with_table.usage.stats("sales")
        assert stats["total_queries"] >= 2
        assert stats["unique_users"] >= 2

    def test_top_tables(self, wh_with_table):
        for _ in range(5):
            wh_with_table.usage.record("sales", queried_by="alice")
        top = wh_with_table.usage.top_tables(limit=3)
        assert top[0]["table_name"] == "sales"
        assert top[0]["query_count"] >= 5

    def test_usage_score_increases_with_queries(self, wh_with_table):
        s0 = wh_with_table.usage.usage_score("sales")
        for _ in range(10):
            wh_with_table.usage.record("sales", queried_by="user")
        s1 = wh_with_table.usage.usage_score("sales")
        assert s1 > s0

    def test_execute_auto_tracks(self, wh_with_table):
        before = wh_with_table.usage.stats("sales").get("total_queries", 0)
        wh_with_table.execute("SELECT * FROM sales", user="tester")
        after = wh_with_table.usage.stats("sales").get("total_queries", 0)
        assert after == before + 1


# ------------------------------------------------------------------ #
#  Lineage                                                             #
# ------------------------------------------------------------------ #

class TestLineage:
    def test_add_and_traverse(self, wh):
        for name in ("raw", "clean", "report"):
            wh.registry.register_table(
                TableMeta(table_name=name, description=name, owner="o", domain="d")
            )
        wh.lineage.add_edge("raw", "clean", "deduplicate")
        wh.lineage.add_edge("clean", "report", "aggregate")

        up = wh.lineage.upstream("report")
        assert any(t["table_name"] == "clean" for t in up)
        assert any(t["table_name"] == "raw" for t in up)

        dn = wh.lineage.downstream("raw")
        assert any(t["table_name"] == "clean" for t in dn)
        assert any(t["table_name"] == "report" for t in dn)

    def test_impact_analysis(self, wh):
        for name in ("src", "mid", "end"):
            wh.registry.register_table(
                TableMeta(table_name=name, description=name, owner="o", domain="d")
            )
        wh.lineage.add_edge("src", "mid")
        wh.lineage.add_edge("mid", "end")
        impact = wh.lineage.impact_analysis("src")
        assert impact["total_affected"] == 2


# ------------------------------------------------------------------ #
#  Incidents                                                           #
# ------------------------------------------------------------------ #

class TestIncidents:
    def test_open_and_resolve(self, wh_with_table):
        inc_id = wh_with_table.incidents.open("sales", "Data gap detected", severity="high")
        open_inc = wh_with_table.incidents.open_incidents("sales")
        assert any(i["id"] == inc_id for i in open_inc)

        wh_with_table.incidents.resolve(inc_id, root_cause="pipeline fixed", resolved_by="sre")
        open_after = wh_with_table.incidents.open_incidents("sales")
        assert not any(i["id"] == inc_id for i in open_after)

    def test_reliability_score_penalised_by_open(self, wh_with_table):
        s_before = wh_with_table.incidents.reliability_score("sales")
        wh_with_table.incidents.open("sales", "Something broke", severity="critical")
        s_after = wh_with_table.incidents.reliability_score("sales")
        assert s_after < s_before


# ------------------------------------------------------------------ #
#  Recommender                                                         #
# ------------------------------------------------------------------ #

class TestRecommender:
    def _setup_multi(self, wh):
        wh.create_table("CREATE TABLE rev_summary (month TEXT, revenue REAL)")
        wh.create_table("CREATE TABLE raw_orders (id TEXT, amount REAL)")
        wh.insert_many("rev_summary", [{"month": "2024-01", "revenue": 50000.0}])
        wh.insert_many("raw_orders",  [{"id": "1", "amount": 100.0}])

        wh.registry.register_table(TableMeta(
            table_name="rev_summary",
            description="Monthly revenue summary for finance reporting",
            owner="analytics@acme.com",
            domain="finance",
            tags=["revenue", "monthly", "summary"],
        ))
        wh.registry.register_table(TableMeta(
            table_name="raw_orders",
            description="Raw order transactions",
            owner="eng@acme.com",
            domain="finance",
            tags=["orders", "raw"],
        ))

        from datetime import datetime, timezone, timedelta
        fresh = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        wh.freshness.record("rev_summary", fresh, 24)
        wh.freshness.record("raw_orders",  fresh, 24)
        wh.quality.run("rev_summary")
        wh.quality.run("raw_orders")
        for _ in range(20):
            wh.usage.record("rev_summary", queried_by="alice")
        for _ in range(2):
            wh.usage.record("raw_orders", queried_by="bob")

    def test_relevant_table_ranks_first(self, wh):
        self._setup_multi(wh)
        results = wh.recommend("monthly revenue finance")
        assert len(results) > 0
        assert results[0].table_name == "rev_summary"

    def test_deprecated_excluded_by_default(self, wh):
        self._setup_multi(wh)
        wh.registry.deprecate_table("raw_orders", "old")
        results = wh.recommend("orders")
        assert not any(r.table_name == "raw_orders" for r in results)

    def test_scores_within_bounds(self, wh):
        self._setup_multi(wh)
        results = wh.recommend("revenue")
        for r in results:
            assert 0 <= r.composite_score <= 100


# ------------------------------------------------------------------ #
#  Describe (integration)                                             #
# ------------------------------------------------------------------ #

class TestDescribe:
    def test_describe_contains_all_sections(self, wh_with_table):
        from datetime import datetime, timezone, timedelta
        wh_with_table.quality.run("sales")
        wh_with_table.freshness.record(
            "sales",
            (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
            24,
        )
        desc = wh_with_table.describe("sales")
        assert "columns" in desc
        assert "quality" in desc
        assert "freshness" in desc
        assert "usage" in desc
        assert "lineage" in desc

    def test_describe_unknown_table(self, wh):
        desc = wh.describe("does_not_exist")
        assert "error" in desc
