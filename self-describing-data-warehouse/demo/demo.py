"""
Interactive demo — runs a guided tour of the self-describing warehouse.
Run: python -m demo.demo
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import argparse
from warehouse.core.warehouse import SelfDescribingWarehouse
from demo.seed import seed


DIVIDER = "\n" + "─" * 65 + "\n"


def run_demo(db_path: str) -> None:
    print("━" * 65)
    print("  Self-Describing Data Warehouse — Live Demo")
    print("━" * 65)

    wh = SelfDescribingWarehouse(db_path=db_path)
    seed(wh)

    # 1. Catalog
    print(DIVIDER + "1. CATALOG — what tables exist?")
    tables = wh.catalog()
    for t in tables:
        print(f"   [{t['domain']:10s}] {t['table_name']:35s}  {t['owner']}")

    # 2. Recommend
    print(DIVIDER + "2. RECOMMEND — 'which table for monthly revenue by region?'")
    results = wh.recommend("monthly revenue by region", top_k=3)
    for i, r in enumerate(results, 1):
        print(f"   #{i}  {r.table_name:<30} score={r.composite_score}  "
              f"qual={r.quality_score}  fresh={r.freshness_score}  "
              f"usage={r.usage_score}")

    print(DIVIDER + "3. RECOMMEND — 'customer segmentation and cohort'")
    results2 = wh.recommend("customer segmentation cohort", top_k=3)
    for i, r in enumerate(results2, 1):
        print(f"   #{i}  {r.table_name:<30} score={r.composite_score}")

    # 3. Describe best table
    print(DIVIDER + "4. DESCRIBE — full metadata profile for 'monthly_revenue_summary'")
    desc = wh.describe("monthly_revenue_summary")
    print(f"   Description : {desc['description'][:80]}...")
    print(f"   Owner       : {desc['owner']}")
    print(f"   Tags        : {', '.join(desc['tags'])}")
    q = desc["quality"]
    print(f"   Quality     : {q['quality_score']}/100  (rows={q['row_count']}, "
          f"null_rate={q['null_rate']:.1%})")
    f = desc["freshness"]
    print(f"   Freshness   : {f['freshness_score']}/100  ({f['hours_since_update']:.1f}h ago)")
    u = desc["usage"]
    print(f"   Usage       : {u['total_queries']} queries by {u['unique_users']} users")
    lin = desc["lineage"]
    print(f"   Upstream    : {', '.join(t['table_name'] for t in lin['upstream'])}")
    print(f"   Downstream  : {', '.join(t['table_name'] for t in lin['downstream']) or 'none'}")

    # 4. Health dashboard
    print(DIVIDER + "5. HEALTH DASHBOARD — warehouse-wide quality overview")
    dashboard = wh.health_dashboard()
    print(f"   {'Table':35s} {'Domain':10s} {'Quality':8s} {'Freshness':10s} {'Queries':8s} {'Trend'}")
    print(f"   {'─'*35} {'─'*10} {'─'*8} {'─'*10} {'─'*8} {'─'*10}")
    for row in dashboard:
        q_val = f"{row['quality_score']:.1f}" if row['quality_score'] is not None else "n/a"
        f_val = f"{row['freshness_score']:.1f}" if row['freshness_score'] is not None else "n/a"
        print(f"   {row['table_name']:35s} {row['domain']:10s} {q_val:8s} {f_val:10s} "
              f"{row['total_queries']:<8d} {row['trend']}")

    # 5. Lineage / impact analysis
    print(DIVIDER + "6. LINEAGE — impact analysis if 'orders' goes down")
    impact = wh.lineage.impact_analysis("orders")
    print(f"   {impact['total_affected']} table(s) would be affected:")
    for t in impact["affected_tables"]:
        print(f"     depth={t['depth']}  {t['table_name']}")

    # 6. Incidents
    print(DIVIDER + "7. INCIDENTS — open issues across all tables")
    open_inc = wh.incidents.open_incidents()
    if open_inc:
        for inc in open_inc:
            print(f"   [{inc['severity'].upper():8s}] {inc['table_name']:25s}  {inc['description'][:55]}")
    else:
        print("   No open incidents.")

    # 7. Deprecated table
    print(DIVIDER + "8. DEPRECATED TABLE — 'revenue_by_product_old'")
    dep = wh.describe("revenue_by_product_old")
    print(f"   Deprecated: {dep.get('is_deprecated')}  Note: {dep.get('deprecation_note')}")

    # 8. Track a new query
    print(DIVIDER + "9. TRACKED QUERY — run SQL and watch usage update automatically")
    rows = wh.execute(
        "SELECT region, SUM(total_revenue) as rev FROM monthly_revenue_summary GROUP BY region",
        user="demo-user@acme.com",
    )
    for row in rows:
        print(f"   {row['region']:6s}  ${row['rev']:,.0f}")
    updated_usage = wh.usage.stats("monthly_revenue_summary")
    print(f"\n   Usage after query: {updated_usage['total_queries']} total queries "
          f"(+1 auto-tracked)")

    print(DIVIDER)
    print("  Demo complete. Explore further with:")
    print("    python -m warehouse.cli.main --db warehouse.db describe orders")
    print("    python -m warehouse.cli.main --db warehouse.db recommend 'product revenue'")
    print("    python -m warehouse.cli.main --db warehouse.db health")
    print()

    wh.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=":memory:", help="DB path (:memory: for ephemeral)")
    args = parser.parse_args()
    run_demo(args.db)
