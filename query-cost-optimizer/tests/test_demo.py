"""Smoke tests for the demo report builder."""

import pytest
from query_cost_optimizer.demo import build_demo_report
from query_cost_optimizer.models import Platform


@pytest.mark.parametrize("platform", ["bigquery", "snowflake"])
def test_demo_report_structure(platform):
    report = build_demo_report(platform)
    assert report.platform == Platform(platform)
    assert report.total_queries_analyzed > 0
    assert report.total_cost_usd > 0
    assert len(report.recommendations) > 0
    assert len(report.expensive_patterns) > 0
    assert report.total_estimated_savings_usd > 0


def test_demo_recommendations_have_actions():
    report = build_demo_report("bigquery")
    for rec in report.recommendations:
        assert rec.action.strip() != ""
        assert rec.estimated_savings_usd_monthly > 0


def test_demo_patterns_have_fixes():
    report = build_demo_report("snowflake")
    for pat in report.expensive_patterns:
        assert pat.fix_suggestion.strip() != ""
        assert pat.estimated_savings_usd > 0
