"""Tests for JSON and HTML reporters."""

import json
import tempfile
from pathlib import Path

import pytest
from query_cost_optimizer.demo import build_demo_report
from query_cost_optimizer.reporters.report import JsonReporter, HtmlReporter


@pytest.fixture
def demo_report():
    return build_demo_report("bigquery")


def test_json_reporter_writes_valid_json(demo_report, tmp_path):
    out = tmp_path / "report.json"
    JsonReporter().render(demo_report, out)
    assert out.exists()
    data = json.loads(out.read_text())
    assert data["platform"] == "bigquery"
    assert "recommendations" in data
    assert "expensive_patterns" in data
    assert data["total_estimated_savings_usd_monthly"] > 0


def test_json_reporter_recommendation_fields(demo_report, tmp_path):
    out = tmp_path / "report.json"
    JsonReporter().render(demo_report, out)
    data = json.loads(out.read_text())
    for rec in data["recommendations"]:
        assert "title" in rec
        assert "action" in rec
        assert rec["estimated_savings_usd_monthly"] > 0


def test_html_reporter_writes_html(demo_report, tmp_path):
    out = tmp_path / "report.html"
    HtmlReporter().render(demo_report, out)
    assert out.exists()
    html = out.read_text()
    assert "<!DOCTYPE html>" in html
    assert "Recommendations" in html
    assert "Expensive Patterns" in html


def test_html_reporter_contains_savings(demo_report, tmp_path):
    out = tmp_path / "report.html"
    HtmlReporter().render(demo_report, out)
    html = out.read_text()
    # The savings value should appear somewhere in the HTML
    assert "$" in html
