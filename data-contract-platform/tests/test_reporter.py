"""Tests for breaking-change reporter."""

from pathlib import Path

from dce.contract import load_contract
from dce.reporter import breaking_change_report, contracts_dir_breaking_changes

CONTRACTS_DIR = Path(__file__).parent.parent / "contracts" / "examples"


def test_no_breaking_change_v1_to_v1_1():
    old = load_contract(CONTRACTS_DIR / "orders" / "v1.0.0.yaml")
    new = load_contract(CONTRACTS_DIR / "orders" / "v1.1.0.yaml")
    report = breaking_change_report(old, new)
    assert not report["is_breaking"]
    assert "shipping_region" in report["added_fields"]


def test_breaking_change_v1_1_to_v2():
    old = load_contract(CONTRACTS_DIR / "orders" / "v1.1.0.yaml")
    new = load_contract(CONTRACTS_DIR / "orders" / "v2.0.0.yaml")
    report = breaking_change_report(old, new)
    assert report["is_breaking"]
    removed = {r.split("'")[1] for r in report["breaking_changes"] if "REMOVED" in r}
    assert "total_amount" in removed or "discount_pct" in removed


def test_scan_contracts_dir_finds_breaking():
    reports = contracts_dir_breaking_changes(CONTRACTS_DIR)
    assert any(r["is_breaking"] for r in reports)


def test_markdown_report_writes(tmp_path):
    from dce.reporter import write_markdown_report
    old = load_contract(CONTRACTS_DIR / "orders" / "v1.1.0.yaml")
    new = load_contract(CONTRACTS_DIR / "orders" / "v2.0.0.yaml")
    report = breaking_change_report(old, new)
    out = tmp_path / "report.md"
    write_markdown_report(report, out)
    content = out.read_text()
    assert "Breaking Change" in content
    assert "1.1.0" in content
