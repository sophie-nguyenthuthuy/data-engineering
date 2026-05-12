"""
Demo: detect and report breaking changes between contract versions.

Run:
    cd data-contract-platform
    pip install -e .
    python examples/breaking_change_demo.py
"""

import json
from pathlib import Path

from dce.contract import load_contract
from dce.reporter import breaking_change_report, write_markdown_report

ORDERS = Path(__file__).parent.parent / "contracts" / "examples" / "orders"

pairs = [
    ("v1.0.0.yaml", "v1.1.0.yaml"),
    ("v1.1.0.yaml", "v2.0.0.yaml"),
]

for from_f, to_f in pairs:
    old = load_contract(ORDERS / from_f)
    new = load_contract(ORDERS / to_f)
    report = breaking_change_report(old, new)

    print(f"\n{'=' * 60}")
    print(f"  {old.version}  →  {new.version}")
    print(f"  Breaking: {report['is_breaking']}")
    print(f"  Summary:  {report['summary']}")
    for change in report["breaking_changes"]:
        print(f"  ⚠  {change}")
    for field in report["added_fields"]:
        print(f"  +  {field}")
    for field in report["removed_fields"]:
        print(f"  -  {field}")

    if report["is_breaking"]:
        md_path = Path(f"/tmp/breaking_{old.version}_to_{new.version}.md")
        write_markdown_report(report, md_path)
        print(f"\n  Markdown report written → {md_path}")
