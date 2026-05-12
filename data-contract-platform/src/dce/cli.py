"""CLI entry point: dce <command> [options]"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

from .contract import load_contract
from .notifier import build_notifier
from .registry import ContractRegistry
from .reporter import (
    breaking_change_report,
    consumer_notification,
    contracts_dir_breaking_changes,
    validation_summary_report,
    write_markdown_report,
    write_report,
)
from .scorer import ReliabilityStore
from .validator import ContractValidator


# ------------------------------------------------------------------ #
# Helpers
# ------------------------------------------------------------------ #

def _load_data(path: str) -> pd.DataFrame:
    p = Path(path)
    if p.suffix == ".csv":
        return pd.read_csv(p)
    if p.suffix == ".parquet":
        return pd.read_parquet(p)
    if p.suffix == ".json":
        return pd.read_json(p)
    raise ValueError(f"Unsupported data format: {p.suffix}")


def _color(text: str, code: str) -> str:
    if sys.stdout.isatty():
        return f"\033[{code}m{text}\033[0m"
    return text


def _ok(text: str) -> str:
    return _color(text, "32")


def _fail(text: str) -> str:
    return _color(text, "31")


def _warn(text: str) -> str:
    return _color(text, "33")


# ------------------------------------------------------------------ #
# Commands
# ------------------------------------------------------------------ #

def cmd_validate(args: argparse.Namespace) -> int:
    contract = load_contract(args.contract)
    df = _load_data(args.data)

    validator = ContractValidator(contract)
    result = validator.validate(
        df,
        freshness_seconds=args.freshness,
        latency_seconds=args.latency,
    )

    # Persist result
    store = ReliabilityStore(args.db)
    store.record(result)

    # Print summary
    status = _ok("PASSED") if result.passed else _fail("FAILED")
    print(f"\n[{status}] {contract.id} @ {contract.version}  ({len(df):,} rows)\n")
    for issue in result.issues:
        prefix = _fail("  ERROR  ") if issue.severity == "error" else _warn("  WARN   ")
        print(f"{prefix} [{issue.rule}] {issue.message}")

    if result.issues:
        print()

    # JSON output
    if args.output:
        report_dict = result.to_dict()
        write_report(report_dict, args.output)
        print(f"Report written → {args.output}")

    # Notifications
    if not result.passed and args.notify:
        notifier = build_notifier(args.notify or None)
        score = store.score(result.producer, result.contract_id)
        payload = consumer_notification(result)
        if score:
            payload["reliability_score"] = score.reliability_score
        notifier.send(payload)

    return 0 if result.passed else 1


def cmd_score(args: argparse.Namespace) -> int:
    store = ReliabilityStore(args.db)
    scores = store.all_scores(window=args.window)
    if not scores:
        print("No validation runs found.")
        return 0

    header = f"{'Producer':<30} {'Contract':<35} {'Score':>7}  {'Runs':>5}  {'Last'}"
    print(header)
    print("-" * len(header))
    for s in sorted(scores, key=lambda x: x.reliability_score):
        pct = f"{s.reliability_score:.1%}"
        color_fn = _ok if s.reliability_score >= 0.95 else (_warn if s.reliability_score >= 0.80 else _fail)
        print(
            f"{s.producer:<30} {s.contract_id:<35} "
            f"{color_fn(pct):>7}  {s.total_runs:>5}  {s.last_validated_at[:19]}"
        )
    return 0


def cmd_diff(args: argparse.Namespace) -> int:
    registry = ContractRegistry(args.contracts_dir)
    old = registry.get(args.contract_id, args.from_version)
    new = registry.get(args.contract_id, args.to_version)
    report = breaking_change_report(old, new)

    if args.markdown:
        write_markdown_report(report, args.markdown)
        print(f"Markdown report written → {args.markdown}")
    else:
        print(json.dumps(report, indent=2, default=str))

    return 1 if report["is_breaking"] else 0


def cmd_check_all(args: argparse.Namespace) -> int:
    """Scan contracts directory and report all breaking changes (CI mode)."""
    reports = contracts_dir_breaking_changes(args.contracts_dir)
    if not reports:
        print(_ok("No breaking changes detected."))
        return 0

    print(_fail(f"{len(reports)} breaking change(s) detected:\n"))
    for r in reports:
        print(f"  {r['contract_id']} {r['from_version']} → {r['to_version']}")
        for change in r["breaking_changes"]:
            print(f"    ⚠  {change}")
    return 1


def cmd_list(args: argparse.Namespace) -> int:
    registry = ContractRegistry(args.contracts_dir)
    for cid in registry.ids():
        versions = registry.versions(cid)
        latest = registry.latest(cid)
        print(f"  {cid:<40} versions: {', '.join(versions)}  producer: {latest.producer}")
    return 0


# ------------------------------------------------------------------ #
# Arg parser
# ------------------------------------------------------------------ #

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dce",
        description="Data Contract Enforcement CLI",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # --- validate ---
    p_val = sub.add_parser("validate", help="Validate a data file against a contract")
    p_val.add_argument("contract", help="Path to contract YAML file")
    p_val.add_argument("data", help="Path to data file (CSV / Parquet / JSON)")
    p_val.add_argument("--db", default="reliability.db", help="Path to reliability SQLite DB")
    p_val.add_argument("--output", "-o", help="Write JSON report to file")
    p_val.add_argument("--freshness", type=float, default=None,
                       help="Data age in seconds (for freshness SLA checks)")
    p_val.add_argument("--latency", type=float, default=None,
                       help="Pipeline latency in seconds")
    p_val.add_argument("--notify", nargs="*", metavar="URL",
                       help="Webhook URLs to notify on failure")

    # --- score ---
    p_score = sub.add_parser("score", help="Show reliability scores per producer")
    p_score.add_argument("--db", default="reliability.db")
    p_score.add_argument("--window", type=int, default=100,
                         help="Rolling window size for score calculation")

    # --- diff ---
    p_diff = sub.add_parser("diff", help="Diff two contract versions")
    p_diff.add_argument("contracts_dir", help="Path to contracts directory")
    p_diff.add_argument("contract_id")
    p_diff.add_argument("from_version")
    p_diff.add_argument("to_version")
    p_diff.add_argument("--markdown", help="Write Markdown report to file")

    # --- check-all ---
    p_ca = sub.add_parser("check-all", help="CI: detect breaking changes across all contracts")
    p_ca.add_argument("contracts_dir", help="Path to contracts directory")

    # --- list ---
    p_list = sub.add_parser("list", help="List all contracts in a directory")
    p_list.add_argument("contracts_dir")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    dispatch = {
        "validate": cmd_validate,
        "score": cmd_score,
        "diff": cmd_diff,
        "check-all": cmd_check_all,
        "list": cmd_list,
    }
    sys.exit(dispatch[args.command](args))


if __name__ == "__main__":
    main()
