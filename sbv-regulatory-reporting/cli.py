#!/usr/bin/env python3
"""SBV Regulatory Reporting — command-line interface.

Usage examples:
    python cli.py run data/sample/transactions.csv
    python cli.py run data/sample/transactions.csv --date 31/12/2025 --operator OPS001
    python cli.py verify data/output/audit/audit_<run_id>.jsonl
    python cli.py generate-sample
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def cmd_run(args):
    from sbv_reporting.pipeline import Pipeline

    result = Pipeline().run(
        input_path=args.input,
        report_date=args.date,
        operator=args.operator,
        write_excel=not args.no_excel,
        write_csv=not args.no_csv,
    )
    result.print_summary()
    sys.exit(0 if result.success else 1)


def cmd_verify(args):
    """Verify the integrity of an audit log file."""
    from sbv_reporting.audit.trail import AuditTrail

    log_path = Path(args.log)
    if not log_path.exists():
        print(f"Error: log file not found: {log_path}")
        sys.exit(1)

    run_id = log_path.stem.replace("audit_", "")
    trail = AuditTrail(run_id, log_dir=log_path.parent)
    ok, errors = trail.verify()

    if ok:
        print(f"Audit chain VALID — {log_path}")
        summary = trail.summary()
        print(f"  Entries    : {summary['total_entries']}")
        print(f"  Chain hash : {summary['chain_hash'][:32]}...")
    else:
        print(f"Audit chain COMPROMISED — {log_path}")
        for e in errors:
            print(f"  ✗ {e}")
    sys.exit(0 if ok else 2)


def cmd_generate_sample(args):
    """Re-generate sample data."""
    import subprocess
    sample_dir = Path(__file__).parent / "data" / "sample"
    subprocess.run([sys.executable, "generate_sample.py"], cwd=sample_dir, check=True)
    print(f"Sample data written to {sample_dir / 'transactions.csv'}")


def main():
    parser = argparse.ArgumentParser(
        prog="sbv-report",
        description="SBV Regulatory Reporting Pipeline",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # run
    p_run = sub.add_parser("run", help="Run the full reporting pipeline")
    p_run.add_argument("input", help="Path to raw transactions CSV")
    p_run.add_argument("--date", default=None, help="Report date dd/mm/yyyy (default: today)")
    p_run.add_argument("--operator", default="SYSTEM", help="Operator ID for audit trail")
    p_run.add_argument("--no-excel", action="store_true", help="Skip Excel output")
    p_run.add_argument("--no-csv", action="store_true", help="Skip CSV output")
    p_run.set_defaults(func=cmd_run)

    # verify
    p_verify = sub.add_parser("verify", help="Verify audit log chain integrity")
    p_verify.add_argument("log", help="Path to audit .jsonl file")
    p_verify.set_defaults(func=cmd_verify)

    # generate-sample
    p_gen = sub.add_parser("generate-sample", help="Re-generate sample transaction data")
    p_gen.set_defaults(func=cmd_generate_sample)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
