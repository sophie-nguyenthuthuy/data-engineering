"""CLI entry point for the Pipeline Topology Compiler."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from . import (
    Target,
    check_equivalence,
    get_target,
    optimize,
    parse_yaml,
    select_target,
)
from .compiler.selector import SelectionReason


def cmd_compile(args: argparse.Namespace) -> int:
    spec = parse_yaml(args.pipeline)

    if args.target:
        target = Target(args.target)
        reason = None
    else:
        reason = select_target(spec)
        target = reason.target

    if not args.no_optimize:
        spec = optimize(spec)

    generator = get_target(target)
    artifact = generator.generate(spec)

    output_dir = Path(args.output or f"compiled/{spec.name}/{target.value}")
    artifact.write_to(output_dir)

    print(f"Compiled '{spec.name}' to {target.value.upper()}")
    if reason:
        print(f"  Target selected because: {reason.reason}")
    print(f"  Output: {output_dir}")
    for fname in artifact.files:
        print(f"    - {fname}")

    if args.verify:
        report = check_equivalence(spec, artifact)
        print(f"\nEquivalence check: {report.status.value.upper()}")
        print(str(report))
        return 0 if report.is_equivalent() else 2

    return 0


def cmd_check(args: argparse.Namespace) -> int:
    spec = parse_yaml(args.pipeline)
    artifact_path = Path(args.artifact)
    if not artifact_path.exists():
        print(f"Error: artifact file not found: {artifact_path}", file=sys.stderr)
        return 1

    with open(artifact_path) as f:
        artifact_data = json.load(f)

    from .targets.base import CompiledArtifact
    from .compiler.selector import Target

    artifact = CompiledArtifact(
        target=Target(artifact_data["target"]),
        spec_name=artifact_data["spec_name"],
        compiled_nodes=artifact_data.get("compiled_nodes", []),
    )

    report = check_equivalence(spec, artifact)
    print(str(report))
    return 0 if report.is_equivalent() else 1


def cmd_inspect(args: argparse.Namespace) -> int:
    spec = parse_yaml(args.pipeline)
    reason = select_target(spec)

    print(f"Pipeline: {spec.name}")
    print(f"  Description: {spec.description or '(none)'}")
    print(f"  SLA:         {spec.sla}")
    print(f"  Nodes ({len(spec.nodes)}):")
    for node in spec.topological_order():
        schema_str = ""
        if node.output_schema:
            schema_str = f"  → {node.output_schema}"
        print(f"    [{node.transform_type.value:12s}] {node.name}{schema_str}")
    print(f"\n  Recommended target: {reason.target.value.upper()}")
    print(f"  Reason: {reason.reason}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ptc",
        description="Pipeline Topology Compiler — compile DAG pipelines to Spark, Flink, or dbt",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # compile
    p_compile = sub.add_parser("compile", help="Compile a pipeline YAML to target code")
    p_compile.add_argument("pipeline", help="Path to pipeline YAML file")
    p_compile.add_argument(
        "--target", choices=["spark", "flink", "dbt"],
        help="Override automatic target selection"
    )
    p_compile.add_argument("--output", "-o", help="Output directory (default: compiled/<name>/<target>)")
    p_compile.add_argument("--no-optimize", action="store_true", help="Skip DAG optimization passes")
    p_compile.add_argument("--verify", action="store_true", help="Run equivalence check after compilation")
    p_compile.set_defaults(func=cmd_compile)

    # check
    p_check = sub.add_parser("check", help="Verify a compiled artifact against its logical spec")
    p_check.add_argument("pipeline", help="Path to pipeline YAML file")
    p_check.add_argument("artifact", help="Path to compiled artifact JSON metadata file")
    p_check.set_defaults(func=cmd_check)

    # inspect
    p_inspect = sub.add_parser("inspect", help="Show pipeline structure and recommended target")
    p_inspect.add_argument("pipeline", help="Path to pipeline YAML file")
    p_inspect.set_defaults(func=cmd_inspect)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
